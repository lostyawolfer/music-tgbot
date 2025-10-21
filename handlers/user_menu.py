import asyncio
import os
import re
from io import BytesIO
import aiohttp
import concurrent.futures

from aiogram import Router, Bot, F
from aiogram.enums import ChatAction, ChatType
from aiogram.filters import Command
from aiogram.types import Message, FSInputFile, BufferedInputFile, LinkPreviewOptions
from PIL import Image
from mutagen.mp3 import MP3
from mutagen.id3 import ID3, APIC, TIT2, TPE1
from yt_dlp import YoutubeDL
from db.db import Music, Analytics
# Add this near the top of your file, after imports
import logging

# Configure yt-dlp logger to suppress HTTP 403 errors for cached content
yt_dlp_logger = logging.getLogger('yt_dlp')
yt_dlp_logger.setLevel(logging.ERROR)

class YtDlpFilter(logging.Filter):
    def filter(self, record):
        # Suppress HTTP 403 errors that are just informational
        if 'HTTP Error 403: Forbidden' in record.getMessage():
            return False
        return True

yt_dlp_logger.addFilter(YtDlpFilter())


router = Router()

DOWNLOAD_DIR = "downloads"
if not os.path.exists(DOWNLOAD_DIR):
    os.makedirs(DOWNLOAD_DIR)

# Initialize database
db = Music()
db_analytics = Analytics()

# Create a thread pool for CPU-bound tasks
thread_pool = concurrent.futures.ThreadPoolExecutor()

# Create a semaphore to limit concurrent downloads
MAX_CONCURRENT_DOWNLOADS = 5
download_semaphore = asyncio.Semaphore(MAX_CONCURRENT_DOWNLOADS)

# Track active tasks per user
user_tasks = {}  # {user_id: [task1, task2, ...]}
user_messages = {}


async def run_in_threadpool(func, *args, **kwargs):
    """Run a synchronous function in a thread pool."""
    return await asyncio.get_event_loop().run_in_executor(
        thread_pool, lambda: func(*args, **kwargs)
    )


async def download_video(url, ydl_opts):
    """Download a video using yt-dlp in a separate thread."""
    with YoutubeDL(ydl_opts) as ydl:
        return await run_in_threadpool(ydl.extract_info, url, download=True)


def _remove_duplicate_artists(artist_string: str) -> str:
    """
    Cleans an artist string by removing duplicate artist names.
    Handles various delimiters like ',', ' and ', ' & '.
    """
    if not artist_string:
        return ""

    # Normalize delimiters: replace ' and ' and ' & ' with ','
    normalized_string = artist_string.replace(' and ', ',').replace(' & ', ',')

    # Split by comma, trim whitespace, and convert to lowercase for case-insensitive comparison
    artists = [a.strip().lower() for a in normalized_string.split(',') if a.strip()]

    # Use a set to keep track of seen artists to ensure uniqueness
    seen = set()
    unique_artists = []

    for artist_name in artists:
        if artist_name not in seen:
            unique_artists.append(artist_name.title()) # Capitalize first letter of each word
            seen.add(artist_name)

    # Reconstruct the string with unique, properly capitalized artists
    return ", ".join(unique_artists)

async def process_audio(audio_filepath, title, artist, thumbnail_url):
    """Process the audio file with metadata and thumbnail."""
    async with aiohttp.ClientSession() as session:
        async with session.get(thumbnail_url) as response:
            thumbnail_data = await response.read()

    # Process image in threadpool (CPU-bound)
    def process_image_and_audio():
        img = Image.open(BytesIO(thumbnail_data))
        if img.mode == "RGBA":
            img = img.convert("RGB")

        # Crop to centered square
        width, height = img.size
        min_dimension = min(width, height)

        # Calculate crop coordinates for centered square
        left = (width - min_dimension) // 2
        top = (height - min_dimension) // 2
        right = left + min_dimension
        bottom = top + min_dimension

        # Crop to square
        img = img.crop((left, top, right, bottom))

        # Further crop the square to make it smaller
        original_square_dim = img.size[0]  # Assuming it's already a square
        target_square_dim = int(original_square_dim * (346 / 461))

        # Calculate crop coordinates for the new smaller centered square
        new_left = (original_square_dim - target_square_dim) // 2
        new_top = (original_square_dim - target_square_dim) // 2
        new_right = new_left + target_square_dim
        new_bottom = new_top + target_square_dim

        img = img.crop((new_left, new_top, new_right, new_bottom))

        thumbnail_bytes = BytesIO()
        img.save(thumbnail_bytes, format="PNG")
        thumbnail_bytes.seek(0)

        audio = MP3(audio_filepath, ID3=ID3)
        if audio.tags is None:
            audio.add_tags()

        # Clear existing APIC tags to avoid duplicates
        if 'APIC:' in audio.tags:
            del audio.tags['APIC:']

        audio.tags.add(
            APIC(
                encoding=3,  # UTF-8
                mime="image/PNG",
                type=3,  # 3 is for Front Cover
                desc="Cover",
                data=thumbnail_bytes.getvalue(),
            )
        )
        audio.tags.add(TIT2(encoding=3, text=title))  # Title
        cleaned_artist = _remove_duplicate_artists(artist)
        audio.tags.add(TPE1(encoding=3, text=cleaned_artist))  # Artist
        audio.save()

        return thumbnail_bytes.getvalue()

    return await run_in_threadpool(process_image_and_audio)


async def animate_starting_progress(progress_msg, original_url, bot):
    """Animate the progress message with ellipsis and show playlist warning if needed."""
    animations = [".", "..", "..."]
    count = 0

    while True:
        try:
            animation = animations[count % len(animations)]
            await bot.send_chat_action(chat_id=progress_msg.chat.id, action=ChatAction.CHOOSE_STICKER)
            message = f"<blockquote>{original_url}</blockquote>\n🛜 подготовка к скачиванию{animation}"

            if count >= 15:
                message = f"<blockquote>{original_url}</blockquote>\n⏳ плейлисты обрабатываются дольше, терпи\n<i>[прошло {count} сек.] -- /cancel чтобы отменить</i>"

            await progress_msg.edit_text(
                message,
                link_preview_options=LinkPreviewOptions(is_disabled=True),
                parse_mode="HTML"
            )
            count += 1
            await asyncio.sleep(1)
        except Exception:
            # Message might have been deleted or edited elsewhere
            break


async def animate_progress(progress_msg, original_url, text,
                           text_after_ellipsis, bot, chat_action: ChatAction):
    """Animate the progress message with ellipsis and show playlist warning if needed."""
    animations = [".", "..", "..."]
    count = 0

    while True:
        try:
            animation = animations[count % len(animations)]
            message = f"<blockquote>{original_url}</blockquote>\n{text}{animation}{text_after_ellipsis}"

            await bot.send_chat_action(chat_id=progress_msg.chat.id, action=chat_action)
            await progress_msg.edit_text(
                message,
                link_preview_options=LinkPreviewOptions(is_disabled=True),
                parse_mode="HTML"
            )
            count += 1
            await asyncio.sleep(1)
        except Exception:
            # Message might have been deleted or edited elsewhere
            break


async def send_cached_audio(msg, bot, video_id, file_id, progress_msg):
    """Send cached audio using existing file_id."""
    try:
        await progress_msg.delete()
        await bot.send_audio(
            chat_id=msg.chat.id,
            audio=file_id,
            disable_notification=True,
        )
        return True
    except Exception as e:
        print(f"Error sending cached audio for {video_id}: {e}")
        # File might be deleted from Telegram, remove from database
        db.remove_data(video_id)
        return False


@router.message(Command(commands=["start"]))
async def start(msg: Message):
    await msg.answer(
        """<b><u>lostya's youtube music downloader</u></b>
этот бот сделан специально для @lostyawolfer но им ты тоже можешь пользоваться

короче смысл такой. я слушаю музыку через телеграм. и меня достало что я не могу просто без ничего лишнего взять и скачать нужные мне музыки.
этот бот решает эту проблему.

<b>РАБОТАЕТ ТОЛЬКО ЮТУБ!</b>

<b><i>КАК ПОЛЬЗОВАТЬСЯ:</i></b>
<blockquote>- отправляешь ссылку на ютуб видео или ютуб плейлист
- бот всё сделает за тебя, скачав звук, добавив в него превьюшку, имя автора и название трека
- можно отправлять несколько ссылок за раз, обработка асинхронна
- если ты чёт перепутал всегда можно использовать команду /cancel чтобы отменить все текущие загрузки
- каждое сообщение прогресса анимировано чтобы ты мог видеть что бот не умер а реально что-то делает
- бот автоматом будет чистить чат от команд и подобного, чтобы держать твой "плейлист" в чистоте! :>
- теперь бот кеширует файлы! повторные запросы одного видео будут мгновенными!</blockquote>

бот почти полностью сделан через ии (gemini 2.5 flash, claude 3.7 sonnet) но я вложил немало усилий в то чтобы заставить эту хрень работать и всё соединить воедино ибо ии обычно немного тупой
но разве это имеет значение? бот офигенен. пользуйся""", parse_mode='HTML')


@router.message(Command(commands=["analytics"]))
async def send_analytics(msg: Message):
    if msg.from_user.id != 653632008:
        await msg.delete()
        return
    await msg.delete()
    analytics_msg = await msg.answer(f'бот использовался всего {db_analytics.get_total_use_count()} раз, {db_analytics.get_user_count()} уникальными пользователями')
    await asyncio.sleep(5)
    await analytics_msg.delete()



@router.message(Command(commands=["cancel"]))
async def cancel_downloads(msg: Message):
    user_id = msg.from_user.id

    if user_id not in user_tasks or not user_tasks[user_id]:
        cancel_msg = await msg.answer("✅ у тебя нечего отменять! :>")
        # Delete the command message and the response after a delay
        await asyncio.sleep(5)
        await msg.delete()
        await cancel_msg.delete()
        return

    # Cancel all tasks for this user
    for task in user_tasks[user_id]:
        if not task.done() and not task.cancelled():
            task.cancel()

    # Delete all progress messages
    if user_id in user_messages:
        for message in user_messages[user_id]:
            try:
                await message.delete()
            except Exception:
                pass
        user_messages[user_id] = []

    # Clear the tasks list
    user_tasks[user_id] = []

    cancel_msg = await msg.answer("✅ отменено! :>")
    # Delete the command message and the response after a delay
    await asyncio.sleep(3)
    await msg.delete()
    await cancel_msg.delete()


@router.message(F.chat.type.in_({ChatType.SUPERGROUP, ChatType.GROUP, ChatType.CHANNEL, ChatType.PRIVATE}))
async def main(msg: Message, bot: Bot):
    if not msg.audio:
        await msg.delete()
    print(f"{msg.from_user.id} (@{msg.from_user.username}) requested {msg.text}")
    text = msg.text
    if not text:
        return

    youtube_regex = (
        r"(?:https?://)?(?:www\.)?(?:m\.)?(?:youtube\.com|youtu\.be)/(?:watch\?v=|embed/|v/|playlist\?list=|)([\w-]{11}|list=[\w-]{34})(?:\S+)?"
    )
    match = re.search(youtube_regex, text)

    if match:
        user_id = msg.from_user.id

        # Initialize user's task list if not exists
        if user_id not in user_tasks:
            user_tasks[user_id] = []

        if user_id not in user_messages:
            user_messages[user_id] = []

        original_url = match.group(0)
        progress_msg = await msg.answer(
            f"<blockquote>{original_url}</blockquote>\n🛜 подготовка к скачиванию...",
            link_preview_options=LinkPreviewOptions(is_disabled=True),
            parse_mode="HTML",
        )
        db_analytics.add_user(msg.from_user.id)
        db_analytics.increment_use_count()

        # Create animation task and track it
        animation_task = asyncio.create_task(animate_starting_progress(progress_msg, original_url, bot))
        user_tasks[user_id].append(animation_task)
        user_messages[user_id].append(animation_task)

        # Create the main download task
        download_task = asyncio.create_task(
            process_download(msg, bot, original_url, progress_msg, animation_task, user_id))
        user_tasks[user_id].append(download_task)
        user_messages[user_id].append(download_task)

        # Set up cleanup when task completes
        download_task.add_done_callback(
            lambda t: user_tasks[user_id].remove(t) if user_id in user_tasks and t in user_tasks[user_id] else None
        )


async def process_download(msg, bot, original_url, progress_msg, animation_task, user_id):
    """Process the download as a separate task that can be cancelled"""
    # Use semaphore to limit concurrent downloads
    async with download_semaphore:
        try:
            ydl_opts = {
                "format": "bestaudio/best",
                "postprocessors": [
                    {
                        "key": "FFmpegExtractAudio",
                        "preferredcodec": "mp3",
                        "preferredquality": "192",
                    }
                ],
                "outtmpl": os.path.join(DOWNLOAD_DIR, "%(id)s.%(ext)s"),
                "quiet": True,
                "no_warnings": True,
                "ignoreerrors": True,
                "extract_flat": False,
            }

            # Extract info without downloading first
            with YoutubeDL(ydl_opts) as ydl:
                # Replace the info extraction part with:
                try:
                    with YoutubeDL(ydl_opts) as ydl:
                        info_dict = await run_in_threadpool(ydl.extract_info, original_url, download=False)
                except Exception as e:
                    print(f"Info extraction error (may be normal for cached content): {e}")
                    # If info extraction fails completely, we can't proceed
                    await progress_msg.edit_text(
                        f"<blockquote>{original_url}</blockquote>\n❌ не удалось получить информацию о видео",
                        link_preview_options=LinkPreviewOptions(is_disabled=True),
                        parse_mode="HTML",
                    )
                    return

            animation_task.cancel()

            # Check if it's a playlist
            if "_type" in info_dict and info_dict["_type"] == "playlist":
                playlist_title = info_dict.get("title", "Unknown Playlist")

                entries = info_dict.get("entries", [])
                if not entries:
                    await msg.answer("❌ ерор!!!\nПлейлист пуст или не удалось получить данные.")
                    await progress_msg.delete()
                    return

                for i, entry in enumerate(entries):
                    # Check if task was cancelled
                    if asyncio.current_task().cancelled():
                        raise asyncio.CancelledError()

                    if entry is None:
                        print(f"Skipping null entry in playlist {original_url}")
                        continue

                    video_url = entry.get("webpage_url")
                    if not video_url:
                        print(f"Could not get URL for entry {i + 1} in playlist {original_url}")
                        continue

                    video_id = entry.get("id")
                    title = entry.get("title", "<unknown>")
                    artist = entry.get("artist", entry.get("uploader", "<unknown>"))
                    thumbnail_url = entry.get("thumbnail")

                    # Check if file is cached
                    cached_file_id = db.get_file_id(video_id)
                    if cached_file_id:
                        try:
                            await progress_msg.delete()
                        except:
                            pass

                        if await send_cached_audio(msg, bot, video_id, cached_file_id, progress_msg):
                            continue  # Skip to next item if cached version sent successfully

                    audio_filepath = os.path.join(DOWNLOAD_DIR, f"{video_id}.mp3")

                    try:
                        try:
                            await progress_msg.delete()
                        except:
                            pass
                        if animation_task and not animation_task.done():
                            animation_task.cancel()

                        progress_msg = await msg.answer(
                            f"<blockquote>{original_url}</blockquote>\n⬇️ плейлист: скачивание...\n<i>({i + 1}/{len(entries)})</i> <b>{title}</b>",
                            link_preview_options=LinkPreviewOptions(is_disabled=True),
                            parse_mode="HTML", disable_notification=True,
                        )
                        user_messages[user_id].append(progress_msg)
                        animation_task = asyncio.create_task(
                            animate_progress(progress_msg, original_url, "⬇️ плейлист: скачивание",
                                             f"\n<i>({i + 1}/{len(entries)})</i> <b>{title}</b>", bot,
                                             ChatAction.RECORD_VIDEO))
                        user_tasks[user_id].append(animation_task)

                        # Check if task was cancelled
                        if asyncio.current_task().cancelled():
                            raise asyncio.CancelledError()

                        # Download asynchronously
                        await download_video(video_url, ydl_opts)

                        if os.path.exists(audio_filepath) and thumbnail_url:
                            animation_task.cancel()
                            await progress_msg.edit_text(
                                f"<blockquote>{original_url}</blockquote>\n✴️ плейлист: обработка...\n<i>({i + 1}/{len(entries)})</i> <b>{title}</b>",
                                link_preview_options=LinkPreviewOptions(is_disabled=True),
                                parse_mode="HTML",
                            )
                            animation_task = asyncio.create_task(
                                animate_progress(progress_msg, original_url, "✴️ плейлист: обработка",
                                                 f"\n<i>({i + 1}/{len(entries)})</i> <b>{title}</b>", bot,
                                                 ChatAction.UPLOAD_PHOTO))
                            user_tasks[user_id].append(animation_task)

                            # Check if task was cancelled
                            if asyncio.current_task().cancelled():
                                raise asyncio.CancelledError()

                            try:
                                # Process audio asynchronously
                                thumbnail_data = await process_audio(audio_filepath, title, artist, thumbnail_url)

                                animation_task.cancel()
                                await progress_msg.edit_text(
                                    f"<blockquote>{original_url}</blockquote>\n❇️ плейлист: отправка...\n<i>({i + 1}/{len(entries)})</i> <b>{title}</b>",
                                    link_preview_options=LinkPreviewOptions(is_disabled=True),
                                    parse_mode="HTML",
                                )
                                animation_task = asyncio.create_task(
                                    animate_progress(progress_msg, original_url, "❇️ плейлист: отправка",
                                                     f"\n<i>({i + 1}/{len(entries)})</i> <b>{title}</b>", bot,
                                                     ChatAction.UPLOAD_VOICE))
                                user_tasks[user_id].append(animation_task)

                                # Check if task was cancelled
                                if asyncio.current_task().cancelled():
                                    raise asyncio.CancelledError()

                                sent_message = await bot.send_audio(
                                    chat_id=msg.chat.id,
                                    audio=FSInputFile(audio_filepath),
                                    title=title,
                                    performer=artist,
                                    thumbnail=BufferedInputFile(
                                        thumbnail_data,
                                        filename=f"{video_id}_thumb.jpg",
                                    ),
                                    disable_notification=True,
                                )

                                # Save file_id to database
                                db.add_data(video_id, sent_message.audio.file_id)

                            except Exception as e:
                                animation_task.cancel()
                                await msg.answer(
                                    f"❌ ерор при обработке '{title}'!!!\n{e}"
                                )
                        else:
                            animation_task.cancel()
                            await msg.answer(
                                f"❌ ерор!!!\n404 ВИДЕО '{title}' НЕТ ютуб момент"
                            )

                    except asyncio.CancelledError:
                        # Handle cancellation
                        print(f"Download cancelled for user {user_id}")
                        if os.path.exists(audio_filepath):
                            os.remove(audio_filepath)
                        raise  # Re-raise to exit the function

                    except Exception as e:
                        animation_task.cancel()
                        error_msg = await msg.answer(f"❌ ерор при скачивании '{title}'!!!\n{e}")
                        user_messages[user_id].append(error_msg)
                        await asyncio.sleep(10)
                        try:
                            await error_msg.delete()
                            user_messages[user_id].remove(error_msg)
                        except Exception:
                            pass
                    finally:
                        if os.path.exists(audio_filepath):
                            os.remove(audio_filepath)
                            print(f"Cleaned up {audio_filepath}")
                # Delete the final progress message for the playlist after all items are sent
                if animation_task and not animation_task.done():
                    animation_task.cancel()
                try:
                    await progress_msg.delete()
                except:
                    pass
                done_msg = await msg.answer("✅ готово, плейлист полностью скачан")
                await asyncio.sleep(10)
                await done_msg.delete()

            # If it's a single video
            else:
                video_id = info_dict.get("id")
                title = info_dict.get("title", "<unknown>")
                artist = info_dict.get("artist", info_dict.get("uploader", "<unknown>"))
                thumbnail_url = info_dict.get("thumbnail")

                # Check if file is cached
                cached_file_id = db.get_file_id(video_id)
                if cached_file_id:
                    animation_task.cancel()
                    if await send_cached_audio(msg, bot, video_id, cached_file_id, progress_msg):
                        return  # Exit if cached version sent successfully

                audio_filepath = os.path.join(DOWNLOAD_DIR, f"{video_id}.mp3")

                await progress_msg.edit_text(
                    f"<blockquote>{original_url}</blockquote>\n⬇️ скачивание...",
                    link_preview_options=LinkPreviewOptions(is_disabled=True),
                    parse_mode="HTML",
                )
                animation_task = asyncio.create_task(
                    animate_progress(progress_msg, original_url, "⬇️ скачивание", "", bot, ChatAction.RECORD_VIDEO))
                user_tasks[user_id].append(animation_task)

                # Check if task was cancelled
                if asyncio.current_task().cancelled():
                    raise asyncio.CancelledError()

                # Download asynchronously
                await download_video(original_url, ydl_opts)

                if os.path.exists(audio_filepath) and thumbnail_url:
                    animation_task.cancel()
                    await progress_msg.edit_text(
                        f"<blockquote>{original_url}</blockquote>\n✴️ обработка...",
                        link_preview_options=LinkPreviewOptions(is_disabled=True),
                        parse_mode="HTML",
                    )
                    animation_task = asyncio.create_task(
                        animate_progress(progress_msg, original_url, "✴️ обработка", "", bot, ChatAction.UPLOAD_PHOTO))
                    user_tasks[user_id].append(animation_task)

                    # Check if task was cancelled
                    if asyncio.current_task().cancelled():
                        raise asyncio.CancelledError()

                    try:
                        # Process audio asynchronously
                        thumbnail_data = await process_audio(audio_filepath, title, artist, thumbnail_url)

                        animation_task.cancel()
                        await progress_msg.edit_text(
                            f"<blockquote>{original_url}</blockquote>\n❇️ отправка...",
                            link_preview_options=LinkPreviewOptions(is_disabled=True),
                            parse_mode="HTML",
                        )
                        animation_task = asyncio.create_task(
                            animate_progress(progress_msg, original_url, "❇️ отправка", "", bot,
                                             ChatAction.UPLOAD_VOICE))
                        user_tasks[user_id].append(animation_task)

                        # Check if task was cancelled
                        if asyncio.current_task().cancelled():
                            raise asyncio.CancelledError()

                        await bot.send_chat_action(
                            chat_id=msg.chat.id, action=ChatAction.UPLOAD_VOICE
                        )
                        sent_message = await bot.send_audio(
                            chat_id=msg.chat.id,
                            audio=FSInputFile(audio_filepath),
                            title=title,
                            performer=artist,
                            thumbnail=BufferedInputFile(
                                thumbnail_data,
                                filename=f"{video_id}_thumb.jpg",
                            ),
                        )

                        # Save file_id to database
                        db.add_data(video_id, sent_message.audio.file_id)

                        animation_task.cancel()
                        await progress_msg.delete()

                    except asyncio.CancelledError:
                        # Handle cancellation
                        print(f"Download cancelled for user {user_id}")
                        if os.path.exists(audio_filepath):
                            os.remove(audio_filepath)
                        raise  # Re-raise to exit the function

                    except Exception as e:
                        animation_task.cancel()
                        error_msg = await msg.answer(f"❌ ерор!!!\n{e}")
                        await asyncio.sleep(10)
                        await progress_msg.delete()
                        await error_msg.delete()
                else:
                    animation_task.cancel()
                    error_msg = await msg.answer(f"❌ ерор!!! такого видео нет")
                    await asyncio.sleep(10)
                    await progress_msg.delete()
                    await error_msg.delete()

        except asyncio.CancelledError:
            # Handle cancellation
            print(f"Download cancelled for user {user_id}")
            if animation_task and not animation_task.done():
                animation_task.cancel()
            await progress_msg.edit_text(
                f"<blockquote>{original_url}</blockquote>\n❌ отменено",
                link_preview_options=LinkPreviewOptions(is_disabled=True),
                parse_mode="HTML",
            )
            await asyncio.sleep(3)
            await progress_msg.delete()

        except Exception as e:
            if animation_task and not animation_task.done():
                animation_task.cancel()
            error_msg = await msg.answer(f"❌ ерор!!!\n{e}")
            await asyncio.sleep(10)
            await progress_msg.delete()
            await error_msg.delete()

        finally:
            if "audio_filepath" in locals() and os.path.exists(audio_filepath):
                os.remove(audio_filepath)
                print(f"Cleaned up {audio_filepath}")
            print(f"{msg.from_user.id} (@{msg.from_user.username})'s request is complete")