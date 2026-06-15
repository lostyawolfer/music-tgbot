import asyncio
import os
import re
from typing import Optional, Dict, List, Any

from aiogram import Router, Bot, F
from aiogram.enums import ChatAction, ChatType
from aiogram.filters import Command
from aiogram.types import Message, FSInputFile, BufferedInputFile, LinkPreviewOptions
from aiogram.exceptions import TelegramBadRequest

from db.db import Music, Analytics
from data.config import env_bool, env_int
from modules.downloader import (
    yt_extract,
    make_ydl_opts,
    build_paths,
    rename_with_collision_avoidance,
)
from modules.processing import process_audio, cleanup_file
from modules.utils import safe_edit_text, remove_duplicate_artists
from modules.progress import animate_ellipsis, animate_download_progress, animate_countdown, animate_starting, format_download_text
import html

# ------------------------------------------------------------------------------
# Router & Database
# ------------------------------------------------------------------------------
router = Router()

DOWNLOAD_DIR = "downloads"
os.makedirs(DOWNLOAD_DIR, exist_ok=True)

db = Music()
db_analytics = Analytics()

# Track per-user running tasks and messages
user_tasks: Dict[int, List[asyncio.Task]] = {}
user_messages: Dict[int, List[Message]] = {}

# Global concurrent limit
MAX_CONCURRENT_DOWNLOADS = 5
download_semaphore = asyncio.Semaphore(MAX_CONCURRENT_DOWNLOADS)

# Single dictionary for progress tracking
download_progress: Dict[str, float] = {}

YOUTUBE_REGEX = (
    r"(?:https?://)?(?:www\.)?(?:m\.)?"
    r"(?:youtube\.com|youtu\.be)/(?:watch\?v=|embed/|v/|playlist\?list=|)"
    r"([\w-]{11}|list=[\w-]{34})(?:\S+)?"
)


# ------------------------------------------------------------------------------
# Premium (cookies) download rate limit
# ------------------------------------------------------------------------------
def premium_cooldown_seconds() -> int:
    # RATE_LIMIT is interpreted as cooldown seconds between premium downloads per user.
    # 0 / empty => disabled
    return env_int("RATE_LIMIT", 0)

def cookies_enabled() -> bool:
    return env_bool("COOKIES_ENABLED", False)
# ------------------------------------------------------------------------------
# Task tracking
# ------------------------------------------------------------------------------
def track_task(uid: int, task: asyncio.Task):
    user_tasks.setdefault(uid, []).append(task)


def untrack_task(uid: int, task: asyncio.Task):
    try:
        user_tasks[uid].remove(task)
    except Exception:
        pass


def track_message(uid: int, message: Message):
    user_messages.setdefault(uid, []).append(message)


async def cancel_user_tasks_and_messages(uid: int):
    for t in user_tasks.get(uid, []):
        if not t.done() and not t.cancelled():
            t.cancel()

    for m in user_messages.get(uid, []):
        try:
            await m.delete()
        except Exception:
            pass

    user_tasks[uid] = []
    user_messages[uid] = []

# ------------------------------------------------------------------------------
# Core file sending
# ------------------------------------------------------------------------------
def extract_video_id(url: str) -> Optional[str]:
    """Extract video ID from YouTube URL"""
    patterns = [
        r'(?:v=|/)([0-9A-Za-z_-]{11}).*',
        r'(?:embed/)([0-9A-Za-z_-]{11})',
        r'^([0-9A-Za-z_-]{11})$',
    ]
    for pattern in patterns:
        match = re.search(pattern, url)
        if match:
            return match.group(1)
    return None


async def send_cached_audio(msg: Message, bot: Bot, vid_id: str, file_id: str, progress_msg: Message) -> bool:
    try:
        await progress_msg.delete()
    except Exception:
        pass
    try:
        await bot.send_audio(chat_id=msg.chat.id, audio=file_id, disable_notification=True)
        return True
    except Exception:
        db.remove_data(vid_id)
        return False


async def send_processed_audio(
    bot: Bot,
    msg: Message,
    vid_id: str,
    audio_path: str,
    title: str,
    artist: str,
    thumb_data: bytes,
    silent: bool = False  # New parameter
) -> str:
    sent = await bot.send_audio(
        chat_id=msg.chat.id,
        audio=FSInputFile(audio_path),
        title=title,
        performer=artist,
        thumbnail=BufferedInputFile(thumb_data, filename=f"{vid_id}_thumb.jpg"),
        disable_notification=silent,
    )
    db.add_data(vid_id, sent.audio.file_id)
    return sent.audio.file_id

# ------------------------------------------------------------------------------
# Video processing pipeline
# ------------------------------------------------------------------------------
async def process_single_video(
        msg: Message,
        bot: Bot,
        original_url: str,
        info: Dict[str, Any],
        progress_msg: Optional[Message],
        user_id: int,
        skip_db_check: bool = False,
        is_from_playlist: bool = False  # New parameter
):
    vid_id = info.get("id")
    title = info.get("title", "<unknown>")
    artist = remove_duplicate_artists(info.get("artist", info.get("uploader", "<unknown>")))
    thumb_url = info.get("thumbnail")

    # Format display name with link
    display_name = f'<blockquote><a href="{original_url}">{title}</a></blockquote>'

    # Check cache first if not skipped
    if not skip_db_check:
        cached_id = db.get_file_id(vid_id)
        if cached_id:
            try:
                await bot.send_audio(
                    chat_id=msg.chat.id,
                    audio=cached_id,
                    disable_notification=is_from_playlist  # Silent if from playlist
                )
                if progress_msg:
                    try:
                        await progress_msg.delete()
                    except Exception:
                        pass
                return
            except Exception:
                db.remove_data(vid_id)

    # Now create progress message if it doesn't exist
    if not progress_msg:
        progress_msg = await msg.answer(
            f"{display_name}\n⬇️ скачивание...",
            link_preview_options=LinkPreviewOptions(is_disabled=True),
            parse_mode="HTML",
        )
        track_message(user_id, progress_msg)

    temp_path, final_path = build_paths(vid_id, title)
    download_progress[vid_id] = 0.0

    filesize = info.get("filesize") or info.get("filesize_approx") or 0
    # Telegram limit 50 MB, add small margin
    if filesize > 48 * 1024 * 1024:
        await safe_edit_text(
            progress_msg,
            f"{display_name}\n⛔️ файл слишком большой для телеграма <i>({round(filesize / 1024 / 1024, 1)} MB)</i>",
            link_preview_options=LinkPreviewOptions(is_disabled=True),
            parse_mode="HTML"
        )
        await asyncio.sleep(7)
        await progress_msg.delete()
        return

    # Update with display name
    await safe_edit_text(
        progress_msg,
        f"{display_name}\n⬇️ скачивание...",
        link_preview_options=LinkPreviewOptions(is_disabled=True),
        parse_mode="HTML",
    )

    anim_task = asyncio.create_task(animate_download_progress(
        progress_msg, display_name, vid_id, bot, download_progress, is_playlist=is_from_playlist
    ))
    track_task(user_id, anim_task)

    # Premium rate limit (only when cookies mode is enabled)
    if cookies_enabled():
        cooldown = premium_cooldown_seconds()
        wait = db_analytics.get_premium_wait_seconds(user_id, cooldown)
        if wait > 0:
            err_txt = f"⏳ лимит premium: подожди {wait} сек"
            await safe_edit_text(
                progress_msg,
                f"{display_name}\n{err_txt} <i>15</i>",
                link_preview_options=LinkPreviewOptions(is_disabled=True),
                parse_mode="HTML",
            )
            await animate_countdown(progress_msg, err_txt, 15, display_name)
            return
        # Reserve the slot immediately to avoid bursts
        db_analytics.mark_premium_download(user_id)

    try:
        ydl_opts = make_ydl_opts(vid_id, download_progress)
        await yt_extract(info.get("webpage_url", original_url), ydl_opts, download=True)

        if not (os.path.exists(temp_path) and thumb_url):
            raise RuntimeError(f"404 - видео '{title}' не существует")

        try:
            final_path = rename_with_collision_avoidance(temp_path, final_path)
        except Exception:
            final_path = temp_path

        anim_task.cancel()
        await progress_msg.edit_text(
            f"{display_name}\n✴️ обработка...",
            link_preview_options=LinkPreviewOptions(is_disabled=True),
            parse_mode="HTML",
        )
        anim_task = asyncio.create_task(animate_ellipsis(
            progress_msg, display_name, "✴️ обработка", "", bot, ChatAction.UPLOAD_PHOTO
        ))
        track_task(user_id, anim_task)

        thumb_data = await process_audio(final_path, title, artist, thumb_url)

        anim_task.cancel()
        await progress_msg.edit_text(
            f"{display_name}\n❇️ отправка...",
            link_preview_options=LinkPreviewOptions(is_disabled=True),
            parse_mode="HTML",
        )

        sent_file_id = await send_processed_audio(
            bot, msg, vid_id, final_path, title, artist, thumb_data, is_from_playlist
        )

        try:
            await progress_msg.delete()
        except Exception:
            pass

    except asyncio.CancelledError:
        try:
            anim_task.cancel()
        except Exception:
            pass
        cleanup_file(temp_path)
        cleanup_file(final_path)
        raise
    except Exception as e:
        try:
            anim_task.cancel()
        except Exception:
            pass
        err_txt = "⛔️ ошибка обработки"
        error_details = html.escape(str(e))
        try:
            await progress_msg.edit_text(
                f"{display_name}\n{err_txt} <i>15</i>\n<pre><code>{error_details}</code></pre>",
                link_preview_options=LinkPreviewOptions(is_disabled=True),
                parse_mode="HTML",
            )
        except Exception:
            progress_msg = await msg.answer(
                f"{display_name}\n{err_txt} <i>15</i>\n<pre><code>{error_details}</code></pre>",
                link_preview_options=LinkPreviewOptions(is_disabled=True),
                parse_mode="HTML",
            )
            track_message(user_id, progress_msg)
        await animate_countdown(progress_msg, err_txt, 15, display_name, error_details)
    finally:
        download_progress.pop(vid_id, None)
        cleanup_file(temp_path)
        cleanup_file(final_path)

# ------------------------------------------------------------------------------
# Entry point for URLs
# ------------------------------------------------------------------------------
# Update handle_url:
async def handle_url(msg: Message, bot: Bot, original_url: str, user_id: int):
    # yt-dlp is much more reliable when the URL has an explicit scheme.
    # Users often paste `youtube.com/watch?v=...` without `https://`.
    if original_url and not re.match(r'^https?://', original_url, re.IGNORECASE):
        original_url = 'https://' + original_url.lstrip('/')

    is_playlist = "list=" in original_url or "/playlist" in original_url

    # For single videos, try DB check before ANY messages
    if not is_playlist:
        vid_id = extract_video_id(original_url)
        if vid_id:
            cached_id = db.get_file_id(vid_id)
            if cached_id:
                db_analytics.add_user(msg.from_user.id)
                db_analytics.increment_use_count()
                try:
                    await bot.send_audio(chat_id=msg.chat.id, audio=cached_id, disable_notification=False)
                    return
                except Exception:
                    db.remove_data(vid_id)

    progress_msg = await msg.answer(
        f"<blockquote>{original_url}</blockquote>\n🛜 достаю {'плейлист' if is_playlist else 'видео'}...",
        link_preview_options=LinkPreviewOptions(is_disabled=True),
        parse_mode="HTML",
    )
    track_message(user_id, progress_msg)

    db_analytics.add_user(msg.from_user.id)
    db_analytics.increment_use_count()

    start_anim = asyncio.create_task(animate_starting(progress_msg, original_url, bot, is_playlist))
    track_task(user_id, start_anim)

    async with download_semaphore:
        try:
            info = await yt_extract(original_url, make_ydl_opts(), download=False)
        except Exception as e:
            start_anim.cancel()
            err_txt = "⛔️ не удалось получить информацию о видео"
            # Show a short error detail to make debugging possible.
            details = html.escape(str(e))
            try:
                await progress_msg.edit_text(
                    f"<blockquote>{original_url}</blockquote>\n{err_txt} <i>15</i>\n<pre><code>{details}</code></pre>",
                    link_preview_options=LinkPreviewOptions(is_disabled=True),
                    parse_mode="HTML",
                )
            except Exception:
                pass
            await animate_countdown(progress_msg, err_txt, 15, original_url, details)
            return

        start_anim.cancel()

        if info.get("_type") == "playlist":
            entries = info.get("entries", []) or []
            if not entries:
                await animate_countdown(progress_msg, "⛔️ плейлист пуст", 15, original_url)
                return

            # Get playlist name and create display link
            playlist_title = info.get("title", "??????")
            playlist_display = f'<blockquote><b><a href="{original_url}">{playlist_title}</a></b></blockquote>'

            total = len(entries)
            await safe_edit_text(
                progress_msg,
                f"{playlist_display}\n📋 скачиваем плейлист <i>(0/{total})</i>",
                link_preview_options=LinkPreviewOptions(is_disabled=True),
                parse_mode="HTML",
            )

            try:
                await bot.pin_chat_message(
                    chat_id=msg.chat.id,
                    message_id=progress_msg.message_id,
                    disable_notification=True  # This prevents the "pinned message" service message
                )
            except Exception:
                pass

            for idx, entry in enumerate(entries, start=1):
                if asyncio.current_task().cancelled():
                    raise asyncio.CancelledError()
                if not entry or not entry.get("webpage_url"):
                    continue

                await safe_edit_text(
                    progress_msg,
                    f"{playlist_display}\n📋 скачиваем плейлист <i>({idx}/{total} - отменить /cancel)</i>",
                    link_preview_options=LinkPreviewOptions(is_disabled=True),
                    parse_mode="HTML",
                )

                # Check DB before creating message
                vid_id = entry.get("id")
                cached_id = db.get_file_id(vid_id) if vid_id else None
                if cached_id:
                    try:
                        await bot.send_audio(
                            chat_id=msg.chat.id,
                            audio=cached_id,
                            disable_notification=True  # Silent for playlist
                        )
                        continue
                    except Exception:
                        db.remove_data(vid_id)

                video_title = entry.get("title", "<unknown>")
                video_display = f'<blockquote><a href="{entry["webpage_url"]}">{video_title}</a></blockquote>'

                video_msg = await msg.answer(
                    f"{video_display}\n⬇️ скачивание...",
                    link_preview_options=LinkPreviewOptions(is_disabled=True),
                    parse_mode="HTML",
                )
                track_message(user_id, video_msg)
                await process_single_video(
                    msg, bot, entry["webpage_url"], entry, video_msg, user_id,
                    skip_db_check=True, is_from_playlist=True
                )

            try:
                await progress_msg.delete()
            except Exception:
                pass
            # Final message with sound notification
            done = await msg.answer(
                "✅ готово, плейлист полностью скачан <i>15</i>",
                parse_mode="HTML",
                disable_notification=False  # Sound notification for completion
            )
            await animate_countdown(done, "✅ готово, плейлист полностью скачан", 30)

        else:
            # Single video - get title and update display
            video_title = info.get("title", "<unknown>")
            video_display = f'<blockquote><a href="{original_url}">{video_title}</a></blockquote>'

            await safe_edit_text(
                progress_msg,
                f"{video_display}\n⬇️ скачивание...",
                link_preview_options=LinkPreviewOptions(is_disabled=True),
                parse_mode="HTML",
            )

            await process_single_video(
                msg, bot, original_url, info, progress_msg, user_id, is_from_playlist=False
            )
            try:
                await progress_msg.delete()
            except Exception:
                pass

# ------------------------------------------------------------------------------
# Commands
# ------------------------------------------------------------------------------
@router.message(Command(commands=["start"]))
async def start(msg: Message):
    await msg.answer(
        """<b><u>lostya's youtube music downloader</u></b>
скачивает аудио с ютуба

нужно скачать видео?
@lostyawolfer_video_bot :)

<b><i>КАК ПОЛЬЗОВАТЬСЯ:</i></b>
<blockquote>- скинь ссылку на видео или плейлист ютуб
- бот скачаeт звук, добавит превью, исполнителя и название
- можно отправлять несколько ссылок
- /cancel отменяет все загрузки
- сообщения прогресса анимированы
- повторные загрузки ранее загружавшихся видео мгновенны</blockquote>""",
        parse_mode="HTML",
    )


@router.message(Command(commands=["analytics"]))
async def send_analytics(msg: Message):
    if msg.from_user.id != 653632008:
        await msg.delete()
        return
    await msg.delete()
    a = await msg.answer(
        f"бот скачал {db_analytics.get_total_use_count()} файлов по запросам от {db_analytics.get_user_count()} пользователей"
    )
    await asyncio.sleep(5)
    await a.delete()


@router.message(Command(commands=["cancel"]))
async def cancel_downloads(msg: Message):
    await msg.delete()
    user_id = msg.from_user.id
    if not user_tasks.get(user_id):
        m = await msg.answer("✅ нечего отменять <i>5</i>", parse_mode="HTML")
        await animate_countdown(m, "✅ нечего отменять", 5)
        return
    await cancel_user_tasks_and_messages(user_id)
    m = await msg.answer("✅ отменено! <i>5</i>", parse_mode="HTML")
    await animate_countdown(m, "✅ отменено!", 5)

# ------------------------------------------------------------------------------
# Main handler
# ------------------------------------------------------------------------------
@router.message(F.chat.type.in_({ChatType.SUPERGROUP, ChatType.GROUP, ChatType.CHANNEL, ChatType.PRIVATE}))
async def main(msg: Message, bot: Bot):
    if not msg.audio:
        try:
            await msg.delete()
        except Exception:
            pass

    if not msg.text:
        return

    print(f"{msg.from_user.id} (@{msg.from_user.username}) requested {msg.text}")
    match = re.search(YOUTUBE_REGEX, msg.text)
    if not match:
        return

    user_id = msg.from_user.id
    user_tasks.setdefault(user_id, [])
    user_messages.setdefault(user_id, [])

    url = match.group(0)
    task = asyncio.create_task(handle_url(msg, bot, url, user_id))
    track_task(user_id, task)
    task.add_done_callback(lambda t: untrack_task(user_id, t))