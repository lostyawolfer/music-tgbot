import os
import asyncio
import logging
from yt_dlp import YoutubeDL
from typing import Any, Dict, Optional

from data.config import env_bool, env_str, configfile
from .utils import run_in_threadpool, sanitize_filename

yt_dlp_logger = logging.getLogger("yt_dlp")
yt_dlp_logger.setLevel(logging.ERROR)


def create_progress_hook(video_id: str, progress_dict: dict):
    def hook(d):
        if d.get("status") == "downloading":
            total = d.get("total_bytes") or d.get("total_bytes_estimate", 0)
            if total:
                progress_dict[video_id] = (d["downloaded_bytes"] / total) * 100
        elif d.get("status") == "finished":
            progress_dict[video_id] = 100.0
    return hook


def make_ydl_opts(video_id=None, progress_dict=None) -> Dict[str, Any]:
    """
    Default downloader options.

    Three independent toggles control the advanced yt-dlp features:
      - COOKIES_ENABLED     → only sets cookiefile from COOKIES_PATH
      - POT_PROVIDER        → enables bgutil POT provider (extractor arg)
      - JS_RUNTIME          → sets js_runtimes + remote_components for EJS flow
    """
    cookies_enabled = env_bool("COOKIES_ENABLED", False)
    cookies_path = env_str("COOKIES_PATH")
    pot_provider = env_bool("POT_PROVIDER", False)
    js_runtime_raw = env_str("JS_RUNTIME")

    opts: Dict[str, Any] = {
        "format": "bestaudio",
        "postprocessors": [
            {
                "key": "FFmpegExtractAudio",
                "preferredcodec": "mp3",
                "preferredquality": "0",
            }
        ],
        "outtmpl": os.path.join("downloads", "%(id)s.%(ext)s"),
        "quiet": True,
        "no_warnings": True,
        "ignoreerrors": True,
    }

    if cookies_enabled and cookies_path:
        opts["cookiefile"] = cookies_path

    if pot_provider:
        opts.setdefault("extractor_args", {}).setdefault("youtube", {})
        opts["extractor_args"]["youtube"]["pot_provider"] = ["bgutil"]

    if js_runtime_raw:
        parts = js_runtime_raw.split(":", 1)
        alias = parts[0]
        if len(parts) > 1:
            opts["js_runtimes"] = {alias: parts[1]}
        else:
            opts["js_runtimes"] = {alias: {}}
        opts["remote_components"] = {"ejs:github"}

    if video_id and progress_dict is not None:
        opts["progress_hooks"] = [create_progress_hook(video_id, progress_dict)]
    return opts


async def yt_extract(url: str, ydl_opts: Dict[str, Any], download=True):
    with YoutubeDL(ydl_opts) as ydl:
        return await run_in_threadpool(ydl.extract_info, url, download)


def build_paths(video_id: str, title: str) -> tuple[str, str]:
    tmp = os.path.join("downloads", f"{video_id}.mp3")
    final = os.path.join("downloads", f"{sanitize_filename(title)}.mp3")
    return tmp, final


def rename_with_collision_avoidance(src: str, desired: str) -> str:
    if not os.path.exists(desired):
        os.rename(src, desired)
        return desired
    base, ext = os.path.splitext(desired)
    i = 1
    while os.path.exists(f"{base}_{i}{ext}"):
        i += 1
    dst = f"{base}_{i}{ext}"
    os.rename(src, dst)
    return dst
