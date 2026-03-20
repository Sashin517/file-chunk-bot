#!/usr/bin/env python3
"""
Telegram File Splitter Bot
- Runs against a local Bot API server (no 20MB limit — up to 2GB)
- User forwards any file/video
- Bot downloads it on the server, splits into 500MB parts, sends back
- You only download small chunks, saving your internet data
"""

import os
import asyncio
import logging
import subprocess
import shutil
import time
from pathlib import Path

from telegram import Update
from telegram.ext import ApplicationBuilder, MessageHandler, CommandHandler, ContextTypes, filters
from telegram.constants import ParseMode
from telegram.error import TelegramError
from telegram.request import HTTPXRequest

BOT_TOKEN        = os.environ["BOT_TOKEN"]
LOCAL_SERVER_URL = os.environ.get("LOCAL_SERVER_URL", "http://localhost:8081")
DOWNLOAD_DIR     = Path(os.environ.get("DOWNLOAD_DIR", "/tmp/tg_splitter"))
SPLIT_SIZE_MB    = int(os.environ.get("SPLIT_SIZE_MB", "490"))
ALLOWED_IDS_RAW  = os.environ.get("ALLOWED_USER_IDS", "")
ALLOWED_IDS      = set(int(x.strip()) for x in ALLOWED_IDS_RAW.split(",") if x.strip())

logging.basicConfig(
    format="%(asctime)s | %(levelname)s | %(message)s",
    level=logging.INFO
)
log = logging.getLogger(__name__)
DOWNLOAD_DIR.mkdir(parents=True, exist_ok=True)


def is_allowed(user_id: int) -> bool:
    return not ALLOWED_IDS or user_id in ALLOWED_IDS


def human_size(num_bytes: int) -> str:
    for unit in ("B", "KB", "MB", "GB"):
        if num_bytes < 1024:
            return f"{num_bytes:.1f} {unit}"
        num_bytes /= 1024
    return f"{num_bytes:.1f} TB"


def split_file(input_path: Path, chunk_mb: int) -> list:
    chunk_bytes = chunk_mb * 1024 * 1024
    out_dir = input_path.parent / (input_path.stem + "_parts")
    out_dir.mkdir(exist_ok=True)
    suffix = input_path.suffix.lower()
    video_exts = {".mp4", ".mkv", ".avi", ".mov", ".flv", ".webm", ".ts", ".m4v"}

    if suffix in video_exts:
        pattern = str(out_dir / f"{input_path.stem}_part%03d{suffix}")
        cmd = [
            "ffmpeg", "-i", str(input_path),
            "-c", "copy", "-map", "0",
            "-segment_size", str(chunk_bytes),
            "-f", "segment",
            "-reset_timestamps", "1",
            pattern, "-y"
        ]
        log.info(f"ffmpeg splitting: {input_path.name}")
        result = subprocess.run(cmd, capture_output=True, text=True)
        if result.returncode != 0:
            log.warning(f"ffmpeg failed, binary split fallback: {result.stderr[:200]}")
            shutil.rmtree(out_dir, ignore_errors=True)
            out_dir.mkdir(exist_ok=True)
            return _binary_split(input_path, out_dir, chunk_bytes)
        return sorted(out_dir.glob(f"{input_path.stem}_part*{suffix}"))
    else:
        return _binary_split(input_path, out_dir, chunk_bytes)


def _binary_split(input_path: Path, out_dir: Path, chunk_bytes: int) -> list:
    parts = []
    suffix = input_path.suffix
    stem = input_path.stem
    with open(input_path, "rb") as f:
        part_num = 0
        while True:
            data = f.read(chunk_bytes)
            if not data:
                break
            part_path = out_dir / f"{stem}_part{part_num:03d}{suffix}"
            part_path.write_bytes(data)
            parts.append(part_path)
            part_num += 1
    return sorted(parts)


async def cmd_start(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "📦 *File Splitter Bot*\n\n"
        "Forward me any large file from any Telegram chat or channel.\n\n"
        "• Files ≤ 500 MB → sent back as-is\n"
        "• Files > 500 MB → split into 500 MB parts and sent one by one\n\n"
        "Max supported size: *2 GB* (Telegram's hard limit)\n\n"
        "You only download the small parts — saving your data 📶",
        parse_mode=ParseMode.MARKDOWN
    )


async def handle_file(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    if not is_allowed(user.id):
        await update.message.reply_text("❌ You are not authorized.")
        return

    msg = update.message
    tg_file = None
    filename = "file"

    if msg.document:
        tg_file = msg.document
        filename = msg.document.file_name or f"file_{msg.document.file_id}"
    elif msg.video:
        tg_file = msg.video
        filename = getattr(msg.video, "file_name", None) or f"video_{msg.video.file_id}.mp4"
    elif msg.audio:
        tg_file = msg.audio
        filename = getattr(msg.audio, "file_name", None) or f"audio_{msg.audio.file_id}.mp3"
    elif msg.voice:
        tg_file = msg.voice
        filename = f"voice_{msg.voice.file_id}.ogg"
    elif msg.video_note:
        tg_file = msg.video_note
        filename = f"videonote_{msg.video_note.file_id}.mp4"
    else:
        await msg.reply_text("⚠️ Please forward a file, video, or audio message.")
        return

    file_size = getattr(tg_file, "file_size", 0) or 0
    log.info(f"User {user.id} | '{filename}' | {human_size(file_size)}")

    status = await msg.reply_text(
        f"⏬ Downloading *{filename}* ({human_size(file_size)}) on server...\n"
        f"_Large files take a few minutes. Please wait._",
        parse_mode=ParseMode.MARKDOWN
    )

    local_path = DOWNLOAD_DIR / filename

    try:
        tg_file_obj = await ctx.bot.get_file(tg_file.file_id)
        await tg_file_obj.download_to_drive(str(local_path))
        actual_size = local_path.stat().st_size
        log.info(f"Downloaded: {local_path} ({human_size(actual_size)})")

        split_threshold = SPLIT_SIZE_MB * 1024 * 1024

        if actual_size <= split_threshold:
            await status.edit_text(
                f"📤 Sending *{filename}* ({human_size(actual_size)})...",
                parse_mode=ParseMode.MARKDOWN
            )
            await msg.reply_document(
                document=local_path,
                filename=filename,
                caption=f"✅ {filename} ({human_size(actual_size)})"
            )
            await status.delete()
            return

        await status.edit_text(
            f"✂️ Splitting *{filename}* ({human_size(actual_size)}) into {SPLIT_SIZE_MB} MB parts...",
            parse_mode=ParseMode.MARKDOWN
        )
        parts = split_file(local_path, SPLIT_SIZE_MB)
        total = len(parts)
        log.info(f"Split into {total} parts")

        for i, part in enumerate(parts, 1):
            part_size = part.stat().st_size
            await status.edit_text(
                f"📤 Sending part *{i}/{total}*: `{part.name}` ({human_size(part_size)})",
                parse_mode=ParseMode.MARKDOWN
            )
            await msg.reply_document(
                document=part,
                filename=part.name,
                caption=(
                    f"📦 *{filename}*\n"
                    f"Part {i} of {total} — {human_size(part_size)}"
                ),
                parse_mode=ParseMode.MARKDOWN
            )
            log.info(f"Sent part {i}/{total}")

        await status.edit_text(
            f"✅ Done! Sent *{total} parts* of *{filename}* ({human_size(actual_size)} total)\n\n"
            f"Download them one by one 📶",
            parse_mode=ParseMode.MARKDOWN
        )

    except TelegramError as e:
        log.exception("Telegram error")
        await status.edit_text(
            f"❌ Telegram error: `{e}`\n\n"
            "Files over 2GB cannot be forwarded — that is Telegram's hard limit.",
            parse_mode=ParseMode.MARKDOWN
        )
    except Exception as e:
        log.exception("Unexpected error")
        await status.edit_text(f"❌ Error: `{e}`", parse_mode=ParseMode.MARKDOWN)
    finally:
        if local_path.exists():
            local_path.unlink()
        parts_dir = DOWNLOAD_DIR / (Path(filename).stem + "_parts")
        if parts_dir.exists():
            shutil.rmtree(parts_dir, ignore_errors=True)


def wait_for_local_server(url: str, retries: int = 20, delay: float = 3.0):
    """Wait until the local Bot API server is ready."""
    import urllib.request
    for i in range(retries):
        try:
            urllib.request.urlopen(f"{url}/", timeout=2)
            log.info("Local Bot API server is ready.")
            return
        except Exception:
            log.info(f"Waiting for local server... ({i+1}/{retries})")
            time.sleep(delay)
    log.warning("Local server did not respond in time, starting anyway.")


def main():
    log.info(f"Connecting to local Bot API server at {LOCAL_SERVER_URL}")
    wait_for_local_server(LOCAL_SERVER_URL)

    app = (
        ApplicationBuilder()
        .token(BOT_TOKEN)
        .base_url(f"{LOCAL_SERVER_URL}/bot")
        .base_file_url(f"{LOCAL_SERVER_URL}/file/bot")
        .get_updates_request(HTTPXRequest(connection_pool_size=8))
        .build()
    )

    app.add_handler(CommandHandler("start", cmd_start))
    app.add_handler(CommandHandler("help", cmd_start))
    app.add_handler(MessageHandler(
        filters.Document.ALL | filters.VIDEO | filters.AUDIO |
        filters.VOICE | filters.VIDEO_NOTE,
        handle_file
    ))

    log.info("Bot started. Waiting for forwarded files...")
    app.run_polling(drop_pending_updates=True)


if __name__ == "__main__":
    main()
