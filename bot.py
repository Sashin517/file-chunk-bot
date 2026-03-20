#!/usr/bin/env python3
"""
Telegram File Splitter Bot — with live status + extended timeouts
"""

import os
import asyncio
import logging
import subprocess
import shutil
import time
from pathlib import Path

from telegram import Update, Message
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

# Generous timeouts — large files need time to upload back
READ_TIMEOUT    = 600   # 10 min per read chunk
WRITE_TIMEOUT   = 600   # 10 min per write chunk
CONNECT_TIMEOUT = 30
POOL_TIMEOUT    = 60

logging.basicConfig(format="%(asctime)s | %(levelname)s | %(message)s", level=logging.INFO)
log = logging.getLogger(__name__)
DOWNLOAD_DIR.mkdir(parents=True, exist_ok=True)


# ── Helpers ────────────────────────────────────────────────────────────────────

def is_allowed(user_id: int) -> bool:
    return not ALLOWED_IDS or user_id in ALLOWED_IDS

def human_size(num_bytes: int) -> str:
    if num_bytes <= 0:
        return "? B"
    for unit in ("B", "KB", "MB", "GB"):
        if num_bytes < 1024:
            return f"{num_bytes:.1f} {unit}"
        num_bytes /= 1024
    return f"{num_bytes:.1f} TB"

def progress_bar(fraction: float, width: int = 16) -> str:
    filled = int(fraction * width)
    return "█" * filled + "░" * (width - filled)

def elapsed(start: float) -> str:
    s = int(time.time() - start)
    return f"{s // 60}m {s % 60}s" if s >= 60 else f"{s}s"


# ── Live status message ────────────────────────────────────────────────────────

class LiveStatus:
    """Edits one Telegram message every ~2s so user sees live progress."""
    def __init__(self, message: Message):
        self.message  = message
        self._text    = ""
        self._running = False
        self._task    = None

    async def start(self, text: str):
        self._text    = text
        self._running = True
        await self._edit(text)
        self._task = asyncio.create_task(self._loop())

    async def update(self, text: str):
        self._text = text

    async def finish(self, text: str):
        self._running = False
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        await self._edit(text)

    async def _loop(self):
        while self._running:
            await asyncio.sleep(2)
            if self._running:
                await self._edit(self._text)

    async def _edit(self, text: str):
        try:
            await self.message.edit_text(text, parse_mode=ParseMode.MARKDOWN)
        except Exception:
            pass


# ── Download with progress ─────────────────────────────────────────────────────

async def download_with_progress(tg_file_obj, local_path: Path, total_size: int,
                                  status: LiveStatus, filename: str, start_time: float):
    done = asyncio.Event()

    async def watcher():
        while not done.is_set():
            await asyncio.sleep(1.5)
            if local_path.exists() and total_size > 0:
                written = local_path.stat().st_size
                frac    = min(written / total_size, 1.0)
                t       = max(time.time() - start_time, 1)
                speed   = written / t
                eta     = int((total_size - written) / speed) if speed > 0 else 0
                await status.update(
                    f"⏬ *Downloading* `{filename}`\n\n"
                    f"`{progress_bar(frac)}` {frac*100:.0f}%\n\n"
                    f"{human_size(written)} / {human_size(total_size)}\n"
                    f"Speed: {human_size(int(speed))}/s\n"
                    f"ETA: {eta}s   Elapsed: {elapsed(start_time)}"
                )

    watcher_task = asyncio.create_task(watcher())
    try:
        await tg_file_obj.download_to_drive(str(local_path))
    finally:
        done.set()
        watcher_task.cancel()
        try:
            await watcher_task
        except asyncio.CancelledError:
            pass


# ── Upload with progress ───────────────────────────────────────────────────────

async def send_file_with_progress(
    msg: Message,
    file_path: Path,
    filename: str,
    caption: str,
    status: LiveStatus,
    status_prefix: str,
    start_time: float
):
    """Send a file back to user, updating status while upload runs."""
    file_size = file_path.stat().st_size
    upload_start = time.time()
    done = asyncio.Event()

    async def watcher():
        # We can't directly track upload bytes, so show a spinner with elapsed time
        frames = ["⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧", "⠇", "⠏"]
        i = 0
        while not done.is_set():
            await asyncio.sleep(1.5)
            if not done.is_set():
                await status.update(
                    f"{status_prefix}\n\n"
                    f"📤 Uploading `{filename}`\n"
                    f"{frames[i % len(frames)]} {human_size(file_size)} — {elapsed(upload_start)} uploading\n"
                    f"⏱ Total: {elapsed(start_time)}"
                )
                i += 1

    watcher_task = asyncio.create_task(watcher())
    try:
        await msg.reply_document(
            document=file_path,
            filename=filename,
            caption=caption,
            parse_mode=ParseMode.MARKDOWN,
            read_timeout=READ_TIMEOUT,
            write_timeout=WRITE_TIMEOUT,
            connect_timeout=CONNECT_TIMEOUT,
        )
    finally:
        done.set()
        watcher_task.cancel()
        try:
            await watcher_task
        except asyncio.CancelledError:
            pass


# ── File splitting ─────────────────────────────────────────────────────────────

def split_file(input_path: Path, chunk_mb: int) -> list:
    chunk_bytes = chunk_mb * 1024 * 1024
    out_dir     = input_path.parent / (input_path.stem + "_parts")
    out_dir.mkdir(exist_ok=True)
    suffix      = input_path.suffix.lower()
    video_exts  = {".mp4", ".mkv", ".avi", ".mov", ".flv", ".webm", ".ts", ".m4v"}

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
        result = subprocess.run(cmd, capture_output=True, text=True)
        if result.returncode != 0:
            log.warning("ffmpeg failed, binary split fallback")
            shutil.rmtree(out_dir, ignore_errors=True)
            out_dir.mkdir(exist_ok=True)
            return _binary_split(input_path, out_dir, chunk_bytes)
        return sorted(out_dir.glob(f"{input_path.stem}_part*{suffix}"))
    return _binary_split(input_path, out_dir, chunk_bytes)

def _binary_split(input_path: Path, out_dir: Path, chunk_bytes: int) -> list:
    parts = []
    with open(input_path, "rb") as f:
        i = 0
        while chunk := f.read(chunk_bytes):
            p = out_dir / f"{input_path.stem}_part{i:03d}{input_path.suffix}"
            p.write_bytes(chunk)
            parts.append(p)
            i += 1
    return sorted(parts)


# ── Handlers ───────────────────────────────────────────────────────────────────

async def cmd_start(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "📦 *File Splitter Bot*\n\n"
        "Forward me any large file from any Telegram chat or channel.\n\n"
        "• Files ≤ 500 MB → sent back as-is\n"
        "• Files > 500 MB → split into 500 MB parts, sent one by one\n"
        "• Max size: *2 GB* (Telegram hard limit)\n\n"
        "You only download small chunks — saving your data 📶",
        parse_mode=ParseMode.MARKDOWN
    )


async def handle_file(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    if not is_allowed(user.id):
        await update.message.reply_text("❌ Not authorized.")
        return

    msg      = update.message
    tg_file  = None
    filename = "file"

    if msg.document:
        tg_file  = msg.document
        filename = msg.document.file_name or f"file_{msg.document.file_id}"
    elif msg.video:
        tg_file  = msg.video
        filename = getattr(msg.video, "file_name", None) or f"video_{msg.video.file_id}.mp4"
    elif msg.audio:
        tg_file  = msg.audio
        filename = getattr(msg.audio, "file_name", None) or f"audio_{msg.audio.file_id}.mp3"
    elif msg.voice:
        tg_file  = msg.voice
        filename = f"voice_{msg.voice.file_id}.ogg"
    elif msg.video_note:
        tg_file  = msg.video_note
        filename = f"videonote_{msg.video_note.file_id}.mp4"
    else:
        await msg.reply_text("⚠️ Please forward a file, video, or audio.")
        return

    file_size  = getattr(tg_file, "file_size", 0) or 0
    start_time = time.time()
    log.info(f"User {user.id} | '{filename}' | {human_size(file_size)}")

    raw    = await msg.reply_text("📥 Received — starting...")
    status = LiveStatus(raw)
    await status.start(
        f"⏬ *Downloading* `{filename}`\n\n"
        f"`{'░' * 16}` 0%\n\n"
        f"Size: {human_size(file_size)}\n"
        f"Starting download on server..."
    )

    local_path = DOWNLOAD_DIR / filename

    try:
        # ── Download ──────────────────────────────────────────────────────────
        tg_file_obj = await ctx.bot.get_file(
            tg_file.file_id,
            read_timeout=READ_TIMEOUT,
            write_timeout=WRITE_TIMEOUT,
        )
        await download_with_progress(
            tg_file_obj, local_path, file_size, status, filename, start_time
        )

        actual_size  = local_path.stat().st_size
        dl_time      = elapsed(start_time)
        split_thresh = SPLIT_SIZE_MB * 1024 * 1024

        # ── Small file — send directly ────────────────────────────────────────
        if actual_size <= split_thresh:
            prefix = (
                f"✅ *Downloaded in {dl_time}*\n\n"
                f"`{filename}` — {human_size(actual_size)}\n"
            )
            await send_file_with_progress(
                msg, local_path, filename,
                f"📦 {filename}  |  {human_size(actual_size)}",
                status, prefix, start_time
            )
            await status.finish(
                f"✅ *Done!*\n\n"
                f"`{filename}`\n"
                f"{human_size(actual_size)} — delivered in {elapsed(start_time)}"
            )
            return

        # ── Large file — split ────────────────────────────────────────────────
        await status.update(
            f"✅ *Downloaded in {dl_time}*\n\n"
            f"`{filename}` — {human_size(actual_size)}\n\n"
            f"✂️ Splitting into {SPLIT_SIZE_MB} MB parts...\n"
            f"_(lossless, no quality loss)_"
        )

        split_start = time.time()
        parts       = await asyncio.get_event_loop().run_in_executor(
            None, split_file, local_path, SPLIT_SIZE_MB
        )
        total      = len(parts)
        split_time = elapsed(split_start)
        log.info(f"Split into {total} parts in {split_time}")

        # ── Send each part ────────────────────────────────────────────────────
        for i, part in enumerate(parts, 1):
            part_size = part.stat().st_size
            prefix = (
                f"✅ *Downloaded* in {dl_time}\n"
                f"✂️ *Split* in {split_time} — {total} parts\n\n"
                f"Part *{i} / {total}* — {human_size(part_size)}\n"
            )
            await send_file_with_progress(
                msg, part, part.name,
                f"📦 *{filename}*\nPart {i} of {total} — {human_size(part_size)}",
                status, prefix, start_time
            )
            log.info(f"Sent part {i}/{total}")

        await status.finish(
            f"✅ *All done!*\n\n"
            f"`{filename}`\n"
            f"{human_size(actual_size)} → {total} parts\n\n"
            f"⬇ Download:  {dl_time}\n"
            f"✂️ Split:      {split_time}\n"
            f"⏱ Total:      {elapsed(start_time)}\n\n"
            f"Download parts one by one to save your data 📶"
        )

    except TelegramError as e:
        log.exception("Telegram error")
        await status.finish(
            f"❌ *Telegram error*\n\n`{e}`\n\n"
            f"If this keeps happening, try a smaller file first to test."
        )
    except Exception as e:
        log.exception("Unexpected error")
        await status.finish(f"❌ *Error*\n\n`{e}`")
    finally:
        if local_path.exists():
            local_path.unlink()
        parts_dir = DOWNLOAD_DIR / (Path(filename).stem + "_parts")
        if parts_dir.exists():
            shutil.rmtree(parts_dir, ignore_errors=True)


# ── Startup ────────────────────────────────────────────────────────────────────

def wait_for_local_server(url: str, retries: int = 20, delay: float = 3.0):
    import urllib.request
    for i in range(retries):
        try:
            urllib.request.urlopen(f"{url}/", timeout=2)
            log.info("Local Bot API server ready.")
            return
        except Exception:
            log.info(f"Waiting for local server... ({i+1}/{retries})")
            time.sleep(delay)
    log.warning("Local server did not respond, starting anyway.")


def main():
    log.info(f"Connecting to local Bot API server at {LOCAL_SERVER_URL}")
    wait_for_local_server(LOCAL_SERVER_URL)

    # Extended timeouts on the application level too
    request = HTTPXRequest(
        connection_pool_size=8,
        read_timeout=READ_TIMEOUT,
        write_timeout=WRITE_TIMEOUT,
        connect_timeout=CONNECT_TIMEOUT,
        pool_timeout=POOL_TIMEOUT,
    )

    app = (
        ApplicationBuilder()
        .token(BOT_TOKEN)
        .base_url(f"{LOCAL_SERVER_URL}/bot")
        .base_file_url(f"{LOCAL_SERVER_URL}/file/bot")
        .get_updates_request(request)
        .request(request)
        .build()
    )

    app.add_handler(CommandHandler("start", cmd_start))
    app.add_handler(CommandHandler("help", cmd_start))
    app.add_handler(MessageHandler(
        filters.Document.ALL | filters.VIDEO | filters.AUDIO |
        filters.VOICE | filters.VIDEO_NOTE,
        handle_file
    ))

    log.info("Bot started.")
    app.run_polling(drop_pending_updates=True)


if __name__ == "__main__":
    main()
