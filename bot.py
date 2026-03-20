#!/usr/bin/env python3
"""
Telegram File Splitter Bot — fixed progress tracking + split timeout
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

READ_TIMEOUT    = 600
WRITE_TIMEOUT   = 600
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
    filled = int(min(fraction, 1.0) * width)
    return "█" * filled + "░" * (width - filled)

def elapsed(start: float) -> str:
    s = int(time.time() - start)
    return f"{s // 60}m {s % 60}s" if s >= 60 else f"{s}s"

def spinner_frame(i: int) -> str:
    return ["⠋","⠙","⠹","⠸","⠼","⠴","⠦","⠧","⠇","⠏"][i % 10]


# ── Live status message ────────────────────────────────────────────────────────

class LiveStatus:
    """Edits one Telegram message every ~2s."""
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


# ── Download with real progress ────────────────────────────────────────────────

async def download_with_progress(
    tg_file_obj,
    local_path: Path,
    total_size: int,
    status: LiveStatus,
    filename: str,
    start_time: float
):
    """
    The local Bot API server writes to its own internal temp dir,
    then atomically moves the file to our path at the very end.
    So we can't watch local_path grow during download.

    Instead: show a time-based spinner with estimated progress
    using a constant assumed speed, updated every 2s.
    This gives the user real feedback that something is happening.
    """
    done       = asyncio.Event()
    spin_i     = 0

    # Try to estimate speed from a rough baseline of 20 MB/s server connection
    # Will self-correct once file lands and we know actual elapsed time
    async def watcher():
        nonlocal spin_i
        assumed_speed = 15 * 1024 * 1024  # 15 MB/s starting estimate
        while not done.is_set():
            await asyncio.sleep(1.8)
            if done.is_set():
                break
            t        = max(time.time() - start_time, 1)
            # Estimate bytes downloaded based on elapsed time + assumed speed
            est_done = min(t * assumed_speed, total_size * 0.95) if total_size > 0 else 0
            frac     = (est_done / total_size) if total_size > 0 else 0
            eta      = int((total_size - est_done) / assumed_speed) if total_size > 0 else 0
            spin_i  += 1
            await status.update(
                f"⏬ *Downloading* `{filename}`\n\n"
                f"`{progress_bar(frac)}` ~{frac*100:.0f}%\n\n"
                f"~{human_size(int(est_done))} / {human_size(total_size)}\n"
                f"{spinner_frame(spin_i)} Elapsed: {elapsed(start_time)}   ETA: ~{eta}s\n"
                f"_(estimate — server is downloading)_"
            )

    watcher_task = asyncio.create_task(watcher())
    dl_start = time.time()
    try:
        await tg_file_obj.download_to_drive(str(local_path))
    finally:
        done.set()
        watcher_task.cancel()
        try:
            await watcher_task
        except asyncio.CancelledError:
            pass

    # Now we know exact size and real speed — show accurate final state
    dl_elapsed = max(time.time() - dl_start, 1)
    actual     = local_path.stat().st_size if local_path.exists() else total_size
    real_speed = actual / dl_elapsed
    await status.update(
        f"⏬ *Downloading* `{filename}`\n\n"
        f"`{progress_bar(1.0)}` 100%\n\n"
        f"{human_size(actual)} / {human_size(actual)}\n"
        f"Speed: {human_size(int(real_speed))}/s   "
        f"Time: {elapsed(start_time)}"
    )


# ── Split with live progress ───────────────────────────────────────────────────

async def split_with_progress(
    input_path: Path,
    chunk_mb: int,
    status: LiveStatus,
    filename: str,
    dl_time: str,
    actual_size: int,
    start_time: float
) -> list:
    """Run ffmpeg split and show a live spinner so user knows it's working."""
    chunk_bytes = chunk_mb * 1024 * 1024
    out_dir     = input_path.parent / (input_path.stem + "_parts")
    out_dir.mkdir(exist_ok=True)
    suffix      = input_path.suffix.lower()
    video_exts  = {".mp4", ".mkv", ".avi", ".mov", ".flv", ".webm", ".ts", ".m4v"}

    done    = asyncio.Event()
    spin_i  = 0

    async def watcher():
        nonlocal spin_i
        split_start = time.time()
        while not done.is_set():
            await asyncio.sleep(1.8)
            if done.is_set():
                break
            # Count parts already written to show incremental progress
            parts_so_far = len(list(out_dir.glob("*"))) if out_dir.exists() else 0
            spin_i += 1
            await status.update(
                f"✅ *Downloaded* in {dl_time}\n\n"
                f"✂️ *Splitting* `{filename}`\n"
                f"{spinner_frame(spin_i)} {human_size(actual_size)} — lossless copy\n"
                f"Parts written so far: {parts_so_far}\n"
                f"Split elapsed: {elapsed(split_start)}   Total: {elapsed(start_time)}"
            )

    watcher_task = asyncio.create_task(watcher())

    try:
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
            # Run ffmpeg in executor so it doesn't block the event loop
            loop   = asyncio.get_event_loop()
            result = await asyncio.wait_for(
                loop.run_in_executor(None, lambda: subprocess.run(cmd, capture_output=True, text=True)),
                timeout=1800  # 30 min max for splitting
            )
            if result.returncode != 0:
                log.warning(f"ffmpeg failed: {result.stderr[:300]}, falling back to binary split")
                shutil.rmtree(out_dir, ignore_errors=True)
                out_dir.mkdir(exist_ok=True)
                parts = await asyncio.wait_for(
                    asyncio.get_event_loop().run_in_executor(
                        None, lambda: _binary_split(input_path, out_dir, chunk_bytes)
                    ),
                    timeout=1800
                )
            else:
                parts = sorted(out_dir.glob(f"{input_path.stem}_part*{suffix}"))
        else:
            parts = await asyncio.wait_for(
                asyncio.get_event_loop().run_in_executor(
                    None, lambda: _binary_split(input_path, out_dir, chunk_bytes)
                ),
                timeout=1800
            )
    finally:
        done.set()
        watcher_task.cancel()
        try:
            await watcher_task
        except asyncio.CancelledError:
            pass

    return list(parts)


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
    file_size    = file_path.stat().st_size
    upload_start = time.time()
    done         = asyncio.Event()
    spin_i       = 0

    async def watcher():
        nonlocal spin_i
        while not done.is_set():
            await asyncio.sleep(1.8)
            if not done.is_set():
                spin_i += 1
                await status.update(
                    f"{status_prefix}\n"
                    f"📤 Uploading `{filename}`\n"
                    f"{spinner_frame(spin_i)} {human_size(file_size)} — "
                    f"{elapsed(upload_start)} uploading\n"
                    f"⏱ Total: {elapsed(start_time)}"
                )

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


# ── Bot commands ───────────────────────────────────────────────────────────────

async def cmd_start(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "📦 *File Splitter Bot*\n\n"
        "Forward me any large file from any Telegram chat or channel.\n\n"
        "• Files ≤ 500 MB → sent back as-is\n"
        "• Files > 500 MB → split into 500 MB parts, sent one by one\n"
        "• Max size: *2 GB* (Telegram hard limit)\n\n"
        "Commands: /status /retry /clear\n\n"
        "You only download small chunks — saving your data 📶",
        parse_mode=ParseMode.MARKDOWN
    )


async def cmd_retry(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    if not is_allowed(user.id):
        return

    sendable = []
    for f in DOWNLOAD_DIR.glob("*"):
        if f.is_file():
            sendable.append(f)
    for d in DOWNLOAD_DIR.glob("*_parts"):
        if d.is_dir():
            sendable.extend(sorted(d.glob("*")))

    if not sendable:
        await update.message.reply_text("📭 No files waiting on server. Forward a file to start.")
        return

    raw    = await update.message.reply_text(f"📦 Found {len(sendable)} file(s). Sending...")
    status = LiveStatus(raw)
    await status.start(f"📤 Sending *{len(sendable)}* file(s) from server...", )
    start_time = time.time()
    sent = 0

    for f in sendable:
        size   = f.stat().st_size
        prefix = f"📦 Retry — file {sent+1}/{len(sendable)}\n"
        await send_file_with_progress(
            update.message, f, f.name,
            f"📦 {f.name}  |  {human_size(size)}",
            status, prefix, start_time
        )
        f.unlink()
        sent += 1

    for d in DOWNLOAD_DIR.glob("*_parts"):
        if d.is_dir() and not any(d.iterdir()):
            d.rmdir()

    await status.finish(
        f"✅ Retry done — sent *{sent}* file(s)\n"
        f"Total time: {elapsed(start_time)}"
    )


async def cmd_status(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not is_allowed(update.effective_user.id):
        return
    files = []
    for f in DOWNLOAD_DIR.glob("*"):
        if f.is_file():
            files.append((f.name, f.stat().st_size))
    for d in DOWNLOAD_DIR.glob("*_parts"):
        if d.is_dir():
            for f in d.glob("*"):
                files.append((f.name, f.stat().st_size))
    if not files:
        await update.message.reply_text("📭 No files stored on server.")
        return
    total = sum(s for _, s in files)
    lines = "\n".join(f"• `{n}` — {human_size(s)}" for n, s in files)
    await update.message.reply_text(
        f"📦 *{len(files)} file(s)* on server — {human_size(total)} total\n\n"
        f"{lines}\n\n/retry to send  |  /clear to delete",
        parse_mode=ParseMode.MARKDOWN
    )


async def cmd_clear(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not is_allowed(update.effective_user.id):
        return
    count = 0
    for f in DOWNLOAD_DIR.glob("*"):
        if f.is_file():
            f.unlink()
            count += 1
    for d in DOWNLOAD_DIR.glob("*_parts"):
        if d.is_dir():
            shutil.rmtree(d, ignore_errors=True)
            count += 1
    await update.message.reply_text(f"🗑 Cleared {count} item(s) from server.")


# ── Main file handler ──────────────────────────────────────────────────────────

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

    raw    = await msg.reply_text("📥 Received — starting download...")
    status = LiveStatus(raw)
    await status.start(
        f"⏬ *Downloading* `{filename}`\n\n"
        f"`{'░' * 16}` ~0%\n\n"
        f"Size: {human_size(file_size)}\n"
        f"Server is fetching the file..."
    )

    local_path = DOWNLOAD_DIR / filename

    try:
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

        # Small file — send directly
        if actual_size <= split_thresh:
            prefix = (
                f"✅ *Downloaded* in {dl_time}\n\n"
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
            local_path.unlink(missing_ok=True)
            return

        # Large file — split then send
        parts      = await split_with_progress(
            local_path, SPLIT_SIZE_MB, status,
            filename, dl_time, actual_size, start_time
        )
        total      = len(parts)
        split_time = elapsed(start_time)
        log.info(f"Split into {total} parts")

        # Delete original now that parts exist
        local_path.unlink(missing_ok=True)

        for i, part in enumerate(parts, 1):
            part_size = part.stat().st_size
            prefix = (
                f"✅ *Downloaded* in {dl_time}\n"
                f"✂️ *Split done* — {total} parts\n\n"
                f"Part *{i} / {total}* — {human_size(part_size)}\n"
            )
            await send_file_with_progress(
                msg, part, part.name,
                f"📦 *{filename}*\nPart {i} of {total} — {human_size(part_size)}",
                status, prefix, start_time
            )
            part.unlink(missing_ok=True)
            log.info(f"Sent + deleted part {i}/{total}")

        # Clean up parts dir
        parts_dir = DOWNLOAD_DIR / (Path(filename).stem + "_parts")
        if parts_dir.exists():
            shutil.rmtree(parts_dir, ignore_errors=True)

        await status.finish(
            f"✅ *All done!*\n\n"
            f"`{filename}`\n"
            f"{human_size(actual_size)} → {total} parts\n\n"
            f"⬇ Download:  {dl_time}\n"
            f"⏱ Total:      {elapsed(start_time)}\n\n"
            f"Download parts one by one to save your data 📶"
        )

    except asyncio.TimeoutError:
        log.exception("Timeout during split")
        await status.finish(
            f"❌ *Timed out during split*\n\n"
            f"The file is still on the server.\n"
            f"Send /status to check, /retry to try sending again."
        )
    except TelegramError as e:
        log.exception("Telegram error")
        parts_dir = DOWNLOAD_DIR / (Path(filename).stem + "_parts")
        kept = local_path.exists() or parts_dir.exists()
        await status.finish(
            f"❌ *Upload failed*\n\n`{e}`\n\n"
            + (
                "File is still on server.\nSend /retry to try again."
                if kept else
                "Please forward the file again."
            )
        )
    except Exception as e:
        log.exception("Unexpected error")
        await status.finish(f"❌ *Error*\n\n`{e}`\n\nSend /retry if download was already done.")


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
    app.add_handler(CommandHandler("retry", cmd_retry))
    app.add_handler(CommandHandler("status", cmd_status))
    app.add_handler(CommandHandler("clear", cmd_clear))
    app.add_handler(MessageHandler(
        filters.Document.ALL | filters.VIDEO | filters.AUDIO |
        filters.VOICE | filters.VIDEO_NOTE,
        handle_file
    ))

    log.info("Bot started.")
    app.run_polling(drop_pending_updates=True)


if __name__ == "__main__":
    main()
