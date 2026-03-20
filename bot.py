#!/usr/bin/env python3
"""
Telegram File Splitter Bot
"""

import os
import asyncio
import logging
import shutil
import time
import uuid
from pathlib import Path

from telegram import Update, Message
from telegram.ext import (
    ApplicationBuilder, MessageHandler,
    CommandHandler, ContextTypes, filters
)
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
SPLIT_TIMEOUT   = 1800   # 30 min hard cap on ffmpeg

logging.basicConfig(
    format="%(asctime)s | %(levelname)s | %(message)s",
    level=logging.INFO
)
log = logging.getLogger(__name__)
DOWNLOAD_DIR.mkdir(parents=True, exist_ok=True)


# ── Helpers ────────────────────────────────────────────────────────────────────

def is_allowed(uid: int) -> bool:
    return not ALLOWED_IDS or uid in ALLOWED_IDS

def human_size(b: int) -> str:
    if b <= 0:
        return "0 B"
    for unit in ("B", "KB", "MB", "GB"):
        if b < 1024:
            return f"{b:.1f} {unit}"
        b /= 1024
    return f"{b:.1f} TB"

def bar(frac: float, w: int = 16) -> str:
    n = int(min(max(frac, 0), 1) * w)
    return "█" * n + "░" * (w - n)

def since(t: float) -> str:
    s = int(time.time() - t)
    return f"{s//60}m {s%60}s" if s >= 60 else f"{s}s"

def spin(i: int) -> str:
    return "⠋⠙⠹⠸⠼⠴⠦⠧⠇⠏"[i % 10]


# ── LiveStatus ─────────────────────────────────────────────────────────────────

class LiveStatus:
    def __init__(self, msg: Message):
        self._msg     = msg
        self._text    = ""
        self._running = False
        self._task    = None

    async def start(self, text: str):
        self._text    = text
        self._running = True
        await self._push(text)
        self._task = asyncio.create_task(self._loop())

    async def set(self, text: str):
        self._text = text

    async def now(self, text: str):
        self._text = text
        await self._push(text)

    async def done(self, text: str):
        self._running = False
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        await self._push(text)

    async def _loop(self):
        while self._running:
            await asyncio.sleep(2)
            if self._running:
                await self._push(self._text)

    async def _push(self, text: str):
        try:
            await self._msg.edit_text(text, parse_mode=ParseMode.MARKDOWN)
        except Exception:
            pass


# ── Download ───────────────────────────────────────────────────────────────────

async def do_download(bot, file_id: str, dest: Path, total: int,
                      st: LiveStatus, name: str, t0: float):
    loop   = asyncio.get_running_loop()
    done   = asyncio.Event()
    spin_i = 0

    async def watcher():
        nonlocal spin_i
        while not done.is_set():
            await asyncio.sleep(2)
            if done.is_set():
                break
            spin_i += 1
            written = dest.stat().st_size if dest.exists() else 0
            frac    = (written / total) if total > 0 else 0
            speed   = written / max(time.time() - t0, 1)
            eta     = int((total - written) / speed) if speed > 0 and total > written else 0
            await st.set(
                f"⏬ *Downloading* `{name}`\n\n"
                f"`{bar(frac)}` {frac*100:.0f}%\n"
                f"{human_size(written)} / {human_size(total)}\n"
                f"{spin(spin_i)} {human_size(int(speed))}/s   ETA: {eta}s\n"
                f"Elapsed: {since(t0)}"
            )

    wt = asyncio.create_task(watcher())
    try:
        tg_file = await bot.get_file(
            file_id,
            read_timeout=READ_TIMEOUT,
            write_timeout=WRITE_TIMEOUT,
        )
        await loop.run_in_executor(
            None,
            lambda: asyncio.run(tg_file.download_to_drive(str(dest)))
        )
    finally:
        done.set()
        wt.cancel()
        try:
            await wt
        except asyncio.CancelledError:
            pass


# ── Split ──────────────────────────────────────────────────────────────────────

VIDEO_EXTS = {".mp4", ".mkv", ".avi", ".mov", ".flv", ".webm", ".ts", ".m4v"}

async def _ffmpeg_split(src: Path, out_dir: Path, chunk_bytes: int) -> list:
    """
    Use asyncio.create_subprocess_exec so the process can actually be killed
    if it hangs — unlike subprocess.run in an executor which cannot be cancelled.
    """
    pattern = str(out_dir / f"part%03d{src.suffix}")
    cmd = [
        "ffmpeg", "-hide_banner", "-loglevel", "error",
        "-i", str(src),
        "-c", "copy", "-map", "0",
        "-segment_size", str(chunk_bytes),
        "-f", "segment",
        "-reset_timestamps", "1",
        pattern, "-y"
    ]
    log.info(f"ffmpeg: {' '.join(cmd)}")
    proc = await asyncio.create_subprocess_exec(
        *cmd,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    try:
        stdout, stderr = await asyncio.wait_for(
            proc.communicate(),
            timeout=SPLIT_TIMEOUT
        )
    except asyncio.TimeoutError:
        proc.kill()
        await proc.wait()
        raise RuntimeError(f"ffmpeg timed out after {SPLIT_TIMEOUT}s")

    if proc.returncode != 0:
        raise RuntimeError(f"ffmpeg error (code {proc.returncode}): {stderr.decode()[:300]}")

    parts = sorted(out_dir.glob(f"part*{src.suffix}"))
    if not parts:
        raise RuntimeError("ffmpeg ran but produced no output files")
    return parts


def _binary_split(src: Path, out_dir: Path, chunk_bytes: int) -> list:
    parts = []
    with open(src, "rb") as f:
        i = 0
        while chunk := f.read(chunk_bytes):
            p = out_dir / f"part{i:03d}{src.suffix}"
            p.write_bytes(chunk)
            parts.append(p)
            i += 1
    return parts


async def do_split(src: Path, chunk_mb: int, st: LiveStatus,
                   name: str, dl_time: str, size: int, t0: float) -> list:
    out_dir     = src.parent / "parts"
    out_dir.mkdir(exist_ok=True)
    chunk_bytes = chunk_mb * 1024 * 1024
    suffix      = src.suffix.lower()

    done   = asyncio.Event()
    spin_i = 0
    ts     = time.time()

    async def watcher():
        nonlocal spin_i
        while not done.is_set():
            await asyncio.sleep(2)
            if done.is_set():
                break
            spin_i += 1
            n = len(list(out_dir.glob("part*")))
            await st.set(
                f"✅ *Downloaded* in {dl_time}\n\n"
                f"✂️ *Splitting* `{name}`\n"
                f"{spin(spin_i)} {human_size(size)} — lossless copy\n"
                f"Parts ready: {n}   Elapsed: {since(ts)}\n"
                f"Total: {since(t0)}"
            )

    wt = asyncio.create_task(watcher())
    loop = asyncio.get_running_loop()
    try:
        if suffix in VIDEO_EXTS:
            try:
                parts = await _ffmpeg_split(src, out_dir, chunk_bytes)
            except Exception as e:
                log.warning(f"ffmpeg failed ({e}), binary split fallback")
                shutil.rmtree(out_dir, ignore_errors=True)
                out_dir.mkdir()
                parts = await loop.run_in_executor(
                    None, lambda: _binary_split(src, out_dir, chunk_bytes)
                )
        else:
            parts = await loop.run_in_executor(
                None, lambda: _binary_split(src, out_dir, chunk_bytes)
            )
    finally:
        done.set()
        wt.cancel()
        try:
            await wt
        except asyncio.CancelledError:
            pass

    return parts


# ── Upload ─────────────────────────────────────────────────────────────────────

async def do_upload(msg: Message, path: Path, display_name: str,
                    caption: str, st: LiveStatus, prefix: str, t0: float):
    size   = path.stat().st_size
    ts     = time.time()
    done   = asyncio.Event()
    spin_i = 0

    async def watcher():
        nonlocal spin_i
        while not done.is_set():
            await asyncio.sleep(2)
            if not done.is_set():
                spin_i += 1
                await st.set(
                    f"{prefix}\n"
                    f"📤 *Uploading* `{display_name}`\n"
                    f"{spin(spin_i)} {human_size(size)}   {since(ts)}\n"
                    f"Total: {since(t0)}"
                )

    wt = asyncio.create_task(watcher())
    try:
        await msg.reply_document(
            document=path,
            filename=display_name,
            caption=caption,
            parse_mode=ParseMode.MARKDOWN,
            read_timeout=READ_TIMEOUT,
            write_timeout=WRITE_TIMEOUT,
            connect_timeout=CONNECT_TIMEOUT,
        )
    finally:
        done.set()
        wt.cancel()
        try:
            await wt
        except asyncio.CancelledError:
            pass


# ── Commands ───────────────────────────────────────────────────────────────────

async def cmd_start(update: Update, _: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "📦 *File Splitter Bot*\n\n"
        "Forward any file — I'll split it into 500 MB parts.\n\n"
        "Max: *2 GB*\n"
        "Commands: /status /retry /clear",
        parse_mode=ParseMode.MARKDOWN
    )

async def cmd_status(update: Update, _: ContextTypes.DEFAULT_TYPE):
    if not is_allowed(update.effective_user.id):
        return
    files = [f for f in DOWNLOAD_DIR.rglob("*") if f.is_file()]
    if not files:
        await update.message.reply_text("📭 No files on server.")
        return
    total = sum(f.stat().st_size for f in files)
    lines = "\n".join(f"• `{f.name}` — {human_size(f.stat().st_size)}" for f in files)
    await update.message.reply_text(
        f"📦 *{len(files)} file(s)* — {human_size(total)}\n\n{lines}\n\n"
        "/retry to send   /clear to delete",
        parse_mode=ParseMode.MARKDOWN
    )

async def cmd_retry(update: Update, _: ContextTypes.DEFAULT_TYPE):
    if not is_allowed(update.effective_user.id):
        return
    files = sorted([f for f in DOWNLOAD_DIR.rglob("*") if f.is_file()])
    if not files:
        await update.message.reply_text("📭 No files waiting.")
        return
    raw = await update.message.reply_text(f"📦 Sending {len(files)} file(s)...")
    st  = LiveStatus(raw)
    await st.start(f"📤 Sending *{len(files)}* file(s)...")
    t0  = time.time()
    for i, f in enumerate(files, 1):
        await do_upload(update.message, f, f.name, f"📦 {f.name}",
                        st, f"📦 Retry {i}/{len(files)}\n", t0)
        f.unlink(missing_ok=True)
    for d in DOWNLOAD_DIR.glob("*/"):
        if d.is_dir() and not any(d.iterdir()):
            d.rmdir()
    await st.done(f"✅ Sent {len(files)} file(s) in {since(t0)}")

async def cmd_clear(update: Update, _: ContextTypes.DEFAULT_TYPE):
    if not is_allowed(update.effective_user.id):
        return
    shutil.rmtree(DOWNLOAD_DIR, ignore_errors=True)
    DOWNLOAD_DIR.mkdir(parents=True, exist_ok=True)
    await update.message.reply_text("🗑 Server cleared.")


# ── Main handler ───────────────────────────────────────────────────────────────

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

    if tg_file is None:
        await msg.reply_text("⚠️ Could not detect a file in this message.")
        return

    file_size = getattr(tg_file, "file_size", 0) or 0
    t0        = time.time()
    log.info(f"User {user.id} | '{filename}' | {human_size(file_size)}")

    job_dir    = DOWNLOAD_DIR / uuid.uuid4().hex[:8]
    job_dir.mkdir(parents=True, exist_ok=True)
    local_path = job_dir / filename

    raw = await msg.reply_text("📥 Received — starting...")
    st  = LiveStatus(raw)
    await st.start(
        f"⏬ *Downloading* `{filename}`\n\n"
        f"`{'░'*16}` 0%\n"
        f"Size: {human_size(file_size)}\n"
        f"Server fetching file..."
    )

    try:
        await do_download(ctx.bot, tg_file.file_id, local_path,
                          file_size, st, filename, t0)

        if not local_path.exists():
            raise RuntimeError("Download finished but file missing on disk.")

        actual  = local_path.stat().st_size
        dl_time = since(t0)
        thresh  = SPLIT_SIZE_MB * 1024 * 1024
        log.info(f"Downloaded '{filename}' {human_size(actual)} in {dl_time}")

        if actual <= thresh:
            await st.now(
                f"✅ *Downloaded* in {dl_time}\n\n"
                f"`{filename}` — {human_size(actual)}\n\n"
                f"📤 Uploading..."
            )
            await do_upload(
                msg, local_path, filename,
                f"📦 `{filename}`  |  {human_size(actual)}",
                st, f"✅ *Downloaded* in {dl_time}\n\n", t0
            )
            await st.done(
                f"✅ *Done!*\n\n"
                f"`{filename}`\n"
                f"{human_size(actual)} — {since(t0)}"
            )
            return

        # Large file
        await st.now(
            f"✅ *Downloaded* in {dl_time}\n\n"
            f"`{filename}` — {human_size(actual)}\n\n"
            f"✂️ Splitting into {SPLIT_SIZE_MB} MB parts..."
        )

        parts      = await do_split(local_path, SPLIT_SIZE_MB, st,
                                     filename, dl_time, actual, t0)
        total      = len(parts)
        split_time = since(t0)
        log.info(f"Split '{filename}' into {total} parts")

        local_path.unlink(missing_ok=True)

        for i, part in enumerate(parts, 1):
            ps     = part.stat().st_size
            prefix = (
                f"✅ *Downloaded* in {dl_time}\n"
                f"✂️ *Split* — {total} parts\n\n"
                f"Part *{i}/{total}* — {human_size(ps)}\n"
            )
            await do_upload(
                msg, part, part.name,
                f"📦 *{filename}*\nPart {i}/{total} — {human_size(ps)}",
                st, prefix, t0
            )
            part.unlink(missing_ok=True)
            log.info(f"Sent part {i}/{total}")

        await st.done(
            f"✅ *All done!*\n\n"
            f"`{filename}`\n"
            f"{human_size(actual)} → {total} × {SPLIT_SIZE_MB} MB\n\n"
            f"⬇ Download: {dl_time}\n"
            f"⏱ Total:    {since(t0)}\n\n"
            f"Download parts one by one 📶"
        )

    except asyncio.TimeoutError:
        log.error("ffmpeg timed out")
        await st.done(
            f"❌ *Split timed out*\n\n"
            f"ffmpeg took too long and was killed.\n"
            f"Send /status — parts already written may be sendable via /retry."
        )
    except TelegramError as e:
        log.exception("Telegram error")
        kept = any(job_dir.rglob("*"))
        await st.done(
            f"❌ *Telegram error*\n\n`{e}`\n\n"
            + ("Files kept. Send /retry to resume." if kept
               else "Please forward the file again.")
        )
    except Exception as e:
        log.exception("Error")
        kept = any(job_dir.rglob("*"))
        await st.done(
            f"❌ *Error*\n\n`{e}`\n\n"
            + ("Files kept. Send /retry to resume." if kept
               else "Please forward the file again.")
        )
    else:
        shutil.rmtree(job_dir, ignore_errors=True)


# ── Catch-all ──────────────────────────────────────────────────────────────────

async def catch_all(update: Update, _: ContextTypes.DEFAULT_TYPE):
    msg = update.message
    if not msg:
        return
    log.warning(
        f"UNHANDLED from {update.effective_user.id}: "
        f"text={bool(msg.text)} doc={bool(msg.document)} "
        f"video={bool(msg.video)} audio={bool(msg.audio)} "
        f"photo={bool(msg.photo)}"
    )
    await msg.reply_text(
        "⚠️ Could not detect a file.\n\n"
        f"doc={bool(msg.document)} video={bool(msg.video)} audio={bool(msg.audio)}\n\n"
        "Make sure you *forward* the file directly.",
        parse_mode=ParseMode.MARKDOWN
    )


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
    log.warning("Local server not responding — starting anyway.")


def main():
    log.info(f"Using local Bot API: {LOCAL_SERVER_URL}")
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

    file_filter = (
        filters.Document.ALL | filters.VIDEO
        | filters.AUDIO | filters.VOICE | filters.VIDEO_NOTE
    )

    app.add_handler(CommandHandler("start",  cmd_start))
    app.add_handler(CommandHandler("help",   cmd_start))
    app.add_handler(CommandHandler("status", cmd_status))
    app.add_handler(CommandHandler("retry",  cmd_retry))
    app.add_handler(CommandHandler("clear",  cmd_clear))
    app.add_handler(MessageHandler(file_filter, handle_file))
    app.add_handler(MessageHandler(
        filters.ALL & ~filters.COMMAND & ~file_filter, catch_all
    ), group=1)

    log.info("Bot ready.")
    app.run_polling(drop_pending_updates=False)


if __name__ == "__main__":
    main()
