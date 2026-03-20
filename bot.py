#!/usr/bin/env python3
"""
Telegram File Splitter Bot
- Uses local_mode=True so downloaded files are accessed directly from disk
- No redundant network transfer — split straight from the Bot API server's path
- Pure binary split, 4 MB buffer, no ffmpeg
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


# ── Get file path (local_mode skips network entirely) ──────────────────────────

async def get_local_path(bot, file_id: str, filename: str,
                          st: LiveStatus, name: str,
                          file_size: int, t0: float) -> Path:
    """
    With local_mode=True the Bot API server already has the file on disk.
    get_file() returns its local path. download_to_drive() just returns
    that same path — no network transfer happens at all.
    We show a spinner while waiting for get_file() to respond.
    """
    spin_i = 0
    done   = asyncio.Event()

    async def watcher():
        nonlocal spin_i
        while not done.is_set():
            await asyncio.sleep(2)
            if done.is_set():
                break
            spin_i += 1
            await st.set(
                f"⏬ *Getting file* `{name}`\n\n"
                f"Size: {human_size(file_size)}\n"
                f"{spin(spin_i)} Locating on server...\n"
                f"Elapsed: {since(t0)}"
            )

    wt = asyncio.create_task(watcher())
    try:
        tg_file   = await bot.get_file(file_id,
                                        read_timeout=READ_TIMEOUT,
                                        write_timeout=WRITE_TIMEOUT)
        # In local_mode, file_path is the absolute path on the server disk
        local_str = tg_file.file_path
        log.info(f"Local file path from Bot API: {local_str}")

        # file_path may be a file:// URI or plain path
        if local_str and local_str.startswith("file://"):
            local_path = Path(local_str[7:])
        elif local_str and local_str.startswith("/"):
            local_path = Path(local_str)
        else:
            # Fallback: download normally to our own dir
            log.warning(f"Unexpected file_path format: {local_str!r}, downloading normally")
            dest = DOWNLOAD_DIR / f"{uuid.uuid4().hex[:8]}_{filename}"
            await tg_file.download_to_drive(str(dest))
            local_path = dest

        return local_path
    finally:
        done.set()
        wt.cancel()
        try:
            await wt
        except asyncio.CancelledError:
            pass


# ── Binary split — 4 MB buffer, never loads full file into RAM ─────────────────

def _binary_split(src: Path, out_dir: Path,
                  chunk_bytes: int, stem: str, suffix: str) -> list:
    READ_BUF     = 4 * 1024 * 1024
    parts        = []
    part_num     = 0
    part_written = 0

    part_path = out_dir / f"{stem}_part{part_num+1:03d}{suffix}"
    out       = open(part_path, "wb")
    parts.append(part_path)

    with open(src, "rb") as f:
        while True:
            to_read = min(READ_BUF, chunk_bytes - part_written)
            buf     = f.read(to_read)
            if not buf:
                out.close()
                if part_written == 0:
                    part_path.unlink(missing_ok=True)
                    parts.pop()
                break

            out.write(buf)
            part_written += len(buf)

            if part_written >= chunk_bytes:
                out.close()
                log.info(f"Part {part_num+1}: {part_path.name} ({human_size(part_written)})")
                part_num    += 1
                part_written = 0
                part_path    = out_dir / f"{stem}_part{part_num+1:03d}{suffix}"
                out          = open(part_path, "wb")
                parts.append(part_path)

    if not out.closed:
        out.close()
        if parts and parts[-1].exists() and part_written > 0:
            log.info(f"Part {part_num+1}: {part_path.name} ({human_size(part_written)})")

    return parts


async def do_split(src: Path, chunk_mb: int, st: LiveStatus,
                   name: str, t0: float, size: int) -> list:
    out_dir     = DOWNLOAD_DIR / uuid.uuid4().hex[:8]
    out_dir.mkdir(parents=True, exist_ok=True)
    chunk_bytes = chunk_mb * 1024 * 1024
    stem        = Path(name).stem
    suffix      = Path(name).suffix or ".bin"

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
            parts_done = list(out_dir.glob(f"{stem}_part*"))
            n          = len(parts_done)
            written    = sum(p.stat().st_size for p in parts_done)
            frac       = written / size if size > 0 else 0
            spd        = written / max(time.time() - ts, 1)
            eta        = int((size - written) / spd) if spd > 0 and written < size else 0
            await st.set(
                f"✂️ *Splitting* `{name}`\n\n"
                f"`{bar(frac)}` {frac*100:.0f}%\n"
                f"{spin(spin_i)} {human_size(written)} / {human_size(size)}\n"
                f"Parts done: {n}   ETA: {eta}s\n"
                f"Total: {since(t0)}"
            )

    wt = asyncio.create_task(watcher())
    loop = asyncio.get_running_loop()
    try:
        parts = await loop.run_in_executor(
            None,
            lambda: _binary_split(src, out_dir, chunk_bytes, stem, suffix)
        )
    finally:
        done.set()
        wt.cancel()
        try:
            await wt
        except asyncio.CancelledError:
            pass

    return parts, out_dir


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
                    f"{prefix}"
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
        "Forward any file — I'll split it into 500 MB parts and send them back.\n\n"
        "To rejoin on your device:\n"
        "`cat file_part001.mp4 file_part002.mp4 > full.mp4`\n"
        "Or use HJSplit / 7-Zip on Windows.\n\n"
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
    for d in list(DOWNLOAD_DIR.glob("*/")):
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

    raw = await msg.reply_text("📥 Received — locating file on server...")
    st  = LiveStatus(raw)
    await st.start(
        f"⏬ *Locating* `{filename}`\n\n"
        f"Size: {human_size(file_size)}\n"
        f"Checking server storage..."
    )

    parts_dir = None

    try:
        # Get local path — no network transfer with local_mode
        local_path = await get_local_path(
            ctx.bot, tg_file.file_id, filename, st, filename, file_size, t0
        )

        if not local_path.exists():
            raise RuntimeError(f"File not found at path: {local_path}")

        actual  = local_path.stat().st_size
        thresh  = SPLIT_SIZE_MB * 1024 * 1024
        log.info(f"File ready at {local_path} — {human_size(actual)}")

        await st.now(
            f"✅ *File located* `{filename}`\n\n"
            f"Size: {human_size(actual)}\n"
            f"Elapsed: {since(t0)}\n\n"
            + ("📤 Uploading..." if actual <= thresh
               else f"✂️ Splitting into {SPLIT_SIZE_MB} MB parts...")
        )

        # ── Small file: send directly ─────────────────────────────────────────
        if actual <= thresh:
            await do_upload(
                msg, local_path, filename,
                f"📦 `{filename}`  |  {human_size(actual)}",
                st, f"✅ *File ready* — {human_size(actual)}\n\n", t0
            )
            await st.done(
                f"✅ *Done!*\n\n"
                f"`{filename}`\n"
                f"{human_size(actual)} — {since(t0)}"
            )
            return

        # ── Large file: split then send ───────────────────────────────────────
        parts, parts_dir = await do_split(
            local_path, SPLIT_SIZE_MB, st, filename, t0, actual
        )
        total = len(parts)
        log.info(f"Split '{filename}' into {total} parts")

        for i, part in enumerate(parts, 1):
            ps     = part.stat().st_size
            prefix = (
                f"✂️ *Split done* — {total} parts\n\n"
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
            f"{human_size(actual)} → {total} parts\n\n"
            f"⏱ Total: {since(t0)}\n\n"
            f"Rejoin: `cat {Path(filename).stem}_part*.mp4 > full.mp4`\n"
            f"Or use HJSplit on Windows 📶"
        )

    except TelegramError as e:
        log.exception("Telegram error")
        kept = parts_dir and any(parts_dir.rglob("*"))
        await st.done(
            f"❌ *Telegram error*\n\n`{e}`\n\n"
            + ("Parts kept. /retry to resume." if kept
               else "Forward the file again.")
        )
    except Exception as e:
        log.exception("Error")
        kept = parts_dir and any(parts_dir.rglob("*"))
        await st.done(
            f"❌ *Error*\n\n`{e}`\n\n"
            + ("Parts kept. /retry to resume." if kept
               else "Forward the file again.")
        )
    else:
        if parts_dir and parts_dir.exists():
            shutil.rmtree(parts_dir, ignore_errors=True)


# ── Catch-all ──────────────────────────────────────────────────────────────────

async def catch_all(update: Update, _: ContextTypes.DEFAULT_TYPE):
    msg = update.message
    if not msg:
        return
    log.warning(
        f"UNHANDLED from {update.effective_user.id}: "
        f"text={bool(msg.text)} doc={bool(msg.document)} "
        f"video={bool(msg.video)} audio={bool(msg.audio)}"
    )
    await msg.reply_text(
        "⚠️ Could not detect a file.\n\n"
        f"doc=`{bool(msg.document)}` video=`{bool(msg.video)}`\n\n"
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
        .local_mode(True)           # tells PTB file_path is a local disk path
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
