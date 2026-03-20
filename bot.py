#!/usr/bin/env python3
"""
Telegram File Splitter Bot — full featured
- local_mode=True: file already on disk, no redundant download
- ffmpeg time-based split for video (playable parts)
- binary split fallback for non-video
- user can choose number of parts
- handles >2GB by processing in 2GB chunks sequentially
"""

import os
import asyncio
import logging
import shutil
import time
import uuid
import math
from pathlib import Path

from telegram import Update, Message
from telegram.ext import (
    ApplicationBuilder, MessageHandler,
    CommandHandler, ContextTypes, filters,
    ConversationHandler, CallbackContext
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

MAX_TG_SIZE     = 2 * 1024 * 1024 * 1024   # 2 GB Telegram hard limit
READ_TIMEOUT    = 600
WRITE_TIMEOUT   = 600
CONNECT_TIMEOUT = 30
POOL_TIMEOUT    = 60

# Conversation state
ASKING_PARTS = 1

logging.basicConfig(
    format="%(asctime)s | %(levelname)s | %(message)s",
    level=logging.INFO
)
log = logging.getLogger(__name__)
DOWNLOAD_DIR.mkdir(parents=True, exist_ok=True)

# Per-user pending job storage  {user_id: {file_id, filename, file_size}}
pending_jobs: dict = {}


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


# ── Get local file path via local_mode ────────────────────────────────────────

async def get_local_path(bot, file_id: str, filename: str,
                          st: LiveStatus, file_size: int, t0: float) -> Path:
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
                f"⏬ *Locating* `{filename}`\n\n"
                f"Size: {human_size(file_size)}\n"
                f"{spin(spin_i)} Server locating file...\n"
                f"Elapsed: {since(t0)}"
            )

    wt = asyncio.create_task(watcher())
    try:
        tg_file   = await bot.get_file(file_id,
                                        read_timeout=READ_TIMEOUT,
                                        write_timeout=WRITE_TIMEOUT)
        local_str = tg_file.file_path
        log.info(f"Bot API file_path: {local_str!r}")

        if local_str and local_str.startswith("file://"):
            return Path(local_str[7:])
        elif local_str and local_str.startswith("/"):
            return Path(local_str)
        else:
            # Fallback: actually download it
            log.warning(f"Unexpected file_path: {local_str!r} — downloading")
            dest = DOWNLOAD_DIR / f"{uuid.uuid4().hex[:8]}_{filename}"
            await tg_file.download_to_drive(str(dest))
            return dest
    finally:
        done.set()
        wt.cancel()
        try:
            await wt
        except asyncio.CancelledError:
            pass


# ── Video duration via ffprobe ─────────────────────────────────────────────────

async def get_duration(path: Path) -> float | None:
    """Return video duration in seconds, or None if not a video / ffprobe fails."""
    try:
        proc = await asyncio.create_subprocess_exec(
            "ffprobe", "-v", "error",
            "-show_entries", "format=duration",
            "-of", "default=noprint_wrappers=1:nokey=1",
            str(path),
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        stdout, _ = await asyncio.wait_for(proc.communicate(), timeout=30)
        val = stdout.decode().strip()
        return float(val) if val else None
    except Exception as e:
        log.warning(f"ffprobe failed: {e}")
        return None


# ── Video split via ffmpeg time-based (produces playable parts) ───────────────

async def ffmpeg_time_split(src: Path, out_dir: Path, n_parts: int,
                             duration: float, st: LiveStatus,
                             name: str, t0: float) -> list:
    """
    Split video into n_parts equal time segments using ffmpeg -ss/-t.
    Each part is a self-contained playable video file.
    Uses stream copy (no re-encoding) — fast and lossless quality.
    Each segment is a separate ffmpeg call so we can track progress
    and kill individual stuck processes.
    """
    seg_dur = duration / n_parts
    suffix  = src.suffix
    parts   = []

    for i in range(n_parts):
        start    = i * seg_dur
        out_path = out_dir / f"{src.stem}_part{i+1:03d}{suffix}"
        cmd = [
            "ffmpeg", "-hide_banner", "-loglevel", "error",
            "-ss", str(start),
            "-i", str(src),
            "-t", str(seg_dur),
            "-c", "copy",
            "-avoid_negative_ts", "make_zero",
            str(out_path), "-y"
        ]
        await st.set(
            f"✂️ *Splitting* `{name}`\n\n"
            f"`{bar((i)/n_parts)}` part {i+1}/{n_parts}\n"
            f"{spin(i)} Cutting segment {i+1}...\n"
            f"Total: {since(t0)}"
        )
        log.info(f"ffmpeg part {i+1}/{n_parts}: start={start:.1f}s dur={seg_dur:.1f}s")

        proc = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        try:
            _, stderr = await asyncio.wait_for(proc.communicate(), timeout=600)
        except asyncio.TimeoutError:
            proc.kill()
            await proc.wait()
            raise RuntimeError(f"ffmpeg timed out on part {i+1}")

        if proc.returncode != 0:
            err = stderr.decode()[:200]
            raise RuntimeError(f"ffmpeg part {i+1} failed: {err}")

        if not out_path.exists() or out_path.stat().st_size == 0:
            raise RuntimeError(f"ffmpeg produced empty file for part {i+1}")

        parts.append(out_path)
        log.info(f"Part {i+1} done: {human_size(out_path.stat().st_size)}")

    return parts


# ── Binary split fallback (non-video files) ───────────────────────────────────

def _binary_split_sync(src: Path, out_dir: Path,
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
                log.info(f"Binary part {part_num+1}: {human_size(part_written)}")
                part_num    += 1
                part_written = 0
                part_path    = out_dir / f"{stem}_part{part_num+1:03d}{suffix}"
                out          = open(part_path, "wb")
                parts.append(part_path)

    if not out.closed:
        out.close()
        if parts and part_written > 0:
            log.info(f"Binary part {part_num+1}: {human_size(part_written)}")

    return parts


# ── Master split function ──────────────────────────────────────────────────────

VIDEO_EXTS = {".mp4", ".mkv", ".avi", ".mov", ".flv", ".webm", ".ts", ".m4v", ".wmv"}

async def do_split(src: Path, n_parts: int, st: LiveStatus,
                   name: str, t0: float) -> tuple:
    """
    Returns (parts_list, out_dir).
    For video: uses ffmpeg time-based split → playable parts.
    For other files: binary split.
    """
    out_dir = DOWNLOAD_DIR / uuid.uuid4().hex[:8]
    out_dir.mkdir(parents=True, exist_ok=True)
    suffix  = src.suffix.lower()
    size    = src.stat().st_size

    if suffix in VIDEO_EXTS:
        duration = await get_duration(src)
        if duration and duration > 0:
            log.info(f"Video duration: {duration:.1f}s, splitting into {n_parts} parts")
            try:
                parts = await ffmpeg_time_split(src, out_dir, n_parts, duration, st, name, t0)
                return parts, out_dir
            except Exception as e:
                log.warning(f"ffmpeg split failed ({e}), falling back to binary")
                shutil.rmtree(out_dir, ignore_errors=True)
                out_dir.mkdir()
        else:
            log.warning("Could not get duration, falling back to binary split")

    # Binary split for non-video or ffmpeg failure
    chunk_bytes = math.ceil(size / n_parts)
    stem        = src.stem
    done        = asyncio.Event()
    spin_i      = 0
    ts          = time.time()

    async def watcher():
        nonlocal spin_i
        while not done.is_set():
            await asyncio.sleep(2)
            if done.is_set():
                break
            spin_i += 1
            written = sum(p.stat().st_size for p in out_dir.glob(f"{stem}_part*")
                          if p.is_file())
            frac    = written / size if size > 0 else 0
            await st.set(
                f"✂️ *Splitting* `{name}`\n\n"
                f"`{bar(frac)}` {frac*100:.0f}%\n"
                f"{spin(spin_i)} {human_size(written)} / {human_size(size)}\n"
                f"Total: {since(t0)}"
            )

    wt = asyncio.create_task(watcher())
    loop = asyncio.get_running_loop()
    try:
        parts = await loop.run_in_executor(
            None,
            lambda: _binary_split_sync(src, out_dir, chunk_bytes, stem, suffix)
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


# ── Core processing ────────────────────────────────────────────────────────────

async def process_file(msg: Message, bot, file_id: str, filename: str,
                        file_size: int, n_parts: int, t0: float,
                        st: LiveStatus):
    """
    Handle one file: locate → split → upload parts.
    For files > 2GB: binary-split into <2GB chunks first,
    upload each chunk's parts before moving to the next chunk.
    """
    parts_dir = None
    try:
        local_path = await get_local_path(bot, file_id, filename, st, file_size, t0)

        if not local_path.exists():
            raise RuntimeError(f"File not found: {local_path}")

        actual = local_path.stat().st_size
        log.info(f"File at {local_path} — {human_size(actual)}")

        # Check if file exceeds 2GB Telegram limit per file
        if actual > MAX_TG_SIZE:
            await st.now(
                f"⚠️ *File is {human_size(actual)}* — over Telegram's 2 GB limit\n\n"
                f"I'll split it into <2 GB chunks first, then split each chunk "
                f"into {SPLIT_SIZE_MB} MB parts.\n\n"
                f"✂️ Pre-splitting large file..."
            )
            # Binary split into <2GB pieces
            chunk_bytes  = int(MAX_TG_SIZE * 0.95)   # 1.9 GB safety margin
            n_superparts = math.ceil(actual / chunk_bytes)
            super_dir    = DOWNLOAD_DIR / uuid.uuid4().hex[:8]
            super_dir.mkdir(parents=True, exist_ok=True)
            loop         = asyncio.get_running_loop()
            super_parts  = await loop.run_in_executor(
                None,
                lambda: _binary_split_sync(
                    local_path, super_dir,
                    chunk_bytes, local_path.stem, local_path.suffix
                )
            )
            log.info(f"Pre-split into {len(super_parts)} super-parts")

            for si, spart in enumerate(super_parts, 1):
                await st.now(
                    f"📦 *Processing super-part {si}/{len(super_parts)}*\n\n"
                    f"`{spart.name}` — {human_size(spart.stat().st_size)}\n\n"
                    f"✂️ Splitting into {SPLIT_SIZE_MB} MB parts..."
                )
                sub_n     = max(1, math.ceil(spart.stat().st_size / (SPLIT_SIZE_MB * 1024 * 1024)))
                sub_parts, parts_dir = await do_split(spart, sub_n, st, spart.name, t0)
                total_sub = len(sub_parts)

                for i, part in enumerate(sub_parts, 1):
                    ps     = part.stat().st_size
                    prefix = (
                        f"📦 Super-part {si}/{len(super_parts)} → "
                        f"Part *{i}/{total_sub}*\n"
                        f"{human_size(ps)}\n\n"
                    )
                    await do_upload(
                        msg, part, part.name,
                        f"📦 *{filename}*\n"
                        f"Chunk {si}/{len(super_parts)} — Part {i}/{total_sub} — {human_size(ps)}",
                        st, prefix, t0
                    )
                    part.unlink(missing_ok=True)

                if parts_dir and parts_dir.exists():
                    shutil.rmtree(parts_dir, ignore_errors=True)
                spart.unlink(missing_ok=True)

            shutil.rmtree(super_dir, ignore_errors=True)
            await st.done(
                f"✅ *All done!*\n\n"
                f"`{filename}`\n"
                f"{human_size(actual)} processed\n"
                f"⏱ Total: {since(t0)}"
            )
            return

        # ── Normal file (≤ 2 GB) ─────────────────────────────────────────────
        thresh = SPLIT_SIZE_MB * 1024 * 1024

        if actual <= thresh or n_parts == 1:
            await st.now(
                f"✅ *File ready* `{filename}`\n\n"
                f"{human_size(actual)}\n\n📤 Uploading..."
            )
            await do_upload(
                msg, local_path, filename,
                f"📦 `{filename}`  |  {human_size(actual)}",
                st, f"✅ *File ready* — {human_size(actual)}\n\n", t0
            )
            await st.done(f"✅ *Done!* `{filename}` — {since(t0)}")
            return

        await st.now(
            f"✅ *File located* `{filename}`\n\n"
            f"{human_size(actual)}\n\n"
            f"✂️ Splitting into {n_parts} parts..."
        )

        parts, parts_dir = await do_split(local_path, n_parts, st, filename, t0)
        total = len(parts)

        for i, part in enumerate(parts, 1):
            ps     = part.stat().st_size
            prefix = (
                f"✂️ *Split done* — {total} parts\n\n"
                f"Part *{i}/{total}* — {human_size(ps)}\n\n"
            )
            await do_upload(
                msg, part, part.name,
                f"📦 *{filename}*\nPart {i}/{total} — {human_size(ps)}",
                st, prefix, t0
            )
            part.unlink(missing_ok=True)
            log.info(f"Sent part {i}/{total}")

        if parts_dir and parts_dir.exists():
            shutil.rmtree(parts_dir, ignore_errors=True)

        await st.done(
            f"✅ *All done!*\n\n"
            f"`{filename}`\n"
            f"{human_size(actual)} → {total} parts\n"
            f"⏱ Total: {since(t0)}"
        )

    except TelegramError as e:
        log.exception("Telegram error")
        await st.done(f"❌ *Telegram error*\n\n`{e}`\n\n/retry if parts were saved.")
    except Exception as e:
        log.exception("Error")
        await st.done(f"❌ *Error*\n\n`{e}`\n\n/retry if parts were saved.")


# ── Commands ───────────────────────────────────────────────────────────────────

async def cmd_start(update: Update, _: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "📦 *File Splitter Bot*\n\n"
        "Forward any file — I'll ask how many parts you want, then split and send.\n\n"
        f"Default part size: *{SPLIT_SIZE_MB} MB* (set `SPLIT_SIZE_MB` in Railway vars)\n"
        "Max per file: *2 GB* (Telegram limit)\n"
        "Files >2 GB are handled automatically in chunks.\n\n"
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
    lines = "\n".join(f"• `{f.name}` — {human_size(f.stat().st_size)}" for f in files[:20])
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
                        st, f"📦 {i}/{len(files)}\n", t0)
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


# ── File handler — ask number of parts ────────────────────────────────────────

async def handle_file(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    if not is_allowed(user.id):
        await update.message.reply_text("❌ Not authorized.")
        return

    msg     = update.message
    tg_file = None
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
        await msg.reply_text("⚠️ Could not detect a file.")
        return

    file_size = getattr(tg_file, "file_size", 0) or 0
    size_mb   = file_size / (1024 * 1024)

    # Calculate default number of parts
    default_parts = max(1, math.ceil(size_mb / SPLIT_SIZE_MB))
    part_size_mb  = size_mb / default_parts if default_parts > 0 else size_mb

    # Store job info for after user replies
    pending_jobs[user.id] = {
        "file_id":   tg_file.file_id,
        "filename":  filename,
        "file_size": file_size,
        "msg":       msg,
    }

    if default_parts == 1:
        await msg.reply_text(
            f"📦 *{filename}*\n"
            f"Size: {human_size(file_size)}\n\n"
            f"This file is under {SPLIT_SIZE_MB} MB — no split needed.\n\n"
            f"Reply with:\n"
            f"• `1` — send as-is\n"
            f"• Any number — split into that many parts\n"
            f"• `auto` — use default ({default_parts} part)",
            parse_mode=ParseMode.MARKDOWN
        )
    else:
        await msg.reply_text(
            f"📦 *{filename}*\n"
            f"Size: {human_size(file_size)}\n\n"
            f"How many parts do you want?\n\n"
            f"• `auto` — default ({default_parts} parts × ~{part_size_mb:.0f} MB)\n"
            f"• Any number — e.g. `3` splits into 3 equal parts\n\n"
            f"_Each part must be under {SPLIT_SIZE_MB} MB_",
            parse_mode=ParseMode.MARKDOWN
        )

    return ASKING_PARTS


async def handle_parts_answer(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    if user.id not in pending_jobs:
        return ConversationHandler.END

    job  = pending_jobs.pop(user.id)
    text = update.message.text.strip().lower()

    file_size    = job["file_size"]
    filename     = job["filename"]
    size_mb      = file_size / (1024 * 1024)
    default_parts = max(1, math.ceil(size_mb / SPLIT_SIZE_MB))

    # Parse user answer
    if text in ("auto", "0", ""):
        n_parts = default_parts
    else:
        try:
            n_parts = int(text)
        except ValueError:
            await update.message.reply_text(
                "⚠️ Please reply with a number or `auto`.",
                parse_mode=ParseMode.MARKDOWN
            )
            pending_jobs[user.id] = job
            return ASKING_PARTS

    if n_parts < 1:
        n_parts = 1

    # Validate: each part must be ≤ SPLIT_SIZE_MB
    if n_parts > 1:
        part_size_mb = size_mb / n_parts
        if part_size_mb > SPLIT_SIZE_MB:
            min_parts = math.ceil(size_mb / SPLIT_SIZE_MB)
            await update.message.reply_text(
                f"⚠️ *{n_parts} parts* would make each part "
                f"*{part_size_mb:.0f} MB* — over the {SPLIT_SIZE_MB} MB limit.\n\n"
                f"Minimum parts needed: *{min_parts}*\n\n"
                f"Reply with a number ≥ {min_parts} or `auto`.",
                parse_mode=ParseMode.MARKDOWN
            )
            pending_jobs[user.id] = job
            return ASKING_PARTS

    t0  = time.time()
    raw = await update.message.reply_text("⚙️ Starting...")
    st  = LiveStatus(raw)
    await st.start(
        f"⏬ *Processing* `{filename}`\n\n"
        f"Size: {human_size(file_size)}\n"
        f"Parts: {n_parts}\n"
        f"Locating file on server..."
    )

    await process_file(
        job["msg"], ctx.bot,
        job["file_id"], filename,
        file_size, n_parts, t0, st
    )

    return ConversationHandler.END


async def handle_cancel(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    pending_jobs.pop(uid, None)
    await update.message.reply_text("❌ Cancelled.")
    return ConversationHandler.END


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
        .local_mode(True)
        .get_updates_request(request)
        .request(request)
        .build()
    )

    file_filter = (
        filters.Document.ALL | filters.VIDEO
        | filters.AUDIO | filters.VOICE | filters.VIDEO_NOTE
    )

    conv = ConversationHandler(
        entry_points=[MessageHandler(file_filter, handle_file)],
        states={
            ASKING_PARTS: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, handle_parts_answer)
            ],
        },
        fallbacks=[CommandHandler("cancel", handle_cancel)],
        per_user=True,
        per_chat=True,
    )

    app.add_handler(CommandHandler("start",  cmd_start))
    app.add_handler(CommandHandler("help",   cmd_start))
    app.add_handler(CommandHandler("status", cmd_status))
    app.add_handler(CommandHandler("retry",  cmd_retry))
    app.add_handler(CommandHandler("clear",  cmd_clear))
    app.add_handler(conv)
    app.add_handler(MessageHandler(
        filters.ALL & ~filters.COMMAND & ~file_filter, catch_all
    ), group=1)

    log.info("Bot ready.")
    app.run_polling(drop_pending_updates=False)


if __name__ == "__main__":
    main()
