#!/usr/bin/env python3
"""
Telegram File Splitter Bot — Pyrogram only (no PTB)
Uses MTProto directly: works for ALL file sizes, no Bot API 2GB limit.
"""

import os
import asyncio
import logging
import shutil
import time
import uuid
import math
from pathlib import Path

from pyrogram import Client, filters
from pyrogram.types import Message

TELEGRAM_API_ID   = int(os.environ["TELEGRAM_API_ID"])
TELEGRAM_API_HASH = os.environ["TELEGRAM_API_HASH"]
BOT_TOKEN         = os.environ["BOT_TOKEN"]
DOWNLOAD_DIR      = Path(os.environ.get("DOWNLOAD_DIR", "/tmp/tg_splitter"))
SPLIT_SIZE_MB     = int(os.environ.get("SPLIT_SIZE_MB", "490"))
ALLOWED_IDS_RAW   = os.environ.get("ALLOWED_USER_IDS", "")
ALLOWED_IDS       = set(int(x.strip()) for x in ALLOWED_IDS_RAW.split(",") if x.strip())

logging.basicConfig(
    format="%(asctime)s | %(levelname)s | %(message)s",
    level=logging.INFO
)
log = logging.getLogger(__name__)
DOWNLOAD_DIR.mkdir(parents=True, exist_ok=True)

app = Client(
    "splitter_bot",
    api_id=TELEGRAM_API_ID,
    api_hash=TELEGRAM_API_HASH,
    bot_token=BOT_TOKEN,
    workdir=str(DOWNLOAD_DIR),
)

# Per-user state
pending:    dict = {}   # uid → {msg, filename, file_size}
stop_flags: dict = {}   # uid → asyncio.Event


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
    n = int(min(max(frac, 0.0), 1.0) * w)
    return "█" * n + "░" * (w - n)

def since(t: float) -> str:
    s = int(time.time() - t)
    return f"{s//60}m {s%60}s" if s >= 60 else f"{s}s"

def spin(i: int) -> str:
    return "⠋⠙⠹⠸⠼⠴⠦⠧⠇⠏"[i % 10]

def is_stopped(uid: int) -> bool:
    ev = stop_flags.get(uid)
    return ev is not None and ev.is_set()


# ── LiveStatus: edits one message every 2s ────────────────────────────────────

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
            await self._msg.edit_text(text)
        except Exception:
            pass


# ── Download via Pyrogram MTProto ──────────────────────────────────────────────

async def download_file(msg: Message, dest: Path,
                         st: LiveStatus, t0: float, uid: int) -> Path:
    """
    Download the media from a Pyrogram Message object directly.
    This is the correct way — Pyrogram Message has the native MTProto
    file reference, no file_id conversion needed.
    Works for files of ANY size.
    """
    # Get file size from the message media object
    media     = msg.document or msg.video or msg.audio or msg.voice or msg.video_note
    file_size = getattr(media, "file_size", 0) or 0
    filename  = getattr(media, "file_name", None) or dest.name
    spin_i    = 0

    async def progress(current, total):
        nonlocal spin_i
        if is_stopped(uid):
            raise asyncio.CancelledError("Stopped by user")
        spin_i += 1
        frac    = current / total if total else 0
        speed   = current / max(time.time() - t0, 1)
        eta     = int((total - current) / speed) if speed > 0 and total > current else 0
        await st.set(
            f"⏬ *Downloading* `{filename}`\n\n"
            f"`{bar(frac)}` {frac*100:.0f}%\n"
            f"{human_size(current)} / {human_size(total)}\n"
            f"{spin(spin_i)} {human_size(int(speed))}/s   ETA: {eta}s\n"
            f"Elapsed: {since(t0)}"
        )

    result = await app.download_media(
        msg,
        file_name=str(dest),
        progress=progress,
    )
    return Path(result)


# ── ffmpeg: H.264 re-encode per segment (playable in Telegram) ─────────────────

VIDEO_EXTS = {".mp4", ".mkv", ".avi", ".mov", ".flv", ".webm", ".ts", ".m4v", ".wmv"}

async def get_duration(path: Path) -> float | None:
    try:
        proc = await asyncio.create_subprocess_exec(
            "ffprobe", "-v", "error",
            "-show_entries", "format=duration",
            "-of", "default=noprint_wrappers=1:nokey=1",
            str(path),
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        out, _ = await asyncio.wait_for(proc.communicate(), timeout=30)
        val = out.decode().strip()
        return float(val) if val else None
    except Exception as e:
        log.warning(f"ffprobe: {e}")
        return None


async def ffmpeg_split(src: Path, out_dir: Path, n: int,
                        dur: float, st: LiveStatus,
                        name: str, t0: float, uid: int) -> list:
    seg = dur / n
    parts = []
    for i in range(n):
        if is_stopped(uid):
            raise asyncio.CancelledError("Stopped")
        start = i * seg
        out   = out_dir / f"{src.stem}_part{i+1:03d}.mp4"
        cmd   = [
            "ffmpeg", "-hide_banner", "-loglevel", "error",
            "-ss", str(start), "-i", str(src),
            "-t", str(seg),
            "-c:v", "libx264", "-preset", "ultrafast", "-crf", "23",
            "-c:a", "aac", "-b:a", "128k",
            "-movflags", "+faststart",
            "-avoid_negative_ts", "make_zero",
            "-vf", "scale=trunc(iw/2)*2:trunc(ih/2)*2",
            str(out), "-y"
        ]
        await st.set(
            f"✂️ *Encoding part {i+1}/{n}*\n\n"
            f"`{bar(i/n)}` {i+1}/{n}\n"
            f"{spin(i)} Re-encoding → H.264 MP4\n"
            f"Total: {since(t0)}\n\n"
            f"_Send_ `stop` _to cancel_"
        )
        proc = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        try:
            _, err = await asyncio.wait_for(proc.communicate(), timeout=3600)
        except asyncio.TimeoutError:
            proc.kill(); await proc.wait()
            raise RuntimeError(f"ffmpeg timeout part {i+1}")
        if proc.returncode != 0:
            raise RuntimeError(f"ffmpeg part {i+1}: {err.decode()[:200]}")
        if not out.exists() or out.stat().st_size == 0:
            raise RuntimeError(f"ffmpeg empty output part {i+1}")
        parts.append(out)
        log.info(f"Encoded part {i+1}: {human_size(out.stat().st_size)}")
    return parts


# ── Binary split (non-video, 4 MB buffer) ─────────────────────────────────────

def _binary_split(src: Path, out_dir: Path, chunk: int, stem: str, suffix: str) -> list:
    BUF   = 4 * 1024 * 1024
    parts = []
    n     = 0
    w     = 0
    p     = out_dir / f"{stem}_part{n+1:03d}{suffix}"
    f_out = open(p, "wb")
    parts.append(p)
    with open(src, "rb") as f_in:
        while True:
            buf = f_in.read(min(BUF, chunk - w))
            if not buf:
                f_out.close()
                if w == 0:
                    p.unlink(missing_ok=True); parts.pop()
                break
            f_out.write(buf); w += len(buf)
            if w >= chunk:
                f_out.close()
                log.info(f"Binary part {n+1}: {human_size(w)}")
                n += 1; w = 0
                p  = out_dir / f"{stem}_part{n+1:03d}{suffix}"
                f_out = open(p, "wb"); parts.append(p)
    if not f_out.closed:
        f_out.close()
        if parts and w > 0:
            log.info(f"Binary part {n+1}: {human_size(w)}")
    return parts


# ── Master split ───────────────────────────────────────────────────────────────

async def do_split(src: Path, n: int, st: LiveStatus,
                   name: str, t0: float, uid: int):
    out_dir = DOWNLOAD_DIR / uuid.uuid4().hex[:8]
    out_dir.mkdir(parents=True, exist_ok=True)
    suffix  = src.suffix.lower()
    size    = src.stat().st_size

    if suffix in VIDEO_EXTS:
        dur = await get_duration(src)
        if dur and dur > 0:
            try:
                parts = await ffmpeg_split(src, out_dir, n, dur, st, name, t0, uid)
                return parts, out_dir
            except asyncio.CancelledError:
                raise
            except Exception as e:
                log.warning(f"ffmpeg failed ({e}), binary fallback")
                shutil.rmtree(out_dir, ignore_errors=True)
                out_dir.mkdir()

    chunk = math.ceil(size / n)
    done  = asyncio.Event()
    si    = 0
    ts    = time.time()

    async def watch():
        nonlocal si
        while not done.is_set():
            await asyncio.sleep(2)
            if done.is_set(): break
            si += 1
            written = sum(f.stat().st_size for f in out_dir.glob(f"{src.stem}_part*") if f.is_file())
            frac    = written / size if size else 0
            await st.set(
                f"✂️ *Splitting* `{name}`\n\n"
                f"`{bar(frac)}` {frac*100:.0f}%\n"
                f"{spin(si)} {human_size(written)} / {human_size(size)}\n"
                f"Total: {since(t0)}\n\n_Send_ `stop` _to cancel_"
            )

    wt   = asyncio.create_task(watch())
    loop = asyncio.get_running_loop()
    try:
        parts = await loop.run_in_executor(
            None, lambda: _binary_split(src, out_dir, chunk, src.stem, src.suffix)
        )
    finally:
        done.set(); wt.cancel()
        try: await wt
        except asyncio.CancelledError: pass

    return parts, out_dir


# ── Upload via Pyrogram ────────────────────────────────────────────────────────

async def do_upload(orig_msg: Message, path: Path, caption: str,
                     st: LiveStatus, prefix: str, t0: float):
    size   = path.stat().st_size
    ts     = time.time()
    done   = asyncio.Event()
    si     = 0
    name   = path.name

    async def progress(current, total):
        nonlocal si
        si   += 1
        frac  = current / total if total else 0
        await st.set(
            f"{prefix}"
            f"📤 *Uploading* `{name}`\n"
            f"`{bar(frac)}` {frac*100:.0f}%\n"
            f"{spin(si)} {human_size(current)} / {human_size(total)}\n"
            f"Total: {since(t0)}"
        )

    is_vid = path.suffix.lower() in VIDEO_EXTS
    if is_vid:
        await app.send_video(
            orig_msg.chat.id,
            str(path),
            caption=caption,
            supports_streaming=True,
            reply_to_message_id=orig_msg.id,
            progress=progress,
        )
    else:
        await app.send_document(
            orig_msg.chat.id,
            str(path),
            caption=caption,
            reply_to_message_id=orig_msg.id,
            progress=progress,
        )


# ── Core job ───────────────────────────────────────────────────────────────────

async def process_job(orig_msg: Message, uid: int, n_parts: int,
                       status_msg: Message):
    media    = orig_msg.document or orig_msg.video or orig_msg.audio \
               or orig_msg.voice or orig_msg.video_note
    filename = getattr(media, "file_name", None) or f"file_{orig_msg.id}"
    filesize = getattr(media, "file_size", 0) or 0

    job_dir  = DOWNLOAD_DIR / uuid.uuid4().hex[:8]
    job_dir.mkdir(parents=True, exist_ok=True)
    dest     = job_dir / filename
    parts_dir = None
    t0        = time.time()

    st = LiveStatus(status_msg)
    await st.start(
        f"⏬ *Downloading* `{filename}`\n\n"
        f"`{'░'*16}` 0%\n"
        f"Size: {human_size(filesize)}\n"
        f"Starting MTProto download..."
    )

    try:
        result = await download_file(orig_msg, dest, st, t0, uid)
        if not result.exists():
            raise RuntimeError("Download finished but file missing.")
        if is_stopped(uid):
            raise asyncio.CancelledError("Stopped")

        actual  = result.stat().st_size
        dl_time = since(t0)
        thresh  = SPLIT_SIZE_MB * 1024 * 1024
        log.info(f"Downloaded '{filename}' {human_size(actual)} in {dl_time}")

        # No split needed
        if actual <= thresh or n_parts == 1:
            await st.now(
                f"✅ *Downloaded* in {dl_time}\n\n"
                f"`{filename}` — {human_size(actual)}\n\n📤 Uploading..."
            )
            await do_upload(
                orig_msg, result,
                f"📦 `{filename}`  |  {human_size(actual)}",
                st, f"✅ *Downloaded* in {dl_time}\n\n", t0
            )
            await st.done(f"✅ *Done!* `{filename}` — {since(t0)}")
            return

        # Split needed
        await st.now(
            f"✅ *Downloaded* in {dl_time}\n\n"
            f"`{filename}` — {human_size(actual)}\n\n"
            f"✂️ Splitting into {n_parts} parts..."
        )

        parts, parts_dir = await do_split(result, n_parts, st, filename, t0, uid)

        if is_stopped(uid):
            raise asyncio.CancelledError("Stopped")

        total = len(parts)
        result.unlink(missing_ok=True)

        suffix = Path(filename).suffix.lower()
        is_vid = suffix in VIDEO_EXTS

        for i, part in enumerate(parts, 1):
            if is_stopped(uid):
                raise asyncio.CancelledError("Stopped")
            ps     = part.stat().st_size
            prefix = f"✂️ *Split done* — {total} parts\n\nPart *{i}/{total}* — {human_size(ps)}\n\n"
            await do_upload(
                orig_msg, part,
                f"📦 *{filename}*\nPart {i}/{total} — {human_size(ps)}",
                st, prefix, t0
            )
            part.unlink(missing_ok=True)
            log.info(f"Sent part {i}/{total}")

        note = "\n_Re-encoded H.264 — playable in Telegram_" if is_vid else ""
        await st.done(
            f"✅ *All done!*\n\n`{filename}`\n"
            f"{human_size(actual)} → {total} parts\n"
            f"⏱ Total: {since(t0)}{note}"
        )

    except asyncio.CancelledError:
        await st.done("🛑 *Stopped.*\n\nForward a file to start again.")
    except Exception as e:
        log.exception("Job error")
        await st.done(f"❌ *Error*\n\n`{e}`")
    finally:
        stop_flags.pop(uid, None)
        if parts_dir and parts_dir.exists():
            shutil.rmtree(parts_dir, ignore_errors=True)
        if job_dir.exists():
            shutil.rmtree(job_dir, ignore_errors=True)


# ── Handlers ───────────────────────────────────────────────────────────────────

@app.on_message(filters.command(["start", "help"]))
async def cmd_start(_, msg: Message):
    await msg.reply(
        "📦 **File Splitter Bot**\n\n"
        "Forward any file — I'll split it into parts and send them back.\n\n"
        f"• Default part size: **{SPLIT_SIZE_MB} MB**\n"
        "• Video parts: re-encoded H.264 — **playable in Telegram**\n"
        "• All file sizes supported via MTProto\n\n"
        "Send `stop` anytime to cancel.\n"
        "Commands: /status /retry /clear"
    )


@app.on_message(filters.command("stop"))
async def cmd_stop(_, msg: Message):
    uid = msg.from_user.id
    if uid in stop_flags:
        stop_flags[uid].set()
        await msg.reply("🛑 Stop signal sent — cancelling...")
    else:
        await msg.reply("Nothing is running right now.")


@app.on_message(filters.command("status"))
async def cmd_status(_, msg: Message):
    if not is_allowed(msg.from_user.id):
        return
    files = [f for f in DOWNLOAD_DIR.rglob("*")
             if f.is_file() and not f.name.endswith(".session")]
    if not files:
        await msg.reply("📭 No files on server.")
        return
    total = sum(f.stat().st_size for f in files)
    lines = "\n".join(f"• `{f.name}` — {human_size(f.stat().st_size)}" for f in files[:20])
    await msg.reply(
        f"📦 **{len(files)} file(s)** — {human_size(total)}\n\n{lines}\n\n"
        "/retry to send   /clear to delete"
    )


@app.on_message(filters.command("clear"))
async def cmd_clear(_, msg: Message):
    if not is_allowed(msg.from_user.id):
        return
    for f in DOWNLOAD_DIR.rglob("*"):
        if f.is_file() and not f.name.endswith(".session"):
            f.unlink(missing_ok=True)
    for d in sorted(DOWNLOAD_DIR.glob("*/"), reverse=True):
        try: d.rmdir()
        except: pass
    await msg.reply("🗑 Server cleared.")


@app.on_message(filters.command("retry"))
async def cmd_retry(_, msg: Message):
    if not is_allowed(msg.from_user.id):
        return
    files = sorted([f for f in DOWNLOAD_DIR.rglob("*")
                    if f.is_file() and not f.name.endswith(".session")])
    if not files:
        await msg.reply("📭 No files waiting.")
        return
    sm = await msg.reply(f"📦 Sending {len(files)} file(s)...")
    st = LiveStatus(sm)
    await st.start(f"📤 Sending **{len(files)}** file(s)...")
    t0  = time.time()
    uid = msg.from_user.id
    stop_flags[uid] = asyncio.Event()
    for i, f in enumerate(files, 1):
        if is_stopped(uid): break
        await do_upload(msg, f, f"📦 {f.name}", st, f"📦 {i}/{len(files)}\n", t0)
        f.unlink(missing_ok=True)
    stop_flags.pop(uid, None)
    await st.done(f"✅ Done in {since(t0)}")


@app.on_message(
    filters.private
    & (filters.document | filters.video | filters.audio
       | filters.voice | filters.video_note)
)
async def handle_file(_, msg: Message):
    uid = msg.from_user.id
    if not is_allowed(uid):
        await msg.reply("❌ Not authorized.")
        return

    media    = msg.document or msg.video or msg.audio or msg.voice or msg.video_note
    filename = getattr(media, "file_name", None) or f"file_{msg.id}"
    filesize = getattr(media, "file_size", 0) or 0
    size_mb  = filesize / (1024 * 1024)

    default_parts = max(1, math.ceil(size_mb / SPLIT_SIZE_MB))
    part_mb       = size_mb / default_parts if default_parts else size_mb
    suffix        = Path(filename).suffix.lower()
    is_vid        = suffix in VIDEO_EXTS

    vid_note = "\n_Video → re-encoded H.264, playable in Telegram_" if is_vid else ""

    pending[uid] = {"msg": msg, "filename": filename, "filesize": filesize}

    if default_parts == 1:
        await msg.reply(
            f"📦 **{filename}**\n"
            f"Size: {human_size(filesize)}{vid_note}\n\n"
            f"Under {SPLIT_SIZE_MB} MB — reply `1` or `auto` to send as-is, "
            f"or any number to split."
        )
    else:
        await msg.reply(
            f"📦 **{filename}**\n"
            f"Size: {human_size(filesize)}{vid_note}\n\n"
            f"How many parts?\n\n"
            f"• `auto` — {default_parts} parts × ~{part_mb:.0f} MB each\n"
            f"• Any number — split into that many equal parts\n\n"
            f"_Each part ≤ {SPLIT_SIZE_MB} MB. Send_ `stop` _to cancel._"
        )


@app.on_message(filters.private & filters.text)
async def handle_text(_, msg: Message):
    uid  = msg.from_user.id
    text = msg.text.strip().lower()

    # Stop command as plain text
    if text == "stop":
        if uid in stop_flags:
            stop_flags[uid].set()
            await msg.reply("🛑 Stop signal sent — cancelling...")
        else:
            await msg.reply("Nothing is running.")
        return

    # Parts answer
    if uid not in pending:
        return

    job      = pending.pop(uid)
    orig_msg = job["msg"]
    filesize = job["filesize"]
    filename = job["filename"]
    size_mb  = filesize / (1024 * 1024)

    default_parts = max(1, math.ceil(size_mb / SPLIT_SIZE_MB))

    if text in ("auto", "0", ""):
        n_parts = default_parts
    else:
        try:
            n_parts = int(text)
        except ValueError:
            await msg.reply("⚠️ Please reply with a number or `auto`.")
            pending[uid] = job
            return

    n_parts = max(1, n_parts)

    if n_parts > 1:
        part_mb = size_mb / n_parts
        if part_mb > SPLIT_SIZE_MB:
            min_n = math.ceil(size_mb / SPLIT_SIZE_MB)
            await msg.reply(
                f"⚠️ **{n_parts} parts** → each ~**{part_mb:.0f} MB** "
                f"(limit {SPLIT_SIZE_MB} MB)\n\n"
                f"Minimum: **{min_n}** parts\n\n"
                f"Reply with ≥ {min_n} or `auto`."
            )
            pending[uid] = job
            return

    status_msg = await msg.reply("⚙️ Starting...")
    stop_flags[uid] = asyncio.Event()
    asyncio.create_task(
        process_job(orig_msg, uid, n_parts, status_msg)
    )


# ── Main ───────────────────────────────────────────────────────────────────────

def main():
    log.info("Starting Pyrogram bot...")
    app.run()


if __name__ == "__main__":
    main()
