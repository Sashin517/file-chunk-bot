# 📦 Telegram File Splitter Bot

Forward any large file to this bot → it splits it into 500MB parts → sends them back to you one by one.  
**You never download the full file. You save your internet data.**

---

## How It Works

```
You forward a 4GB video
        ↓
Bot downloads it on the SERVER (Railway free server)
        ↓
Bot splits it into 8 × 500MB parts
        ↓
Bot sends Part 1, Part 2 ... Part 8 to you as Telegram messages
        ↓
You download only the parts you need, one at a time 📶
```

---

## ⚠️ Important Limit to Know

Telegram's public Bot API only allows bots to download files up to **20MB**.

To go beyond that (up to 2GB), you must use a **local Telegram Bot API server**.  
This guide includes that setup using Docker Compose.

> Files shared in Telegram chats/channels cannot exceed 2GB — so 2GB is the real ceiling.

---

## Setup: Step by Step

### 1. Create Your Bot

1. Open Telegram → search `@BotFather`
2. Send `/newbot` → follow the prompts → give it a name
3. Copy the **Bot Token** (looks like `7123456789:AAFxxx...`)

---

### 2. Get Your Telegram API credentials (for >20MB files)

1. Go to [https://my.telegram.org](https://my.telegram.org)
2. Log in with your phone number
3. Click **API development tools**
4. Create an app (any name/platform is fine)
5. Copy your **App api_id** and **App api_hash**

---

### 3. Get Your Telegram User ID (for whitelist)

- Message `@userinfobot` on Telegram
- It will reply with your numeric user ID (e.g. `123456789`)

---

### 4. Deploy to Railway (Free, No Credit Card)

**Railway gives you a free server that runs 24/7.**

1. Go to [https://railway.app](https://railway.app) → sign up with GitHub
2. Click **New Project → Deploy from GitHub repo**
3. Upload or push this folder to a GitHub repo first (see below)
4. In Railway, go to your service → **Variables** tab → add these:

| Variable | Value |
|---|---|
| `BOT_TOKEN` | Your bot token from BotFather |
| `ALLOWED_USER_IDS` | Your Telegram user ID (e.g. `123456789`) |
| `SPLIT_SIZE_MB` | `490` |

5. Click **Deploy** → done!

> Railway free tier = 500 hours/month. More than enough for personal use.

---

### 5. Push to GitHub (required for Railway)

If you don't have git installed, [download it here](https://git-scm.com/).

```bash
cd tg_splitter
git init
git add .
git commit -m "first commit"
# Create a repo on github.com, then:
git remote add origin https://github.com/YOUR_USERNAME/YOUR_REPO.git
git push -u origin main
```

Then connect that repo in Railway.

---

## Run Locally (Optional, for testing)

### Without the local Bot API server (20MB limit):

```bash
pip install -r requirements.txt
# Install ffmpeg:  sudo apt install ffmpeg  or  brew install ffmpeg
export BOT_TOKEN="your_token_here"
export ALLOWED_USER_IDS="your_user_id"
python bot.py
```

### With local Bot API server (up to 2GB):

```bash
# Install docker + docker compose first
# Then:
export BOT_TOKEN="your_token"
export TELEGRAM_API_ID="your_api_id"
export TELEGRAM_API_HASH="your_api_hash"
export ALLOWED_USER_IDS="your_user_id"
docker compose up
```

---

## File Structure

```
tg_splitter/
├── bot.py            ← The bot
├── requirements.txt  ← Python deps
├── Dockerfile        ← For Railway/Docker deploy
├── docker-compose.yml← Local run with local Bot API server
├── railway.toml      ← Railway deploy config
└── README.md         ← This file
```

---

## Environment Variables

| Variable | Required | Description |
|---|---|---|
| `BOT_TOKEN` | ✅ | From @BotFather |
| `ALLOWED_USER_IDS` | Recommended | Comma-separated user IDs. Leave empty = anyone can use |
| `SPLIT_SIZE_MB` | ❌ | Default: `490` |
| `LOCAL_SERVER_URL` | ❌ | Set to `http://localhost:8081` if using local Bot API server |
| `DOWNLOAD_DIR` | ❌ | Default: `/tmp/tg_splitter` |

---

## Cost

| Item | Cost |
|---|---|
| Railway free tier | $0 |
| Telegram Bot API | $0 |
| ffmpeg | $0 |
| **Total** | **$0/month** |
