FROM python:3.12-slim

RUN apt-get update && apt-get install -y \
    ffmpeg \
    wget \
    curl \
    supervisor \
    && rm -rf /var/lib/apt/lists/*

# Download the pre-built Telegram Bot API server binary
RUN wget -q https://github.com/tdlib/telegram-bot-api/releases/download/v7.3/telegram-bot-api-amd64-linux.zip \
    -O /tmp/tgapi.zip \
    && unzip /tmp/tgapi.zip -d /usr/local/bin/ \
    && chmod +x /usr/local/bin/telegram-bot-api \
    && rm /tmp/tgapi.zip

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY bot.py .
COPY supervisord.conf .

RUN mkdir -p /tmp/tg_splitter /var/lib/tgapi /var/log/supervisor

CMD ["supervisord", "-c", "/app/supervisord.conf"]
