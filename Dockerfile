FROM python:3.12-slim

RUN apt-get update && apt-get install -y \
    ffmpeg supervisor gcc python3-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY bot.py .
COPY supervisord.conf .

RUN mkdir -p /tmp/tg_splitter /var/log/supervisor

CMD ["supervisord", "-c", "/app/supervisord.conf"]
