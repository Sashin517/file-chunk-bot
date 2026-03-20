# Stage 1: build the telegram-bot-api binary from source
FROM ubuntu:22.04 AS builder

ENV DEBIAN_FRONTEND=noninteractive
RUN apt-get update && apt-get install -y \
    git cmake g++ libssl-dev zlib1g-dev gperf \
    && rm -rf /var/lib/apt/lists/*

RUN git clone --recursive --depth=1 https://github.com/tdlib/telegram-bot-api.git /src
WORKDIR /src/build
RUN cmake -DCMAKE_BUILD_TYPE=Release .. \
    && cmake --build . --target telegram-bot-api -j$(nproc)

# Stage 2: runtime image
FROM python:3.12-slim

RUN apt-get update && apt-get install -y \
    ffmpeg supervisor libssl3 zlib1g \
    gcc python3-dev \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /src/build/telegram-bot-api /usr/local/bin/telegram-bot-api
RUN chmod +x /usr/local/bin/telegram-bot-api

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY bot.py .
COPY supervisord.conf .

RUN mkdir -p /tmp/tg_splitter /var/lib/tgapi /tmp/tgapi /var/log/supervisor

CMD ["supervisord", "-c", "/app/supervisord.conf"]
