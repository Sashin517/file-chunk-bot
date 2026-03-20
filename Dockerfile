FROM python:3.12-slim

# ffmpeg for lossless video splitting
RUN apt-get update && \
    apt-get install -y ffmpeg curl && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY bot.py .
RUN mkdir -p /tmp/tg_splitter

CMD ["python", "-u", "bot.py"]
