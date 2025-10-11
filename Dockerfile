FROM python:3.11-slim

RUN apt-get update && apt-get install -y \
    ffmpeg \
    wget \
    gnupg \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

RUN playwright install chromium
RUN playwright install-deps chromium

COPY video_generator.py /app/video_generator.py
WORKDIR /app

ENV HEADLESS=true
ENV CSV_PATH=/input/leads.csv
ENV OVERLAY_PATH=/input/overlay.mp4
ENV OUTPUT_PATH=/output

CMD ["python", "video_generator.py"]
