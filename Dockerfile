FROM saladtechnologies/ffmpeg-nvenc:1.0.0

RUN apt-get update && apt-get install -y \
    python3 \
    python3-pip \
    python3-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt /app/
COPY video_generator.py /app/

RUN pip install --no-cache-dir -r requirements.txt

RUN playwright install chromium
RUN playwright install-deps chromium

ENV PYTHONUNBUFFERED=1

CMD ["python3", "video_generator.py"]