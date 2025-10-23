# Use pre-built ffmpeg with NVENC support
FROM jrottenberg/ffmpeg:7.1-nvidia2204 AS ffmpeg

# Now use CUDA base and copy ffmpeg from above
FROM nvidia/cuda:12.6.0-cudnn-runtime-ubuntu22.04

# Copy ffmpeg binaries from ffmpeg image
COPY --from=ffmpeg /usr/local /usr/local

# Prevent interactive prompts
ENV DEBIAN_FRONTEND=noninteractive

# Install Python, git, and Playwright dependencies
RUN apt-get update && apt-get install -y \
    python3 \
    python3-pip \
    python3-dev \
    wget \
    git \
    curl \
    # Playwright dependencies
    libnss3 \
    libnspr4 \
    libatk1.0-0 \
    libatk-bridge2.0-0 \
    libcups2 \
    libdrm2 \
    libxkbcommon0 \
    libxcomposite1 \
    libxdamage1 \
    libxfixes3 \
    libxrandr2 \
    libgbm1 \
    libasound2 \
    libpango-1.0-0 \
    libcairo2 \
    libatspi2.0-0 \
    libx11-6 \
    libx11-xcb1 \
    libxcb1 \
    libxext6 \
    libdbus-1-3 \
    libglib2.0-0 \
    && rm -rf /var/lib/apt/lists/*

# Clone the repository
WORKDIR /app
RUN git clone https://github.com/nawedwahedi/videogen-salad.git /app

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Install Playwright browsers
RUN playwright install chromium
RUN playwright install-deps chromium

# Run the script
CMD ["python3", "video_generator.py"]
