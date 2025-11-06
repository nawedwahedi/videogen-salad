#!/usr/bin/env python3
"""
Video Generator v27 - ORIGINAL WORKING CODE + UUID FIX ONLY
Simple, clean, working video generation - NOTHING FANCY
"""

import os
import sys
import csv
import time
import boto3
import asyncio
import json
import hashlib
from pathlib import Path
from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeout

# ================================
# ONLY NEW CODE: UUID FIX
# ================================
def get_worker_id():
    """Convert SALAD_MACHINE_ID to worker ID - handles both old IDs and UUIDs"""
    machine_id = os.getenv("SALAD_MACHINE_ID", "0")
    try:
        return int(machine_id)  # Old style numeric IDs
    except ValueError:
        # Hash UUID to get consistent number
        hash_obj = hashlib.md5(machine_id.encode())
        worker_id = int(hash_obj.hexdigest(), 16) % 10000
        print(f"[DEBUG] UUID {machine_id} converted to Worker ID: {worker_id}")
        return worker_id

# ================================
# ORIGINAL WORKING CONFIGURATION
# ================================
WORKER_ID = get_worker_id()  # ONLY CHANGED LINE
TOTAL_WORKERS = int(os.getenv("TOTAL_WORKERS", "500"))
CSV_FILENAME = os.getenv("CSV_FILENAME", "master.csv")

# R2 Configuration
R2_ENDPOINT = os.getenv("R2_ENDPOINT_URL")
R2_ACCESS_KEY = os.getenv("R2_ACCESS_KEY_ID")
R2_SECRET_KEY = os.getenv("R2_SECRET_ACCESS_KEY")
R2_BUCKET = os.getenv("R2_BUCKET_NAME")
R2_PUBLIC_URL = os.getenv("R2_PUBLIC_URL", "https://v.heedeestudios.com")
CALENDLY_URL = os.getenv("CALENDLY_URL", "https://calendly.com/heedeestudios/seo-strategy-session")

# Paths
OUTPUT_DIR = Path("/output")
OVERLAY_DIR = Path("/tmp/overlays")
CSV_PATH = Path(f"/tmp/{CSV_FILENAME}")

# Video settings
WIDTH = int(os.getenv("WIDTH", "1280"))
HEIGHT = int(os.getenv("HEIGHT", "720"))
FPS = int(os.getenv("FPS", "12"))

# ================================
# ORIGINAL WORKING S3/R2 CLIENT
# ================================
s3 = boto3.client(
    's3',
    endpoint_url=R2_ENDPOINT,
    aws_access_key_id=R2_ACCESS_KEY,
    aws_secret_access_key=R2_SECRET_KEY,
    region_name='auto'
)

# ================================
# ORIGINAL WORKING HELPER FUNCTIONS
# ================================
def log(msg):
    timestamp = time.strftime('%H:%M:%S')
    print(f"[{timestamp}] {msg}", flush=True)

def download_from_r2(key, local_path, retries=3):
    """Original simple download function"""
    for attempt in range(retries):
        try:
            s3.download_file(R2_BUCKET, key, str(local_path))
            return True
        except Exception as e:
            if attempt < retries - 1:
                log(f"[RETRY] Download failed (attempt {attempt + 1}/{retries}): {e}")
                time.sleep(2 ** attempt)
            else:
                log(f"[ERROR] Failed to download {key} after {retries} attempts: {e}")
                return False

def upload_to_r2(local_path, key, content_type="application/octet-stream", retries=3):
    """Original simple upload function"""
    for attempt in range(retries):
        try:
            s3.upload_file(
                str(local_path),
                R2_BUCKET,
                key,
                ExtraArgs={"ContentType": content_type}
            )
            return True
        except Exception as e:
            if attempt < retries - 1:
                log(f"[RETRY] Upload failed (attempt {attempt + 1}/{retries}): {e}")
                time.sleep(2 ** attempt)
            else:
                log(f"[ERROR] Failed to upload {key} after {retries} attempts: {e}")
                return False

def video_exists_in_r2(slug):
    """Check if video already exists"""
    try:
        video_key = f"{slug}/video.mp4"
        s3.head_object(Bucket=R2_BUCKET, Key=video_key)
        return True
    except:
        return False

# ================================
# ORIGINAL SCREENSHOT FUNCTION
# ================================
async def take_screenshot_with_retry(page, output_path, max_retries=3):
    """Original working screenshot function"""
    for attempt in range(1, max_retries + 1):
        try:
            log(f"📸 Screenshot attempt {attempt}/{max_retries}")
            
            # Wait for page to load
            await page.wait_for_load_state("networkidle", timeout=10000)
            await asyncio.sleep(3)
            
            # Take screenshot
            await page.screenshot(path=str(output_path), full_page=True)
            
            # Validate screenshot
            file_size = output_path.stat().st_size
            if file_size < 10000:
                log(f"⚠️ Screenshot too small ({file_size} bytes), retrying...")
                if attempt < max_retries:
                    await asyncio.sleep(5)
                    continue
            
            log(f"✅ Screenshot successful ({file_size} bytes)")
            return True
            
        except Exception as e:
            log(f"❌ Screenshot attempt {attempt} failed: {e}")
            if attempt < max_retries:
                await asyncio.sleep(5)
            else:
                raise
    
    return False

# ================================
# ORIGINAL LANDING PAGE
# ================================
def generate_landing_page(slug, video_url, thumbnail_url, calendly_url):
    """Original landing page generator"""
    return f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Personalized Video for {slug.replace('-', ' ').title()}</title>
    
    <!-- Social Media Meta Tags -->
    <meta property="og:title" content="I made this video for you">
    <meta property="og:description" content="Watch this personalized video message">
    <meta property="og:image" content="{thumbnail_url}">
    <meta property="og:video" content="{video_url}">
    <meta property="og:type" content="video.other">
    <meta name="twitter:card" content="player">
    <meta name="twitter:title" content="Personalized Video">
    <meta name="twitter:description" content="Watch this personalized video message">
    <meta name="twitter:image" content="{thumbnail_url}">
    
    <style>
        * {{
            margin: 0;
            padding: 0;
            box-sizing: border-box;
        }}
        
        body {{
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            min-height: 100vh;
            display: flex;
            justify-content: center;
            align-items: center;
            padding: 20px;
        }}
        
        .container {{
            background: white;
            border-radius: 20px;
            box-shadow: 0 20px 60px rgba(0,0,0,0.3);
            max-width: 900px;
            width: 100%;
            padding: 40px;
            text-align: center;
        }}
        
        h1 {{
            color: #333;
            font-size: 2.5rem;
            margin-bottom: 10px;
        }}
        
        .subtitle {{
            color: #666;
            font-size: 1.1rem;
            margin-bottom: 30px;
        }}
        
        video {{
            width: 100%;
            max-width: 800px;
            border-radius: 12px;
            box-shadow: 0 10px 30px rgba(0,0,0,0.2);
            margin-bottom: 30px;
        }}
        
        .cta-button {{
            display: inline-block;
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            color: white;
            padding: 18px 48px;
            border-radius: 50px;
            text-decoration: none;
            font-size: 1.2rem;
            font-weight: 600;
            transition: all 0.3s ease;
            box-shadow: 0 4px 15px rgba(102, 126, 234, 0.4);
            margin-top: 10px;
        }}
        
        .cta-button:hover {{
            transform: translateY(-2px);
            box-shadow: 0 6px 20px rgba(102, 126, 234, 0.6);
        }}
        
        @media (max-width: 768px) {{
            .container {{ padding: 30px 20px; }}
            h1 {{ font-size: 2rem; }}
            .cta-button {{ padding: 15px 36px; font-size: 1rem; }}
        }}
    </style>
</head>
<body>
    <div class="container">
        <h1>Hi there!</h1>
        <p class="subtitle">I recorded this personalized video for you</p>
        
        <video controls poster="{thumbnail_url}" preload="metadata">
            <source src="{video_url}" type="video/mp4">
            Your browser does not support the video tag.
        </video>
        
        <br>
        <a href="{calendly_url}" class="cta-button" target="_blank">
            📅 Book a FREE 10 Minute Call
        </a>
    </div>
</body>
</html>"""

# ================================
# ORIGINAL PROCESSING FUNCTION
# ================================
async def process_row(row, browser, start_time):
    """Original working process function"""
    url = row['website_url']
    slug = row['Instagram_username']
    niche = row['Niche']
    
    log(f"🎬 Processing: {slug} | {niche}")
    
    # Check if already completed
    if video_exists_in_r2(slug):
        log(f"⏭️ Skipping {slug} - already exists in R2")
        return None
    
    # Paths
    overlay_path = OVERLAY_DIR / f"{niche}.mp4"
    output_video = OUTPUT_DIR / f"{slug}.mp4"
    thumbnail_path = OUTPUT_DIR / f"{slug}_thumbnail.jpg"
    landing_page_path = OUTPUT_DIR / f"{slug}_landing.html"
    
    if not overlay_path.exists():
        log(f"❌ Overlay not found: {niche}.mp4")
        return None
    
    context = None
    try:
        # Create browser context
        context = await browser.new_context(
            viewport={"width": WIDTH, "height": HEIGHT},
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
        )
        page = await context.new_page()
        
        # Navigate and screenshot
        log(f"🌐 Loading: {url}")
        await page.goto(url, wait_until="domcontentloaded", timeout=45000)
        
        if not await take_screenshot_with_retry(page, thumbnail_path):
            raise Exception("Failed to take screenshot")
        
        # Get overlay duration
        probe = await asyncio.create_subprocess_exec(
            "ffprobe", "-v", "error", "-show_entries", "format=duration",
            "-of", "default=noprint_wrappers=1:nokey=1", str(overlay_path),
            stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
        )
        stdout, stderr = await probe.communicate()
        
        if probe.returncode != 0:
            raise Exception(f"FFprobe failed: {stderr.decode()}")
        
        overlay_duration = float(stdout.decode().strip())
        log(f"🎵 Overlay duration: {overlay_duration:.1f}s")
        
        # Create video
        log(f"🎞️ Creating video...")
        ffmpeg_cmd = [
            "ffmpeg", "-y", "-hide_banner", "-loglevel", "error",
            "-loop", "1", "-i", str(thumbnail_path),
            "-i", str(overlay_path),
            "-filter_complex",
            f"[0:v]scale={WIDTH}:{HEIGHT}:force_original_aspect_ratio=decrease,"
            f"pad={WIDTH}:{HEIGHT}:(ow-iw)/2:(oh-ih)/2,setsar=1,fps={FPS}[bg];"
            f"[1:v]scale={WIDTH//4}:-1[overlay];"
            f"[bg][overlay]overlay=W-w-20:H-h-20:enable='between(t,0,{overlay_duration})'[outv]",
            "-map", "[outv]", "-map", "1:a?",
            "-t", str(overlay_duration),
            "-c:v", "h264_nvenc" if os.path.exists("/dev/nvidia0") else "libx264",
            "-preset", "fast", "-crf", "23",
            "-c:a", "aac", "-b:a", "128k",
            str(output_video)
        ]
        
        process = await asyncio.create_subprocess_exec(*ffmpeg_cmd)
        await process.communicate()
        
        if process.returncode != 0:
            raise Exception(f"FFmpeg failed with return code {process.returncode}")
        
        # Upload files
        log(f"☁️ Uploading files for {slug}...")
        
        # Upload thumbnail
        thumbnail_key = f"{slug}/thumbnail.jpg"
        if not upload_to_r2(thumbnail_path, thumbnail_key, "image/jpeg"):
            raise Exception("Failed to upload thumbnail")
        
        # Upload video
        video_key = f"{slug}/video.mp4"
        if not upload_to_r2(output_video, video_key, "video/mp4"):
            raise Exception("Failed to upload video")
        
        # Generate and upload landing page
        landing_html = generate_landing_page(
            slug,
            f"{R2_PUBLIC_URL}/{video_key}",
            f"{R2_PUBLIC_URL}/{thumbnail_key}",
            CALENDLY_URL
        )
        landing_page_path.write_text(landing_html, encoding='utf-8')
        landing_key = f"{slug}/index.html"
        if not upload_to_r2(landing_page_path, landing_key, "text/html"):
            raise Exception("Failed to upload landing page")
        
        # Success!
        elapsed = time.time() - start_time
        log(f"✅ Completed {slug} in {elapsed//60:.0f}:{elapsed%60:02.0f}")
        log(f"🔗 Landing page: {R2_PUBLIC_URL}/{slug}/")
        
        return {
            'website_url': url,
            'instagram_username': slug,
            'niche': niche,
            'video_url': f"{R2_PUBLIC_URL}/{video_key}",
            'thumbnail_url': f"{R2_PUBLIC_URL}/{thumbnail_key}",
            'landing_page': f"{R2_PUBLIC_URL}/{slug}/",
            'duration': overlay_duration,
            'worker_id': WORKER_ID,
            'completed_at': time.strftime('%Y-%m-%d %H:%M:%S')
        }
        
    except Exception as e:
        log(f"❌ Failed to process {slug}: {e}")
        return None
        
    finally:
        if context:
            await context.close()
        
        # Cleanup
        for file_path in [output_video, thumbnail_path, landing_page_path]:
            if file_path.exists():
                file_path.unlink()

# ================================
# ORIGINAL MAIN FUNCTION
# ================================
async def main():
    start_time = time.time()
    
    log("🚀 Starting Video Generator v27 - BACK TO BASICS")
    log(f"👷 Worker {WORKER_ID} of {TOTAL_WORKERS}")
    log(f"📄 CSV Input: {CSV_FILENAME}")
    log(f"☁️ R2 Public URL: {R2_PUBLIC_URL}")
    
    # Check GPU
    nvenc_available = os.path.exists("/dev/nvidia0")
    log(f"🎮 GPU Encoding: {'NVENC Available ✅' if nvenc_available else 'CPU Only ⚠️'}")
    
    # Create directories
    OUTPUT_DIR.mkdir(exist_ok=True)
    OVERLAY_DIR.mkdir(exist_ok=True)
    
    # Download CSV
    log(f"📥 Downloading {CSV_FILENAME}...")
    if not download_from_r2(CSV_FILENAME, CSV_PATH):
        log(f"❌ Failed to download {CSV_FILENAME}")
        sys.exit(1)
    log("✅ CSV downloaded successfully")
    
    # Read CSV
    with open(CSV_PATH, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        all_rows = list(reader)
    
    # Download overlays
    niches = set(row['Niche'] for row in all_rows)
    log(f"🎬 Found {len(niches)} unique niches: {sorted(niches)}")
    
    for niche in niches:
        overlay_key = f"{niche}.mp4"
        overlay_path = OVERLAY_DIR / overlay_key
        log(f"📥 Downloading overlay: {niche}.mp4")
        if not download_from_r2(overlay_key, overlay_path):
            log(f"❌ Failed to download {overlay_key}")
            sys.exit(1)
    
    # Filter rows for this worker
    my_rows = [row for i, row in enumerate(all_rows) if i % TOTAL_WORKERS == WORKER_ID]
    
    log(f"🎯 Processing {len(my_rows)} videos at {WIDTH}x{HEIGHT}@{FPS}fps")
    
    # Process videos
    results = []
    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=True,
            args=[
                "--disable-blink-features=AutomationControlled",
                "--disable-dev-shm-usage",
                "--no-sandbox",
                "--disable-gpu"
            ]
        )
        
        for row in my_rows:
            result = await process_row(row, browser, start_time)
            if result:
                results.append(result)
        
        await browser.close()
    
    # Final summary
    elapsed = time.time() - start_time
    log(f"🏁 Completed {len(results)} videos in {int(elapsed//60)}:{int(elapsed%60):02d}")
    log(f"📊 Rate: {len(results)/(elapsed/3600):.1f} videos/hour")
    
    log(f"🎉 Worker {WORKER_ID} completed!")

if __name__ == "__main__":
    asyncio.run(main())
