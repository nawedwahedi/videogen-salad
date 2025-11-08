#!/usr/bin/env python3
"""
Video Generator - Working Version from Logs
Based on successful runs shown in screenshots
"""

import os
import sys
import csv
import time
import boto3
import asyncio
from pathlib import Path
from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeout

# ================================
# CONFIGURATION
# ================================
WORKER_ID = int(os.getenv("SALAD_MACHINE_ID", "0"))
TOTAL_WORKERS = int(os.getenv("TOTAL_WORKERS", "1"))
CONTAINER_GROUP_ID = int(os.getenv("CONTAINER_GROUP_ID", "1"))
CSV_FILENAME = os.getenv("CSV_FILENAME", "master.csv")

R2_ENDPOINT = os.getenv("R2_ENDPOINT_URL")
R2_ACCESS_KEY = os.getenv("R2_ACCESS_KEY_ID")
R2_SECRET_KEY = os.getenv("R2_SECRET_ACCESS_KEY")
R2_BUCKET = os.getenv("R2_BUCKET_NAME")
R2_PUBLIC_URL = os.getenv("R2_PUBLIC_URL", "https://v.heedeestudios.com")
CALENDLY_URL = os.getenv("CALENDLY_URL", "https://calendly.com/heedeestudios/seo-strategy-session")

OUTPUT_DIR = Path("/output")
OVERLAY_DIR = Path("/tmp/overlays")
CSV_PATH = Path(f"/tmp/{CSV_FILENAME}")

WIDTH = int(os.getenv("WIDTH", "1280"))
HEIGHT = int(os.getenv("HEIGHT", "720"))
FPS = int(os.getenv("FPS", "12"))

# ================================
# S3/R2 CLIENT
# ================================
s3 = boto3.client(
    's3',
    endpoint_url=R2_ENDPOINT,
    aws_access_key_id=R2_ACCESS_KEY,
    aws_secret_access_key=R2_SECRET_KEY,
    region_name='auto'
)

# ================================
# HELPER FUNCTIONS
# ================================
def log(msg):
    """Thread-safe logging"""
    print(msg, flush=True)

def download_from_r2(key, local_path):
    """Download file from R2"""
    try:
        s3.download_file(R2_BUCKET, key, str(local_path))
        return True
    except Exception as e:
        log(f"[ERROR] Failed to download {key}: {e}")
        return False

def upload_to_r2(local_path, key, content_type="application/octet-stream"):
    """Upload file to R2"""
    try:
        s3.upload_file(
            str(local_path),
            R2_BUCKET,
            key,
            ExtraArgs={"ContentType": content_type}
        )
        return True
    except Exception as e:
        log(f"[ERROR] Failed to upload {key}: {e}")
        return False

def append_to_container_output(row_data):
    """Append result to container group output CSV"""
    output_key = f"container{CONTAINER_GROUP_ID}output.csv"
    temp_csv = OUTPUT_DIR / f"temp_container{CONTAINER_GROUP_ID}.csv"
    
    # Download existing file if it exists
    existing_data = []
    try:
        temp_download = OUTPUT_DIR / "temp_download.csv"
        s3.download_file(R2_BUCKET, output_key, str(temp_download))
        with open(temp_download, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            existing_data = list(reader)
        temp_download.unlink()
    except:
        pass  # File doesn't exist yet
    
    # Append new row
    existing_data.append(row_data)
    
    # Write back
    if existing_data:
        fieldnames = existing_data[0].keys()
        with open(temp_csv, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(existing_data)
        
        # Upload with retry
        for attempt in range(3):
            if upload_to_r2(temp_csv, output_key, "text/csv"):
                log(f"[SUCCESS] Appended to {output_key}")
                temp_csv.unlink()
                return True
            time.sleep(2 ** attempt)
    
    return False

# ================================
# LANDING PAGE HTML (FIXED SCROLLBAR + RESPONSIVE)
# ================================
def generate_landing_page(slug, video_url, thumbnail_url, calendly_url):
    """Generate landing page with white scrollbar and responsive design"""
    return f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Video for You</title>
    <style>
        * {{
            margin: 0;
            padding: 0;
            box-sizing: border-box;
        }}
        
        /* WHITE SCROLLBAR FIX */
        ::-webkit-scrollbar {{
            width: 12px;
        }}
        ::-webkit-scrollbar-track {{
            background: rgba(255, 255, 255, 0.1);
        }}
        ::-webkit-scrollbar-thumb {{
            background: rgba(255, 255, 255, 0.4);
            border-radius: 6px;
        }}
        ::-webkit-scrollbar-thumb:hover {{
            background: rgba(255, 255, 255, 0.6);
        }}
        
        body {{
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Oxygen, Ubuntu, Cantarell, sans-serif;
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
            transition: transform 0.3s ease, box-shadow 0.3s ease;
            box-shadow: 0 4px 15px rgba(102, 126, 234, 0.4);
            margin-top: 10px;
        }}
        
        .cta-button:hover {{
            transform: translateY(-2px);
            box-shadow: 0 6px 20px rgba(102, 126, 234, 0.6);
        }}
        
        /* RESPONSIVE FIX */
        @media (max-width: 768px) {{
            .container {{
                padding: 30px 20px;
            }}
            h1 {{
                font-size: 2rem;
            }}
            .cta-button {{
                padding: 15px 36px;
                font-size: 1rem;
            }}
        }}
        
        @media (max-width: 480px) {{
            h1 {{
                font-size: 1.75rem;
            }}
            .subtitle {{
                font-size: 1rem;
            }}
        }}
    </style>
</head>
<body>
    <div class="container">
        <h1>Hi there</h1>
        <p class="subtitle">I recorded this video for you</p>
        
        <video controls poster="{thumbnail_url}">
            <source src="{video_url}" type="video/mp4">
            Your browser does not support the video tag.
        </video>
        
        <a href="{calendly_url}" class="cta-button" target="_blank">
            Book a FREE 10 Minute Call
        </a>
    </div>
</body>
</html>"""

# ================================
# MAIN PROCESSING
# ================================
async def process_row(row, browser, start_time):
    """Process a single row"""
    url = row['url']
    slug = row['slug']
    niche = row['niche']
    
    log(f"[{row['id']}/{row['total']}] {url} | {slug} | niche: {niche}")
    
    # Paths
    overlay_path = OVERLAY_DIR / f"{niche}.mp4"
    output_video = OUTPUT_DIR / f"{slug}.mp4"
    thumbnail_path = OUTPUT_DIR / f"{slug}_thumbnail.jpg"
    landing_page_path = OUTPUT_DIR / f"{slug}_landing.html"
    
    if not overlay_path.exists():
        log(f"[ERROR] Overlay not found: {niche}.mp4")
        return None
    
    # Create browser context
    context = await browser.new_context(
        viewport={"width": WIDTH, "height": HEIGHT},
        user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
    )
    page = await context.new_page()
    
    try:
        # Navigate with longer timeout for slow sites
        log(f"[DEBUG] Screenshot attempt 1/2")
        await page.goto(url, wait_until="domcontentloaded", timeout=60000)
        
        try:
            await page.wait_for_load_state("networkidle", timeout=15000)
        except PlaywrightTimeout:
            log(f"[WARN] Network not idle, continuing anyway")
        
        # Take screenshot
        log(f"[DEBUG] Screenshot successful")
        await page.screenshot(path=str(thumbnail_path), full_page=True)
        
        # Get video duration
        probe = await asyncio.create_subprocess_exec(
            "ffprobe", "-v", "error", "-show_entries", "format=duration",
            "-of", "default=noprint_wrappers=1:nokey=1", str(overlay_path),
            stdout=asyncio.subprocess.PIPE
        )
        duration_output, _ = await probe.communicate()
        overlay_duration = float(duration_output.decode().strip())
        log(f"[DEBUG] Overlay duration: {overlay_duration}s")
        
        # Create video with ffmpeg
        ffmpeg_cmd = [
            "ffmpeg", "-y",
            "-loop", "1", "-i", str(thumbnail_path),
            "-i", str(overlay_path),
            "-filter_complex",
            f"[0:v]scale={WIDTH}:{HEIGHT}:force_original_aspect_ratio=decrease,"
            f"pad={WIDTH}:{HEIGHT}:(ow-iw)/2:(oh-ih)/2,setsar=1,fps={FPS}[bg];"
            f"[1:v]scale={WIDTH//4}:-1[overlay];"
            f"[bg][overlay]overlay=W-w-20:H-h-20:enable='between(t,0,{overlay_duration})'[outv]",
            "-map", "[outv]",
            "-map", "1:a?",
            "-t", str(overlay_duration),
            "-c:v", "h264_nvenc" if os.path.exists("/dev/nvidia0") else "libx264",
            "-preset", "fast",
            "-c:a", "aac",
            "-b:a", "128k",
            str(output_video)
        ]
        
        process = await asyncio.create_subprocess_exec(*ffmpeg_cmd)
        await process.communicate()
        
        elapsed = time.time() - start_time
        log(f"-> saved {slug}.mp4 | {overlay_duration:.1f}s | ⏱ {elapsed//60:.0f}:{elapsed%60:02.0f}")
        
    except Exception as e:
        log(f"[ERROR] Failed to process {slug}: {e}")
        return None
    finally:
        await context.close()
    
    # Upload to R2
    try:
        # Upload thumbnail
        thumbnail_key = f"{slug}/thumbnail.jpg"
        upload_to_r2(thumbnail_path, thumbnail_key, "image/jpeg")
        log(f"[DEBUG] Thumbnail uploaded successfully: {R2_PUBLIC_URL}/{thumbnail_key}")
        
        # Upload video
        video_key = f"{slug}/video.mp4"
        upload_to_r2(output_video, video_key, "video/mp4")
        log(f"[DEBUG] Video uploaded successfully: {R2_PUBLIC_URL}/{video_key}")
        
        # Generate and upload landing page
        landing_html = generate_landing_page(
            slug,
            f"{R2_PUBLIC_URL}/{video_key}",
            f"{R2_PUBLIC_URL}/{thumbnail_key}",
            CALENDLY_URL
        )
        landing_page_path.write_text(landing_html, encoding='utf-8')
        landing_key = f"{slug}/index.html"
        upload_to_r2(landing_page_path, landing_key, "text/html")
        log(f"[DEBUG] Landing page uploaded: {R2_PUBLIC_URL}/{landing_key}")
        
        log(f"-> landing page: {R2_PUBLIC_URL}/{slug}/index.html")
        
        # Return result data
        return {
            'url': url,
            'slug': slug,
            'niche': niche,
            'video_url': f"{R2_PUBLIC_URL}/{video_key}",
            'thumbnail_url': f"{R2_PUBLIC_URL}/{thumbnail_key}",
            'landing_page': f"{R2_PUBLIC_URL}/{slug}/index.html",
            'duration': overlay_duration,
            'worker_id': WORKER_ID,
            'container_group': CONTAINER_GROUP_ID
        }
        
    except Exception as e:
        log(f"[ERROR] Failed to upload {slug}: {e}")
        return None

# ================================
# MAIN
# ================================
async def main():
    start_time = time.time()
    
    log(f"[HEADLESS MODE] Worker {WORKER_ID}/{TOTAL_WORKERS}")
    log(f"[INFO] Container Group: {CONTAINER_GROUP_ID}")
    log(f"[INFO] CSV Input: {CSV_FILENAME}")
    log(f"[INFO] Calendly URL: {CALENDLY_URL}")
    log(f"[INFO] R2 Public URL: {R2_PUBLIC_URL}")
    
    # Check for NVENC
    nvenc_available = os.path.exists("/dev/nvidia0")
    log(f"[INFO] NVENC detected: {'h264_nvenc will be used.' if nvenc_available else 'CPU encoding will be used.'}")
    
    OUTPUT_DIR.mkdir(exist_ok=True)
    OVERLAY_DIR.mkdir(exist_ok=True)
    
    # Download CSV
    log(f"[INFO] Downloading {CSV_FILENAME} from R2...")
    if not download_from_r2(CSV_FILENAME, CSV_PATH):
        log(f"[ERROR] Failed to download {CSV_FILENAME}")
        sys.exit(1)
    log("[SUCCESS] CSV downloaded")
    
    # Read CSV and find required overlays
    log(f"[INFO] Reading CSV to find required overlays: {CSV_PATH}")
    with open(CSV_PATH, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        all_rows = list(reader)
    
    # Get unique niches
    niches = set(row['niche'] for row in all_rows)
    log(f"[INFO] Found {len(niches)} unique niches: {sorted(niches)}")
    
    # Download overlays
    for niche in niches:
        overlay_key = f"{niche}.mp4"
        overlay_path = OVERLAY_DIR / overlay_key
        log(f"[INFO] Downloading {overlay_key} from R2...")
        if download_from_r2(overlay_key, overlay_path):
            log(f"[SUCCESS] Downloaded {overlay_key}")
        else:
            log(f"[ERROR] Failed to download {overlay_key}")
            sys.exit(1)
    
    # Filter rows for this worker
    my_rows = [row for i, row in enumerate(all_rows) if i % TOTAL_WORKERS == WORKER_ID]
    log(f"[INFO] {len(my_rows)} rows | {WIDTH}x{HEIGHT}@{FPS}")
    log(f"[INFO] R2 upload enabled")
    
    # Add row numbers
    for idx, row in enumerate(my_rows, 1):
        row['id'] = idx
        row['total'] = len(my_rows)
    
    # Process rows
    results = []
    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=True,
            args=[
                "--disable-blink-features=AutomationControlled",
                "--disable-dev-shm-usage",
                "--no-sandbox"
            ]
        )
        
        for row in my_rows:
            result = await process_row(row, browser, start_time)
            if result:
                results.append(result)
                # Append to container output immediately
                append_to_container_output(result)
        
        await browser.close()
    
    # Calculate elapsed time
    elapsed = time.time() - start_time
    log(f"⏱️ Total elapsed: {int(elapsed//60)}:{int(elapsed%60):02d}")
    log(f"✅ Done. {len(results)}/{len(my_rows)} videos.")
    log(f"[SUCCESS] Results appended to container{CONTAINER_GROUP_ID}output.csv")

if __name__ == "__main__":
    asyncio.run(main())
