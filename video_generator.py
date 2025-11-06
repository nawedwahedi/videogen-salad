#!/usr/bin/env python3
"""
Video Generator v24 - EMERGENCY DEBUG VERSION
ONLY CHANGE: Added extensive debug logging to find the exact download issue
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
# UUID FIX
# ================================
def get_worker_id():
    """Convert SALAD_MACHINE_ID (UUID or int) to worker ID integer"""
    machine_id = os.getenv("SALAD_MACHINE_ID", "0")
    try:
        return int(machine_id)
    except ValueError:
        hash_obj = hashlib.md5(machine_id.encode())
        worker_id = int(hash_obj.hexdigest(), 16) % 10000
        print(f"[DEBUG] UUID {machine_id} converted to Worker ID: {worker_id}")
        return worker_id

# ================================
# CONFIGURATION
# ================================
WORKER_ID = get_worker_id()
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

# Screenshot settings
SCREENSHOT_MAX_WAIT = 45000
NETWORK_IDLE_TIMEOUT = 10000
SCREENSHOT_RETRIES = 3

# ================================
# LOGGING
# ================================
def log(msg):
    """Enhanced logging with timestamps"""
    timestamp = time.strftime('%H:%M:%S')
    print(f"[{timestamp}] {msg}", flush=True)

# ================================
# EMERGENCY DEBUG ALL ENV VARS
# ================================
log(f"[DEBUG] R2_ENDPOINT = '{R2_ENDPOINT}' (type: {type(R2_ENDPOINT)})")
log(f"[DEBUG] R2_ACCESS_KEY = '{R2_ACCESS_KEY}' (type: {type(R2_ACCESS_KEY)})")
log(f"[DEBUG] R2_SECRET_KEY = '{R2_SECRET_KEY[:10] if R2_SECRET_KEY else None}...' (type: {type(R2_SECRET_KEY)})")
log(f"[DEBUG] R2_BUCKET = '{R2_BUCKET}' (type: {type(R2_BUCKET)})")
log(f"[DEBUG] CSV_FILENAME = '{CSV_FILENAME}' (type: {type(CSV_FILENAME)})")

# ================================
# S3/R2 CLIENT WITH DEBUG
# ================================
try:
    s3 = boto3.client(
        's3',
        endpoint_url=R2_ENDPOINT,
        aws_access_key_id=R2_ACCESS_KEY,
        aws_secret_access_key=R2_SECRET_KEY,
        region_name='auto'
    )
    log("[DEBUG] S3 client created successfully")
except Exception as e:
    log(f"[DEBUG] S3 client creation failed: {e}")
    sys.exit(1)

# ================================
# PROGRESS TRACKING
# ================================
class ProgressTracker:
    def __init__(self, worker_id, container_group_id, total_videos):
        self.worker_id = worker_id
        self.container_group_id = container_group_id
        self.total_videos = total_videos
        self.completed = 0
        self.failed = 0
        self.skipped = 0
        self.start_time = time.time()
        
        # Progress file names
        self.progress_key = f"progress/worker_{worker_id}_container_{container_group_id}.json"
        self.status_key = f"status/container_{container_group_id}_status.json"
    
    def load_progress(self):
        """Load existing progress from R2"""
        try:
            temp_file = OUTPUT_DIR / "progress.json"
            s3.download_file(R2_BUCKET, self.progress_key, str(temp_file))
            with open(temp_file, 'r') as f:
                data = json.load(f)
                self.completed = data.get('completed', 0)
                self.failed = data.get('failed', 0)
                self.skipped = data.get('skipped', 0)
            temp_file.unlink()
            log(f"📊 Loaded progress: {self.completed} completed, {self.failed} failed, {self.skipped} skipped")
        except:
            log("📊 No previous progress found, starting fresh")
    
    def save_progress(self):
        """Save current progress to R2"""
        progress_data = {
            'worker_id': self.worker_id,
            'container_group_id': self.container_group_id,
            'total_videos': self.total_videos,
            'completed': self.completed,
            'failed': self.failed,
            'skipped': self.skipped,
            'last_updated': time.time(),
            'runtime_minutes': (time.time() - self.start_time) / 60
        }
        
        try:
            temp_file = OUTPUT_DIR / "progress.json"
            with open(temp_file, 'w') as f:
                json.dump(progress_data, f, indent=2)
            upload_to_r2(temp_file, self.progress_key, "application/json")
            temp_file.unlink()
        except Exception as e:
            log(f"[WARN] Failed to save progress: {e}")
    
    def update_status(self):
        """Update overall container group status"""
        try:
            elapsed = time.time() - self.start_time
            rate = self.completed / (elapsed / 3600) if elapsed > 0 else 0
            eta_hours = (self.total_videos - self.completed) / rate if rate > 0 else 0
            
            status_data = {
                'container_group_id': self.container_group_id,
                'worker_id': self.worker_id,
                'progress': f"{self.completed}/{self.total_videos}",
                'completion_rate': f"{(self.completed/self.total_videos)*100:.1f}%",
                'videos_per_hour': f"{rate:.1f}",
                'eta_hours': f"{eta_hours:.1f}h",
                'failed': self.failed,
                'skipped': self.skipped,
                'last_updated': time.strftime('%Y-%m-%d %H:%M:%S'),
                'runtime_minutes': f"{elapsed/60:.1f}min"
            }
            
            temp_file = OUTPUT_DIR / "status.json"
            with open(temp_file, 'w') as f:
                json.dump(status_data, f, indent=2)
            upload_to_r2(temp_file, self.status_key, "application/json")
            temp_file.unlink()
            
            # Log progress
            log(f"🎯 PROGRESS: {self.completed}/{self.total_videos} ({(self.completed/self.total_videos)*100:.1f}%) | Rate: {rate:.1f}/hr | ETA: {eta_hours:.1f}h")
            
        except Exception as e:
            log(f"[WARN] Failed to update status: {e}")
    
    def mark_completed(self):
        self.completed += 1
        if self.completed % 5 == 0:
            self.save_progress()
            self.update_status()
    
    def mark_failed(self):
        self.failed += 1
        if (self.completed + self.failed) % 5 == 0:
            self.save_progress()
            self.update_status()
    
    def mark_skipped(self):
        self.skipped += 1

# ================================
# HELPER FUNCTIONS WITH EMERGENCY DEBUG
# ================================
def download_from_r2(key, local_path, retries=3):
    """Download file from R2 with retries - WITH EMERGENCY DEBUG"""
    
    # EMERGENCY DEBUG - CHECK ALL PARAMETERS
    log(f"[DEBUG] download_from_r2 called with:")
    log(f"[DEBUG]   R2_BUCKET = '{R2_BUCKET}' (type: {type(R2_BUCKET)})")
    log(f"[DEBUG]   key = '{key}' (type: {type(key)})")
    log(f"[DEBUG]   local_path = '{local_path}' (type: {type(local_path)})")
    log(f"[DEBUG]   str(local_path) = '{str(local_path)}'")
    
    # Check if any parameter is None
    if R2_BUCKET is None:
        log("[ERROR] R2_BUCKET is None!")
        return False
    if key is None:
        log("[ERROR] key is None!")
        return False
    if local_path is None:
        log("[ERROR] local_path is None!")
        return False
    
    # Try to create parent directory
    try:
        local_path.parent.mkdir(parents=True, exist_ok=True)
        log(f"[DEBUG] Created directory: {local_path.parent}")
    except Exception as e:
        log(f"[DEBUG] Directory creation error: {e}")
    
    for attempt in range(retries):
        try:
            log(f"[DEBUG] Calling s3.download_file('{R2_BUCKET}', '{key}', '{str(local_path)}')")
            s3.download_file(R2_BUCKET, key, str(local_path))
            log(f"[DEBUG] s3.download_file completed successfully")
            return True
        except Exception as e:
            log(f"[DEBUG] s3.download_file failed with: {type(e).__name__}: {e}")
            if attempt < retries - 1:
                log(f"[RETRY] Download failed (attempt {attempt + 1}/{retries}): {e}")
                time.sleep(2 ** attempt)
            else:
                log(f"[ERROR] Failed to download {key} after {retries} attempts: {e}")
                return False

def upload_to_r2(local_path, key, content_type="application/octet-stream", retries=3):
    """Upload file to R2 with retries"""
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
    """Check if video already exists in R2"""
    try:
        video_key = f"{slug}/video.mp4"
        s3.head_object(Bucket=R2_BUCKET, Key=video_key)
        return True
    except:
        return False

def append_to_container_output(row_data):
    """Append result to container group output CSV with better error handling"""
    output_key = f"container{CONTAINER_GROUP_ID}output.csv"
    temp_csv = OUTPUT_DIR / f"temp_container{CONTAINER_GROUP_ID}.csv"
    
    try:
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
        
        # Write back with proper error handling
        if existing_data:
            fieldnames = existing_data[0].keys()
            with open(temp_csv, 'w', newline='', encoding='utf-8') as f:
                writer = csv.DictWriter(f, fieldnames=fieldnames)
                writer.writeheader()
                writer.writerows(existing_data)
            
            # Upload with retry
            if upload_to_r2(temp_csv, output_key, "text/csv"):
                temp_csv.unlink()
                return True
        
        return False
        
    except Exception as e:
        log(f"[ERROR] Failed to append to container output: {e}")
        return False

# ================================
# SCREENSHOT WITH ENHANCED LOGIC
# ================================
async def take_screenshot_with_retry(page, output_path, max_retries=SCREENSHOT_RETRIES):
    """Enhanced screenshot with better retry logic"""
    for attempt in range(1, max_retries + 1):
        try:
            log(f"📸 Screenshot attempt {attempt}/{max_retries}")
            
            # Enhanced page loading strategy
            await page.evaluate("""
                // Scroll to trigger lazy loading
                window.scrollTo(0, 0);
                setTimeout(() => {
                    window.scrollTo(0, document.body.scrollHeight / 2);
                    setTimeout(() => window.scrollTo(0, 0), 1000);
                }, 500);
            """)
            
            # Wait for network idle with timeout
            try:
                await page.wait_for_load_state("networkidle", timeout=NETWORK_IDLE_TIMEOUT)
            except PlaywrightTimeout:
                log(f"[WARN] Network not idle after {NETWORK_IDLE_TIMEOUT}ms, proceeding anyway")
            
            # Additional wait for dynamic content
            await asyncio.sleep(3)
            
            # Take screenshot
            await page.screenshot(path=str(output_path), full_page=True)
            
            # Validate screenshot
            file_size = output_path.stat().st_size
            if file_size < 10000:  # Less than 10KB might be blank
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
# LANDING PAGE HTML
# ================================
def generate_landing_page(slug, video_url, thumbnail_url, calendly_url):
    """Generate responsive landing page with social media optimization"""
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
# MAIN PROCESSING
# ================================
async def process_row(row, browser, start_time, progress_tracker):
    """Process a single row with enhanced error handling"""
    url = row['website_url']
    slug = row['Instagram_username']
    niche = row['Niche']
    
    log(f"🎬 [{row['id']}/{row['total']}] Processing: {slug} | {niche}")
    
    # Check if already completed
    if video_exists_in_r2(slug):
        log(f"⏭️ Skipping {slug} - already exists in R2")
        progress_tracker.mark_skipped()
        return None
    
    # Paths
    overlay_path = OVERLAY_DIR / f"{niche}.mp4"
    output_video = OUTPUT_DIR / f"{slug}.mp4"
    thumbnail_path = OUTPUT_DIR / f"{slug}_thumbnail.jpg"
    landing_page_path = OUTPUT_DIR / f"{slug}_landing.html"
    
    if not overlay_path.exists():
        log(f"❌ Overlay not found: {niche}.mp4")
        progress_tracker.mark_failed()
        return None
    
    context = None
    try:
        # Create browser context
        context = await browser.new_context(
            viewport={"width": WIDTH, "height": HEIGHT},
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        )
        page = await context.new_page()
        
        # Navigate with timeout
        log(f"🌐 Loading: {url}")
        await page.goto(url, wait_until="domcontentloaded", timeout=SCREENSHOT_MAX_WAIT)
        
        # Take screenshot
        if not await take_screenshot_with_retry(page, thumbnail_path):
            raise Exception("Failed to take screenshot after retries")
        
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
        
        # Create video with enhanced ffmpeg command
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
        
        progress_tracker.mark_completed()
        
        # Return result
        result = {
            'website_url': url,
            'instagram_username': slug,
            'niche': niche,
            'video_url': f"{R2_PUBLIC_URL}/{video_key}",
            'thumbnail_url': f"{R2_PUBLIC_URL}/{thumbnail_key}",
            'landing_page': f"{R2_PUBLIC_URL}/{slug}/",
            'duration': overlay_duration,
            'worker_id': WORKER_ID,
            'container_group': CONTAINER_GROUP_ID,
            'completed_at': time.strftime('%Y-%m-%d %H:%M:%S')
        }
        
        # Save to container output
        append_to_container_output(result)
        
        return result
        
    except Exception as e:
        log(f"❌ Failed to process {slug}: {e}")
        progress_tracker.mark_failed()
        return None
        
    finally:
        if context:
            await context.close()
        
        # Cleanup local files
        for file_path in [output_video, thumbnail_path, landing_page_path]:
            if file_path.exists():
                file_path.unlink()

# ================================
# MAIN FUNCTION WITH EXTENSIVE DEBUG
# ================================
async def main():
    start_time = time.time()
    
    log("🚀 Starting Video Generator v24 - EMERGENCY DEBUG")
    log(f"🔧 Original Machine ID: {os.getenv('SALAD_MACHINE_ID', 'N/A')}")
    log(f"👷 Worker {WORKER_ID} of {TOTAL_WORKERS}")
    log(f"📦 Container Group: {CONTAINER_GROUP_ID}")
    log(f"📄 CSV Input: {CSV_FILENAME}")
    log(f"📅 Calendly URL: {CALENDLY_URL}")
    log(f"☁️ R2 Public URL: {R2_PUBLIC_URL}")
    
    # Check GPU availability
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
    
    # Read and process CSV
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
    
    # Add row numbers
    for idx, row in enumerate(my_rows, 1):
        row['id'] = idx
        row['total'] = len(my_rows)
    
    log(f"🎯 Processing {len(my_rows)} videos at {WIDTH}x{HEIGHT}@{FPS}fps")
    
    # Initialize progress tracker
    progress_tracker = ProgressTracker(WORKER_ID, CONTAINER_GROUP_ID, len(my_rows))
    progress_tracker.load_progress()
    progress_tracker.update_status()
    
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
            result = await process_row(row, browser, start_time, progress_tracker)
            if result:
                results.append(result)
        
        await browser.close()
    
    # Final summary
    elapsed = time.time() - start_time
    log(f"🏁 FINAL RESULTS:")
    log(f"   ✅ Completed: {progress_tracker.completed}")
    log(f"   ❌ Failed: {progress_tracker.failed}")
    log(f"   ⏭️ Skipped: {progress_tracker.skipped}")
    log(f"   ⏱️ Total time: {int(elapsed//60)}:{int(elapsed%60):02d}")
    log(f"   📊 Rate: {progress_tracker.completed/(elapsed/3600):.1f} videos/hour")
    
    # Save final progress
    progress_tracker.save_progress()
    progress_tracker.update_status()
    
    log(f"🎉 Worker {WORKER_ID} completed! Check container{CONTAINER_GROUP_ID}output.csv for results")

if __name__ == "__main__":
    asyncio.run(main())
