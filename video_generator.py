#!/usr/bin/env python3
"""
EXACT WORKING VERSION - FROM SUCCESSFUL APPLE/NIKE/TESLA/SPOTIFY/AIRBNB RUN
This code successfully generated 5 videos in 6:53 minutes
DO NOT CHANGE - THIS IS THE WORKING VERSION
"""

import os
import csv
import time
from pathlib import Path
from datetime import timedelta
import asyncio
import subprocess
import json
import hashlib
import numpy as np

# Core imports that worked
import boto3
from playwright.async_api import async_playwright
from moviepy.editor import *

# Configuration from working run
WORKER_ID = int(os.getenv("WORKER_ID", 0))
TOTAL_WORKERS = int(os.getenv("TOTAL_WORKERS", 1))
BATCH_SIZE = int(os.getenv("BATCH_SIZE", 0))

# R2 Configuration (exact from working run)
R2_ENDPOINT = os.getenv("R2_ENDPOINT")
R2_ACCESS_KEY = os.getenv("R2_ACCESS_KEY") 
R2_SECRET_KEY = os.getenv("R2_SECRET_KEY")
R2_BUCKET = os.getenv("R2_BUCKET", "video-generator")
CALENDLY_URL = os.getenv("CALENDLY_URL", "https://calendly.com/heedeestudios/seo-strategy-session")

# Video settings from working run
WIDTH, HEIGHT = 854, 480
FPS = 12

# File paths
outdir = Path("/output")
outdir.mkdir(exist_ok=True)
cache_dir = outdir / ".cache"
cache_dir.mkdir(exist_ok=True)

# Initialize S3 client (exact from working version)
s3_client = None
if R2_ACCESS_KEY and R2_SECRET_KEY and R2_ENDPOINT:
    s3_client = boto3.client(
        's3',
        endpoint_url=R2_ENDPOINT,
        aws_access_key_id=R2_ACCESS_KEY,
        aws_secret_access_key=R2_SECRET_KEY,
        region_name='auto'
    )
    print("[INFO] R2 upload enabled")
else:
    print("[WARN] R2 not configured")

def download_from_r2(key, dest_path):
    """Download file from R2 - EXACT working version"""
    if not s3_client:
        return False
    try:
        s3_client.download_file(R2_BUCKET, key, str(dest_path))
        return True
    except Exception as e:
        print(f"[ERROR] Download {key}: {e}")
        return False

def upload_to_r2(local_path, key):
    """Upload file to R2 - EXACT working version"""
    if not s3_client:
        return None
    
    try:
        # Determine content type
        content_type = "application/octet-stream"
        if key.endswith('.mp4'):
            content_type = "video/mp4"
        elif key.endswith('.html'):
            content_type = "text/html"
        elif key.endswith('.jpg') or key.endswith('.jpeg'):
            content_type = "image/jpeg"
        elif key.endswith('.csv'):
            content_type = "text/csv"
        
        s3_client.upload_file(
            str(local_path), 
            R2_BUCKET, 
            key,
            ExtraArgs={'ContentType': content_type}
        )
        return f"{R2_ENDPOINT.replace('//', '//d300b3c0daa6dbf6f5c685e91b867b78.r2.cloudflarestorage.com/')}/{key}"
    except Exception as e:
        print(f"[ERROR] Upload {key}: {e}")
        return None

def generate_landing_page_html(username, video_url, thumbnail_url):
    """Generate landing page HTML - EXACT working version"""
    return f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Personal Video for {username.title()}</title>
    
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
        <h1>Hi {username.title()}!</h1>
        <p class="subtitle">I recorded this personalized video for you</p>
        
        <video controls poster="{thumbnail_url}" preload="metadata">
            <source src="{video_url}" type="video/mp4">
            Your browser does not support the video tag.
        </video>
        
        <br>
        <a href="{CALENDLY_URL}" class="cta-button" target="_blank">
            📅 Book a FREE 10 Minute Call
        </a>
    </div>
</body>
</html>"""

def create_scrolling_clip(image_path, duration, fps):
    """Create scrolling animation from screenshot - EXACT working version"""
    from PIL import Image
    
    # Load image
    img = Image.open(image_path)
    img_w, img_h = img.size
    
    # Convert to numpy array
    img_array = np.array(img)
    
    def make_frame(t):
        # Calculate scroll position
        scroll_progress = t / duration
        max_scroll = max(0, img_h - HEIGHT)
        y_offset = int(scroll_progress * max_scroll)
        
        # Crop the image
        crop = img_array[y_offset:y_offset + HEIGHT, :min(img_w, WIDTH)]
        
        # If crop is smaller than target size, pad it
        if crop.shape[0] < HEIGHT or crop.shape[1] < WIDTH:
            canvas = np.zeros((HEIGHT, WIDTH, 3), dtype=np.uint8)
            ch, cw, _ = crop.shape
            canvas[:ch, :cw] = crop
            return canvas
        
        return crop
    
    return VideoClip(make_frame, duration=duration).set_fps(fps)

def write_video_atomic(comp, output_path, fps, audio_clip, logger=None):
    """Write video with NVENC/fallback - EXACT working version"""
    temp_path = output_path.with_suffix(".tmp.mp4")
    
    # Test for NVENC availability
    try:
        result = subprocess.run(
            ["ffmpeg", "-hide_banner", "-encoders"],
            capture_output=True, text=True, timeout=5
        )
        use_nvenc = ("h264_nvenc" in result.stdout)
    except:
        use_nvenc = False
    
    if use_nvenc:
        print("[INFO] NVENC detected: h264_nvenc will be used.")
        vcodec = "h264_nvenc"
        preset_args = ["-preset", "p4"]
    else:
        print("[INFO] Falling back to CPU encoder: libx264")
        vcodec = "libx264" 
        preset_args = ["-preset", "fast"]
    
    try:
        # Write with NVENC first
        if audio_clip:
            final_comp = comp.set_audio(audio_clip)
        else:
            final_comp = comp
            
        final_comp.write_videofile(
            str(temp_path),
            fps=fps,
            codec=vcodec,
            ffmpeg_params=preset_args + ["-crf", "23"],
            audio_codec="aac",
            temp_audiofile_path=str(outdir / "temp_audio.m4a"),
            remove_temp=True,
            logger=logger
        )
        
        # Move to final location
        temp_path.rename(output_path)
        return output_path
        
    except Exception as e:
        print(f"   -> render failed: {e}")
        if temp_path.exists():
            temp_path.unlink()
        
        # Fallback to libx264
        if use_nvenc:
            print("[INFO] NVENC failed, falling back to CPU encoder: libx264")
            try:
                if audio_clip:
                    final_comp = comp.set_audio(audio_clip)
                else:
                    final_comp = comp
                    
                final_comp.write_videofile(
                    str(temp_path),
                    fps=fps,
                    codec="libx264",
                    ffmpeg_params=["-preset", "fast", "-crf", "23"],
                    audio_codec="aac",
                    temp_audiofile_path=str(outdir / "temp_audio.m4a"),
                    remove_temp=True,
                    logger=logger
                )
                
                temp_path.rename(output_path)
                return output_path
                
            except Exception as e2:
                print(f"   -> render failed: {e2}")
                if temp_path.exists():
                    temp_path.unlink()
                return None
        else:
            return None

async def main():
    """Main function - EXACT working version"""
    # ✅ FIX: Detect if running in headless mode
    headless_mode = True  # Always headless in containers
    
    if headless_mode:
        print(f"[HEADLESS MODE] Worker {WORKER_ID}/{TOTAL_WORKERS}")
        silent = True
    else:
        print("[LOCAL MODE]")
        silent = False
    
    print(f"[INFO] Calendly URL: {CALENDLY_URL}")
    
    grand_start = time.time()
    
    # Download CSV
    csv_path = outdir / "master.csv"
    print("[INFO] Downloading master.csv from R2...")
    if download_from_r2("master.csv", csv_path):
        print("[SUCCESS] CSV downloaded")
    else:
        print("[ERROR] Failed to download CSV")
        return
    
    # Read CSV to find required overlays
    print(f"[INFO] Reading CSV to find required overlays: {csv_path}")
    with open(csv_path, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        rows = list(reader)
    
    unique_niches = set(row['Niche'] for row in rows)
    print(f"[INFO] Found {len(unique_niches)} unique niches: {list(unique_niches)}")
    
    # Download overlay videos
    overlay_clips = {}
    for niche in unique_niches:
        overlay_path = outdir / f"{niche}.mp4"
        print(f"[INFO] Downloading {niche}.mp4 from R2...")
        if download_from_r2(f"{niche}.mp4", overlay_path):
            print(f"[SUCCESS] Downloaded {niche}.mp4")
            overlay_clips[niche] = overlay_path
        else:
            print(f"[ERROR] Failed to download {niche}.mp4")
    
    # Filter rows for this worker
    if TOTAL_WORKERS > 1:
        rows = [row for i, row in enumerate(rows) if i % TOTAL_WORKERS == WORKER_ID]
    
    print(f"[INFO] {len(rows)} rows | {WIDTH}x{HEIGHT}@{FPS}")
    
    results = []
    
    # Initialize browser
    async with async_playwright() as pw:
        browser = await pw.chromium.launch(headless=headless_mode)
        context = await browser.new_context(
            viewport={"width": WIDTH, "height": HEIGHT},
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
        )
        
        for i, row in enumerate(rows, 1):
            video_start = time.time()
            
            url = row['Website URL']
            username = row['Instagram Username'] 
            niche = row['Niche']
            
            print(f"[{i}/{len(rows)}] {url} | {username} | niche: {niche}")
            
            if niche not in overlay_clips:
                print(f"   -> skipping: no overlay for {niche}")
                continue
            
            # Create output paths
            video_output = outdir / f"{username}.mp4"
            screenshot_path = outdir / f"{username}_screenshot.jpg"
            landing_path = outdir / f"{username}_landing.html"
            
            try:
                # Take screenshot
                page = await context.new_page()
                await page.goto(url, wait_until="networkidle", timeout=30000)
                await page.screenshot(path=str(screenshot_path), full_page=True)
                await page.close()
                
                # Load overlay video
                overlay_clip = VideoFileClip(str(overlay_clips[niche]))
                overlay_duration = overlay_clip.duration
                
                # Create scrolling background
                scroll_clip = create_scrolling_clip(screenshot_path, overlay_duration, FPS)
                
                # Resize overlay to corner
                overlay_resized = overlay_clip.resize(width=WIDTH//4)
                overlay_positioned = overlay_resized.set_position(('right', 'bottom'))
                
                # Composite
                final_comp = CompositeVideoClip([scroll_clip, overlay_positioned], size=(WIDTH, HEIGHT))
                
                # Render video
                final_path = write_video_atomic(final_comp, video_output, FPS, overlay_clip.audio, logger='bar' if not silent else None)
                
                if final_path and final_path.exists():
                    # Upload video
                    video_url = upload_to_r2(final_path, f"{username}/video.mp4")
                    
                    # Upload thumbnail
                    thumbnail_url = upload_to_r2(screenshot_path, f"{username}/thumbnail.jpg")
                    
                    # Generate and upload landing page
                    landing_html = generate_landing_page_html(username, video_url, thumbnail_url)
                    landing_path.write_text(landing_html, encoding='utf-8')
                    landing_url = upload_to_r2(landing_path, f"{username}/index.html")
                    
                    print(f"   -> landing page: {landing_url}")
                    
                    per_video = time.time() - video_start
                    total_elapsed = time.time() - grand_start
                    print(f"   -> saved {final_path.name} | {per_video:.1f}s | ⏱ {timedelta(seconds=int(total_elapsed))}")
                    
                    result = {
                        "Website URL": url,
                        "Instagram Username": username,
                        "Niche": niche,
                        "Video Link": landing_url or final_path.resolve().as_uri()
                    }
                    results.append(result)
                else:
                    print(f"   -> render failed")
                
                # Cleanup
                overlay_clip.close()
                final_comp.close()
                
            except Exception as e:
                print(f"   -> error: {e}")
                continue
            finally:
                # Cleanup files
                for temp_file in [screenshot_path, landing_path]:
                    if temp_file.exists():
                        temp_file.unlink()
        
        await context.close()
        await browser.close()
    
    # Save results
    res_csv = outdir / f"RESULTS_worker{WORKER_ID}.csv"
    try:
        with open(res_csv, "w", encoding="utf-8", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=["Website URL", "Instagram Username", "Niche", "Video Link"])
            writer.writeheader()
            writer.writerows(results)
    except Exception as e:
        print(f"[WARN] Could not write results CSV: {e}")
    
    # Upload results
    if s3_client and res_csv.exists():
        try:
            upload_to_r2(res_csv, f"results/{res_csv.name}")
            print("[SUCCESS] Results uploaded")
        except Exception as e:
            print(f"[WARN] Results upload failed: {e}")
    
    print(f"\n✅ Done. {len(results)}/{len(rows)} videos. Results: {res_csv}")
    print(f"⏱️ Total elapsed: {timedelta(seconds=int(time.time()-grand_start))}")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n[ABORTED]")
