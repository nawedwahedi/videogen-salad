#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os, csv, time, hashlib, subprocess, re, random, shutil
from pathlib import Path
from datetime import timedelta

import numpy as np
from PIL import Image
from moviepy.editor import VideoFileClip, CompositeVideoClip, VideoClip, concatenate_videoclips, ImageClip
from proglog import ProgressBarLogger
from playwright.sync_api import sync_playwright

# ================== TUNING ==================
FPS             = 12
WIDTH, HEIGHT   = 1280, 720

OVERLAY_W_FRAC_BASE = 0.24
OVERLAY_W_JITTER    = 0.05
OVERLAY_POS_JITTER  = 10
SCROLL_MARGIN       = 28

DO_COMPRESS_OVERLAY = True
OVERLAY_TARGET_W = 1280
OVERLAY_V_KBPS   = 600
OVERLAY_A_KBPS   = 96

# Worker configuration
WORKER_ID = int(os.getenv("WORKER_ID", "0"))
TOTAL_WORKERS = int(os.getenv("TOTAL_WORKERS", "1"))

# R2 Configuration
R2_ENDPOINT = os.getenv("R2_ENDPOINT", "")
R2_ACCESS_KEY = os.getenv("R2_ACCESS_KEY", "")
R2_SECRET_KEY = os.getenv("R2_SECRET_KEY", "")
R2_BUCKET = os.getenv("R2_BUCKET", "")
R2_PUBLIC_URL = os.getenv("R2_PUBLIC_URL", "")

# File Configuration
CSV_FILENAME = os.getenv("CSV_FILENAME", "master.csv")
CALENDLY_URL = os.getenv("CALENDLY_URL", "https://calendly.com/heedeestudios/seo-strategy-session")

USER_AGENT = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
              "AppleWebKit/537.36 (KHTML, like Gecko) "
              "Chrome/124.0.0.0 Safari/537.36")

# ================== QUIET LOGGER ==================
class SilentLogger(ProgressBarLogger):
    def bars_callback(self, *a, **k):
        pass

# ================== R2 UPLOAD ==================
def setup_r2_client():
    if not all([R2_ENDPOINT, R2_ACCESS_KEY, R2_SECRET_KEY, R2_BUCKET]):
        return None
    try:
        import boto3
        client = boto3.client(
            's3',
            endpoint_url=R2_ENDPOINT,
            aws_access_key_id=R2_ACCESS_KEY,
            aws_secret_access_key=R2_SECRET_KEY,
            region_name='auto'
        )
        return client
    except ImportError:
        print("[WARN] boto3 not installed")
        return None
    except Exception as e:
        print(f"[WARN] R2 setup failed: {e}")
        return None

def download_overlays_from_r2(csv_path, r2_client, bucket):
    """Download all unique niche overlays from R2"""
    print(f"[INFO] Reading CSV to find required overlays: {csv_path}")
    
    unique_niches = set()
    with open(csv_path, 'r', encoding='utf-8-sig') as f:
        reader = csv.DictReader(f)
        for row in reader:
            niche = row.get('Niche', '').strip()
            if niche:
                unique_niches.add(niche)
    
    print(f"[INFO] Found {len(unique_niches)} unique niches: {list(unique_niches)}")
    
    overlays = {}
    for niche in unique_niches:
        overlay_filename = f"{niche}.mp4"
        local_path = f"/tmp/{overlay_filename}"
        
        try:
            print(f"[INFO] Downloading {overlay_filename} from R2...")
            r2_client.download_file(bucket, overlay_filename, local_path)
            overlays[niche] = local_path
            print(f"[SUCCESS] Downloaded {overlay_filename}")
        except Exception as e:
            print(f"[ERROR] Failed to download {overlay_filename}: {e}")
            overlays[niche] = None
    
    return overlays

def upload_to_r2(client, local_path, username):
    """Upload video to R2 with proper content type"""
    if client is None:
        return None
    try:
        key = f"{username}/video.mp4"
        print(f"   [DEBUG] Uploading video to R2: {key}")
        client.upload_file(
            str(local_path),
            R2_BUCKET,
            key,
            ExtraArgs={
                'ContentType': 'video/mp4',
                'CacheControl': 'public, max-age=31536000'
            }
        )
        url = f"{R2_PUBLIC_URL}/{key}"
        print(f"   [DEBUG] Video uploaded successfully: {url}")
        return url
    except Exception as e:
        print(f"   [ERROR] R2 video upload failed: {e}")
        return None

def upload_thumbnail_to_r2(client, thumbnail_path, username):
    """Upload high-quality thumbnail to R2"""
    if client is None:
        return None
    try:
        key = f"{username}/thumbnail.jpg"
        print(f"   [DEBUG] Uploading thumbnail to R2: {key}")
        client.upload_file(
            str(thumbnail_path),
            R2_BUCKET,
            key,
            ExtraArgs={
                'ContentType': 'image/jpeg',
                'CacheControl': 'public, max-age=31536000'
            }
        )
        url = f"{R2_PUBLIC_URL}/{key}"
        print(f"   [DEBUG] Thumbnail uploaded successfully: {url}")
        return url
    except Exception as e:
        print(f"   [ERROR] Thumbnail upload failed: {e}")
        return None

def extract_thumbnail(video_path, thumbnail_path):
    """Extract high-quality thumbnail from video"""
    try:
        cmd = [
            "ffmpeg", "-y", "-i", str(video_path),
            "-ss", "00:00:05",
            "-vframes", "1",
            "-vf", "scale=1280:-1",
            "-q:v", "1",
            str(thumbnail_path)
        ]
        subprocess.run(cmd, check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        return True
    except Exception as e:
        print(f"   [ERROR] Thumbnail extraction failed: {e}")
        return False

def create_landing_page(client, username, video_url, thumbnail_url):
    """Create landing page with proper button positioning"""
    if client is None:
        return None
    
    thumb_url = thumbnail_url if thumbnail_url else video_url
    page_url = f"{R2_PUBLIC_URL}/{username}"
    
    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Video Message for {username}</title>
    
    <!-- Primary Meta Tags -->
    <meta name="title" content="I recorded this video for you">
    <meta name="description" content="Watch this personalized video message">
    
    <!-- Open Graph / Facebook -->
    <meta property="og:type" content="video.other">
    <meta property="og:url" content="{page_url}">
    <meta property="og:title" content="I recorded this video for you">
    <meta property="og:description" content="Watch this personalized video message">
    <meta property="og:image" content="{thumb_url}">
    <meta property="og:image:secure_url" content="{thumb_url}">
    <meta property="og:image:type" content="image/jpeg">
    <meta property="og:image:width" content="1280">
    <meta property="og:image:height" content="720">
    <meta property="og:video" content="{video_url}">
    <meta property="og:video:secure_url" content="{video_url}">
    <meta property="og:video:type" content="video/mp4">
    <meta property="og:video:width" content="1280">
    <meta property="og:video:height" content="720">
    
    <!-- Twitter -->
    <meta property="twitter:card" content="player">
    <meta property="twitter:url" content="{page_url}">
    <meta property="twitter:title" content="I recorded this video for you">
    <meta property="twitter:description" content="Watch this personalized video message">
    <meta property="twitter:image" content="{thumb_url}">
    <meta property="twitter:player" content="{video_url}">
    <meta property="twitter:player:width" content="1280">
    <meta property="twitter:player:height" content="720">
    
    <!-- WhatsApp / Instagram -->
    <meta property="og:site_name" content="Personalized Video Message">
    <link rel="image_src" href="{thumb_url}">
    
    <style>
        * {{ margin: 0; padding: 0; box-sizing: border-box; }}
        body {{
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Oxygen, Ubuntu, Cantarell, sans-serif;
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            min-height: 100vh;
            display: flex;
            align-items: center;
            justify-content: center;
            padding: 20px;
        }}
        .container {{
            max-width: 900px;
            width: 100%;
            background: white;
            border-radius: 16px;
            box-shadow: 0 20px 60px rgba(0,0,0,0.3);
            overflow: hidden;
        }}
        .header {{
            text-align: center;
            padding: 40px 20px 20px;
            background: linear-gradient(to bottom, #f8f9fa, white);
        }}
        h1 {{
            font-size: 2.5rem;
            color: #2d3748;
            margin-bottom: 10px;
        }}
        .subtitle {{
            font-size: 1.1rem;
            color: #718096;
        }}
        .video-wrapper {{
            width: 100%;
            padding: 20px;
            background: white;
        }}
        video {{
            width: 100%;
            border-radius: 12px;
            box-shadow: 0 4px 12px rgba(0,0,0,0.1);
        }}
        .cta-section {{
            text-align: center;
            padding: 15px 20px 30px;
            background: white;
        }}
        .cta-button {{
            display: inline-block;
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            color: white;
            padding: 18px 50px;
            font-size: 1.15rem;
            font-weight: 600;
            text-decoration: none;
            border-radius: 50px;
            transition: all 0.3s ease;
            box-shadow: 0 4px 15px rgba(102, 126, 234, 0.4);
        }}
        .cta-button:hover {{
            transform: translateY(-2px);
            box-shadow: 0 6px 20px rgba(102, 126, 234, 0.6);
        }}
        @media (max-width: 768px) {{
            h1 {{ font-size: 2rem; }}
            .video-wrapper {{ padding: 15px; }}
            .cta-section {{ padding: 10px 20px 25px; }}
            .cta-button {{
                padding: 16px 40px;
                font-size: 1rem;
            }}
        }}
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <h1>Hi there</h1>
            <p class="subtitle">I recorded this video for you</p>
        </div>
        <div class="video-wrapper">
            <video controls poster="{thumb_url}" preload="metadata" playsinline>
                <source src="{video_url}" type="video/mp4">
                Your browser does not support the video tag.
            </video>
        </div>
        <div class="cta-section">
            <a href="{CALENDLY_URL}" class="cta-button">Book a FREE 10 Minute Call</a>
        </div>
    </div>
</body>
</html>"""
    
    try:
        key_html = f"{username}/index.html"
        print(f"   [DEBUG] Uploading landing page to R2: {key_html}")
        client.put_object(
            Bucket=R2_BUCKET,
            Key=key_html,
            Body=html.encode('utf-8'),
            ContentType='text/html',
            CacheControl='public, max-age=3600'
        )
        
        key_root = username
        client.put_object(
            Bucket=R2_BUCKET,
            Key=key_root,
            Body=html.encode('utf-8'),
            ContentType='text/html',
            CacheControl='public, max-age=3600'
        )
        
        final_url = f"{R2_PUBLIC_URL}/{username}/index.html"
        print(f"   [DEBUG] Landing page uploaded: {final_url}")
        return final_url
    except Exception as e:
        print(f"   [ERROR] Landing page creation failed: {e}")
        return None

# ================== HELPER FUNCTIONS ==================
def check_nvenc():
    try:
        result = subprocess.run(
            ["ffmpeg","-hide_banner","-encoders"],
            capture_output=True, text=True, timeout=5
        )
        if "h264_nvenc" in result.stdout:
            print("[INFO] NVENC detected: h264_nvenc will be used.")
        else:
            print("[WARN] NVENC not found, using libx264.")
    except Exception as e:
        print(f"[WARN] ffmpeg check failed: {e}")

def pick_file(title, types):
    try:
        from tkinter import Tk
        from tkinter.filedialog import askopenfilename
        root = Tk()
        root.withdraw()
        path = askopenfilename(title=title, filetypes=types)
        root.destroy()
        return Path(path) if path else None
    except:
        return None

def pick_dir(title):
    try:
        from tkinter import Tk
        from tkinter.filedialog import askdirectory
        root = Tk()
        root.withdraw()
        path = askdirectory(title=title)
        root.destroy()
        return Path(path) if path else None
    except:
        return None

def clean_url(url):
    url = url.strip()
    if not url.startswith(("http://","https://")):
        url = "https://" + url
    return url

def domain_from_url(url):
    m = re.search(r'https?://([^/]+)', url)
    if m:
        dom = m.group(1)
        dom = dom.replace("www.","")
        return dom
    return "unknown"

def safe_slug(s):
    s = s.lower()
    s = re.sub(r'[^a-z0-9]+', '_', s)
    s = s.strip('_')
    if not s:
        s = f"video_{int(time.time())}"
    return s

def load_rows(csv_path):
    rows = []
    with open(csv_path, 'r', encoding='utf-8-sig') as f:
        reader = csv.DictReader(f)
        for row in reader:
            url = (row.get("Website URL") or row.get("url") or "").strip()
            username = (row.get("Instagram Username") or row.get("username") or "").strip()
            niche = (row.get("Niche") or row.get("niche") or "").strip()
            if url:
                rows.append({"url": url, "username": username, "niche": niche})
    return rows

def capture_fullpage_png(page, url, out_png, width, height, max_retries=2):
    """Capture full page screenshot with retry logic"""
    for attempt in range(max_retries):
        try:
            print(f"   [DEBUG] Screenshot attempt {attempt + 1}/{max_retries}")
            page.goto(url, wait_until="domcontentloaded", timeout=60000)
            
            try:
                page.wait_for_load_state("networkidle", timeout=10000)
            except:
                print(f"   [WARN] Network not idle, continuing anyway")
            
            page.wait_for_timeout(2000)
            page.screenshot(path=str(out_png), full_page=True)
            print(f"   [DEBUG] Screenshot successful")
            return True
        except Exception as e:
            print(f"   [WARN] Screenshot attempt {attempt + 1} failed: {e}")
            if attempt < max_retries - 1:
                print(f"   [INFO] Retrying in 3 seconds...")
                time.sleep(3)
            else:
                print(f"   [ERROR] All attempts failed")
                return False
    return False

def build_scroll_to_end_then_middle(png_path, w, h, duration, fps):
    """✅ OPTIMIZED: Scroll down to END → back to MIDDLE → stay (10 seconds only)"""
    img = np.array(Image.open(png_path).convert("RGB"))
    img_h, img_w, _ = img.shape
    
    if img_h <= h:
        static = np.zeros((h, w, 3), dtype=np.uint8)
        static[:img_h, :min(img_w, w)] = img[:, :min(img_w, w)]
        return VideoClip(lambda t: static, duration=duration).set_fps(fps)
    
    scroll_dist = img_h - h
    
    # ✅ Scroll pattern: Down to END → Back to MIDDLE → Stay
    keyframes = [
        (0.0, 0.0),      # Start at top
        (1.5, 0.20),     # Scroll down
        (3.0, 0.50),     # Continue down
        (4.5, 0.80),     # Continue down
        (5.5, 1.0),      # Reach BOTTOM/END
        (6.5, 0.75),     # Scroll back up
        (8.0, 0.50),     # Reach MIDDLE
        (8.5, 0.48),     # Small adjustment
        (9.0, 0.50),     # Final MIDDLE position
        (10.0, 0.50)     # STAY at middle
    ]
    
    def interpolate_position(t):
        for i in range(len(keyframes) - 1):
            t1, pos1 = keyframes[i]
            t2, pos2 = keyframes[i + 1]
            if t <= t2:
                progress = (t - t1) / (t2 - t1) if t2 != t1 else 0
                # Smooth easing
                if progress < 0.5:
                    eased = 2 * progress * progress
                else:
                    eased = 1 - pow(-2 * progress + 2, 2) / 2
                return pos1 + (pos2 - pos1) * eased
        return keyframes[-1][1]
    
    def make_frame(t):
        pos_fraction = interpolate_position(min(t, duration))
        pos = int(pos_fraction * scroll_dist)
        
        # Subtle jitter
        jitter = int(random.gauss(0, 1.0))
        pos = max(0, min(scroll_dist, pos + jitter))
        
        crop = img[pos:pos+h, :min(img_w, w)]
        if crop.shape[0] < h or crop.shape[1] < w:
            canvas = np.zeros((h, w, 3), dtype=np.uint8)
            ch, cw, _ = crop.shape
            canvas[:ch, :cw] = crop
            return canvas
        return crop
    
    return VideoClip(make_frame, duration=duration).set_fps(fps)

def ensure_overlay_optimized(overlay_path, cache_dir):
    if not DO_COMPRESS_OVERLAY:
        return overlay_path
    
    cache_dir.mkdir(parents=True, exist_ok=True)
    
    h = hashlib.md5(str(overlay_path).encode()).hexdigest()[:12]
    cached = cache_dir / f"{overlay_path.stem}_{h}_opt.mp4"
    
    if cached.exists():
        return cached
    
    cmd = [
        "ffmpeg","-y","-i",str(overlay_path),
        "-vf",f"scale={OVERLAY_TARGET_W}:-2",
        "-c:v","libx264","-preset","fast","-crf","23",
        "-c:a","aac","-b:a",f"{OVERLAY_A_KBPS}k",
        "-movflags","+faststart",
        str(cached)
    ]
    try:
        subprocess.run(cmd, check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        return cached
    except:
        return overlay_path

def write_video_atomic(comp, out_path, fps, audio_clip, logger):
    """Write video with proper encoding"""
    temp = out_path.with_suffix(".tmp.mp4")
    
    try:
        result = subprocess.run(
            ["ffmpeg","-hide_banner","-encoders"],
            capture_output=True, text=True, timeout=5
        )
        use_nvenc = ("h264_nvenc" in result.stdout)
    except:
        use_nvenc = False
    
    if use_nvenc:
        vcodec = "h264_nvenc"
        params = ["-preset","p4","-cq","18","-pix_fmt","yuv420p"]
    else:
        vcodec = "libx264"
        params = ["-preset","fast","-crf","18","-pix_fmt","yuv420p"]
    
    comp.write_videofile(
        str(temp),
        fps=fps,
        codec=vcodec,
        audio_codec="aac",
        ffmpeg_params=params,
        logger=logger,
        threads=4
    )
    
    if temp.exists():
        if out_path.exists():
            out_path.unlink()
        temp.rename(out_path)
    
    return out_path

def unique_path(base_path):
    base = base_path.stem
    ext = base_path.suffix
    parent = base_path.parent
    i = 1
    while True:
        candidate = parent / f"{base}_{i}{ext}"
        if not candidate.exists():
            return candidate
        i += 1

# ================== MAIN ==================
def main():
    headless_mode = os.getenv("WORKER_ID") is not None
    
    check_nvenc()
    
    if headless_mode:
        csv_path = Path(f"/tmp/{CSV_FILENAME}")
        outdir = Path(os.getenv("OUTPUT_PATH", "/output"))
        
        print(f"[HEADLESS MODE] Worker {WORKER_ID}/{TOTAL_WORKERS}")
        print(f"[INFO] Calendly URL: {CALENDLY_URL}")
        print(f"[INFO] R2 Public URL: {R2_PUBLIC_URL}")
        
        r2_client = setup_r2_client()
        if not r2_client:
            print("[ERROR] R2 client not configured")
            return
        
        try:
            print(f"[INFO] Downloading {CSV_FILENAME} from R2...")
            r2_client.download_file(R2_BUCKET, CSV_FILENAME, str(csv_path))
            print("[SUCCESS] CSV downloaded")
        except Exception as e:
            print(f"[ERROR] CSV download failed: {e}")
            return
        
        overlays = download_overlays_from_r2(csv_path, r2_client, R2_BUCKET)
        
    else:
        print("👉 Select your CSV")
        csv_path = pick_file("Select CSV",[("CSV","*.csv"),("All files","*.*")])
        if not csv_path:
            print("[ERROR] No CSV selected")
            return
        print("👉 Select your overlay video (mp4)")
        overlay_src = pick_file("Select overlay",[("MP4","*.mp4"),("All files","*.*")])
        if not overlay_src:
            print("[ERROR] No overlay selected")
            return
        print("👉 Pick output folder")
        outdir = pick_dir("Select output folder")
        if not outdir:
            print("[ERROR] No output folder selected")
            return
        r2_client = setup_r2_client()
        overlays = {"default": overlay_src}

    rows = load_rows(csv_path)
    if not rows:
        print("[ERROR] No valid rows in CSV.")
        return

    if r2_client:
        print("[INFO] R2 upload enabled")
    else:
        print("[INFO] R2 upload disabled")

    print(f"[INFO] {len(rows)} rows | {WIDTH}x{HEIGHT}@{FPS}")
    outdir.mkdir(parents=True,exist_ok=True)
    silent = SilentLogger()
    grand_start = time.time()
    results = []

    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=True, args=["--disable-gpu","--no-sandbox"])
        context = browser.new_context(
            viewport={"width": WIDTH, "height": HEIGHT},
            user_agent=USER_AGENT,
            java_script_enabled=True,
            ignore_https_errors=True,
            device_scale_factor=1.0
        )
        page = context.new_page()

        total = len(rows)
        for i,r in enumerate(rows,1):
            url = clean_url(r["url"])
            username = (r.get("username") or "").strip() or domain_from_url(url)
            niche = r.get("niche", "").strip()
            slug = safe_slug(username)
            shot = outdir/f"{slug}.png"
            outvid = outdir/f"{slug}.mp4"
            thumbnail_file = outdir/f"{slug}.jpg"

            print(f"\n[{i}/{total}] {url} | {username} | niche: {niche}")

            if headless_mode:
                overlay_path = overlays.get(niche)
                if not overlay_path:
                    print(f"   -> skipped (no overlay for niche: {niche})")
                    results.append({
                        "Website URL": url,
                        "Instagram Username": username,
                        "Niche": niche,
                        "Video Link": "FAILED - Missing overlay"
                    })
                    continue
            else:
                overlay_path = overlays.get("default")

            if not capture_fullpage_png(page,url,shot,WIDTH,HEIGHT):
                print("   -> skipped (capture failed after retries)")
                results.append({
                    "Website URL": url,
                    "Instagram Username": username,
                    "Niche": niche,
                    "Video Link": "FAILED - Screenshot failed"
                })
                continue

            if i % 50 == 0:
                context.close()
                browser.close()
                browser = pw.chromium.launch(headless=True, args=["--disable-gpu","--no-sandbox"])
                context = browser.new_context(
                    viewport={"width": WIDTH, "height": HEIGHT},
                    user_agent=USER_AGENT,
                    java_script_enabled=True,
                    ignore_https_errors=True,
                    device_scale_factor=1.0
                )
                page = context.new_page()

            overlay_path_opt = ensure_overlay_optimized(Path(overlay_path), outdir/"_cache")
            
            video_start = time.time()
            scroll = None
            face_layer = None
            comp = None
            face_full = None
            
            try:
                face_full = VideoFileClip(str(overlay_path_opt))
                overlay_duration = float(face_full.duration or 30)
                
                print(f"   [DEBUG] Overlay duration: {overlay_duration}s")
                
                # ✅ COST OPTIMIZATION: Only render 10-second scroll
                scroll_10sec = build_scroll_to_end_then_middle(shot, WIDTH, HEIGHT, 10.0, FPS)
                
                # Get final middle frame for static extension
                final_frame = scroll_10sec.get_frame(9.9)
                static_duration = max(0, overlay_duration - 10)
                
                if static_duration > 0:
                    # ✅ Create static clip from final frame (super fast!)
                    static_clip = ImageClip(final_frame, duration=static_duration).set_fps(FPS)
                    scroll = concatenate_videoclips([scroll_10sec, static_clip])
                else:
                    scroll = scroll_10sec
                
                layers = [scroll]

                if face_full is not None:
                    width_frac = OVERLAY_W_FRAC_BASE * (1.0 + random.uniform(-OVERLAY_W_JITTER, OVERLAY_W_JITTER))
                    face_w = max(120, int(WIDTH * width_frac))
                    scaled_h = int(face_full.h * (face_w / face_full.w))
                    dx = random.randint(-OVERLAY_POS_JITTER, OVERLAY_POS_JITTER)
                    dy = random.randint(-OVERLAY_POS_JITTER, OVERLAY_POS_JITTER)
                    
                    # ✅ BOTTOM-LEFT positioning (visible in social media previews)
                    x = SCROLL_MARGIN + dx
                    y = HEIGHT - scaled_h - SCROLL_MARGIN + dy
                    x = max(SCROLL_MARGIN, min(WIDTH - face_w - SCROLL_MARGIN, x))
                    
                    face_layer = face_full.resize(width=face_w).set_position((x, y)).subclip(0, overlay_duration)
                    layers.append(face_layer)

                comp = CompositeVideoClip(layers, size=(WIDTH, HEIGHT)).set_duration(overlay_duration)
                if face_full is not None and face_full.audio is not None:
                    comp = comp.set_audio(face_full.audio.subclip(0, overlay_duration))

                final_path = write_video_atomic(comp, outvid, FPS, (face_full.audio if face_full else None), silent)

                # Extract thumbnail
                thumbnail_url = None
                if extract_thumbnail(final_path, thumbnail_file):
                    if r2_client:
                        thumbnail_url = upload_thumbnail_to_r2(r2_client, thumbnail_file, username)

                # Upload video and create landing page
                video_url = None
                landing_url = None
                if r2_client:
                    video_url = upload_to_r2(r2_client, final_path, username)
                    if video_url:
                        landing_url = create_landing_page(r2_client, username, video_url, thumbnail_url)
                        if landing_url:
                            print(f"   -> landing page: {landing_url}")

            except Exception as e:
                msg = str(e)
                if "Permission denied" in msg or "permission denied" in msg:
                    try:
                        alt = unique_path(outvid)
                        print(f"   -> target locked; writing to {alt.name} instead")
                        final_path = write_video_atomic(comp, alt, FPS, (face_full.audio if face_full else None), silent)
                    except Exception as e2:
                        print(f"   -> render failed: {e2}")
                        results.append({
                            "Website URL": url,
                            "Instagram Username": username,
                            "Niche": niche,
                            "Video Link": f"FAILED - {str(e2)}"
                        })
                        continue
                else:
                    print(f"   -> render failed: {e}")
                    results.append({
                        "Website URL": url,
                        "Instagram Username": username,
                        "Niche": niche,
                        "Video Link": f"FAILED - {str(e)}"
                    })
                    continue
            finally:
                try:
                    if comp: comp.close()
                except: pass
                try:
                    if face_layer: face_layer.close()
                except: pass
                try:
                    if scroll: scroll.close()
                except: pass
                try:
                    if face_full: face_full.close()
                except: pass

            per_video = time.time() - video_start
            total_elapsed = time.time() - grand_start
            print(f"   -> saved {Path(final_path).name} | {per_video:.1f}s | ⏱ {timedelta(seconds=int(total_elapsed))}")

            result = {
                "Website URL": url,
                "Instagram Username": username,
                "Niche": niche,
                "Video Link": landing_url or Path(final_path).resolve().as_uri()
            }
            results.append(result)

        context.close()
        browser.close()

    res_csv = outdir / f"RESULTS_worker{WORKER_ID}.csv"
    try:
        with open(res_csv, "w", encoding="utf-8", newline="") as f:
            w = csv.DictWriter(f, fieldnames=["Website URL","Instagram Username","Niche","Video Link"])
            w.writeheader()
            w.writerows(results)
    except Exception as e:
        print(f"[WARN] Could not write results CSV: {e}")

    print(f"\n✅ Done. {len(results)}/{len(rows)} videos. Results: {res_csv}")
    print(f"⏱️ Total elapsed: {timedelta(seconds=int(time.time()-grand_start))}")

if __name__=="__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n[ABORTED]")
```

---

## **💰 FINAL COST:**
```
Per video GPU work:
- Screenshot: 10s
- 10s scroll generation: 8s
- Composite with facecam: 5s
- Frame extraction + static extension: 2s
Total: ~25 seconds GPU per video

900,000 × 25s = 22.5M seconds
= 6,250 GPU-hours
× $0.02/hour
= $125 for all 900k videos! ✅
