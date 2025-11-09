#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os, csv, time, hashlib, subprocess, re, random, shutil
from pathlib import Path
from datetime import timedelta

import numpy as np
from PIL import Image
from moviepy.editor import VideoFileClip, CompositeVideoClip, VideoClip
from proglog import ProgressBarLogger
from playwright.sync_api import sync_playwright

# ================== TUNING ==================
SEGMENT_MIN_SEC = 2  # Shorter pause at top
SEGMENT_MAX_SEC = 3
FPS             = 12
WIDTH, HEIGHT   = 1280, 720  # ✅ UPGRADED TO 720p

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
ZERO_WIDTH = ''.join(['\ufeff','\u200b','\u200c','\u200d','\u2060','\u200e','\u200f'])

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
    if client is None:
        return None
    try:
        key = f"{username}/video.mp4"
        client.upload_file(
            str(local_path),
            R2_BUCKET,
            key,
            ExtraArgs={'ContentType': 'video/mp4'}
        )
        return f"{R2_PUBLIC_URL}/{username}/video.mp4"
    except Exception as e:
        print(f"   -> R2 upload failed: {e}")
        return None

def upload_thumbnail_to_r2(client, thumbnail_path, username):
    """Upload thumbnail to R2"""
    if client is None:
        return None
    try:
        key = f"{username}/thumbnail.jpg"
        client.upload_file(
            str(thumbnail_path),
            R2_BUCKET,
            key,
            ExtraArgs={'ContentType': 'image/jpeg'}
        )
        return f"{R2_PUBLIC_URL}/{username}/thumbnail.jpg"
    except Exception as e:
        print(f"   -> Thumbnail upload failed: {e}")
        return None

def extract_thumbnail(video_path, thumbnail_path):
    """Extract thumbnail from video at 2 seconds"""
    try:
        cmd = [
            "ffmpeg", "-y", "-i", str(video_path),
            "-ss", "00:00:02",
            "-vframes", "1",
            "-q:v", "2",
            str(thumbnail_path)
        ]
        subprocess.run(cmd, check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        return True
    except Exception as e:
        print(f"   -> Thumbnail extraction failed: {e}")
        return False

def create_landing_page(client, username, video_url, thumbnail_url):
    """Create landing page with Open Graph tags"""
    if client is None:
        return None
    
    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Video for {username}</title>
    
    <!-- Open Graph tags for social media -->
    <meta property="og:title" content="I recorded this video for you">
    <meta property="og:description" content="Personalized video message for {username}">
    <meta property="og:image" content="{thumbnail_url}">
    <meta property="og:video" content="{video_url}">
    <meta property="og:type" content="video.other">
    <meta property="og:url" content="{R2_PUBLIC_URL}/{username}/index.html">
    
    <!-- Twitter Card tags -->
    <meta name="twitter:card" content="player">
    <meta name="twitter:title" content="I recorded this video for you">
    <meta name="twitter:description" content="Personalized video message">
    <meta name="twitter:image" content="{thumbnail_url}">
    
    <style>
        * {{ margin: 0; padding: 0; box-sizing: border-box; }}
        body {{
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Oxygen, Ubuntu, Cantarell, sans-serif;
            background: #f5f5f5;
            display: flex;
            flex-direction: column;
            align-items: center;
            justify-content: center;
            min-height: 100vh;
            padding: 20px;
        }}
        .container {{
            max-width: 1200px;
            width: 100%;
            background: white;
            border-radius: 12px;
            box-shadow: 0 4px 6px rgba(0,0,0,0.1);
            overflow: hidden;
        }}
        h1 {{
            text-align: center;
            padding: 40px 20px 20px;
            font-size: 2.5rem;
            color: #333;
        }}
        .subtitle {{
            text-align: center;
            padding: 0 20px 30px;
            font-size: 1.1rem;
            color: #666;
        }}
        .video-wrapper {{
            width: 100%;
            padding: 0 40px 40px;
        }}
        video {{
            width: 100%;
            max-width: 100%;
            border-radius: 8px;
            box-shadow: 0 2px 8px rgba(0,0,0,0.15);
        }}
        .cta-section {{
            text-align: center;
            padding: 40px 20px;
            background: #f9f9f9;
        }}
        .cta-button {{
            display: inline-block;
            background: #5b4cdb;
            color: white;
            padding: 16px 48px;
            font-size: 1.1rem;
            font-weight: 600;
            text-decoration: none;
            border-radius: 8px;
            transition: all 0.3s ease;
            box-shadow: 0 4px 12px rgba(91,76,219,0.3);
        }}
        .cta-button:hover {{
            background: #4a3dc4;
            transform: translateY(-2px);
            box-shadow: 0 6px 16px rgba(91,76,219,0.4);
        }}
        @media (max-width: 768px) {{
            h1 {{
                font-size: 1.8rem;
            }}
            .video-wrapper {{
                padding: 0 20px 30px;
            }}
            .cta-button {{
                padding: 14px 36px;
                font-size: 1rem;
            }}
        }}
    </style>
</head>
<body>
    <div class="container">
        <h1>Hi there</h1>
        <p class="subtitle">I recorded this video for you</p>
        <div class="video-wrapper">
            <video controls poster="{thumbnail_url}">
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
        key = f"{username}/index.html"
        client.put_object(
            Bucket=R2_BUCKET,
            Key=key,
            Body=html.encode('utf-8'),
            ContentType='text/html'
        )
        return f"{R2_PUBLIC_URL}/{username}/index.html"
    except Exception as e:
        print(f"   -> Landing page creation failed: {e}")
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

def capture_fullpage_png(page, url, out_png, width, height):
    """✅ FIXED: Simple, reliable screenshot function"""
    try:
        page.goto(url, wait_until="domcontentloaded", timeout=30000)
        page.wait_for_timeout(2000)
        page.screenshot(path=str(out_png), full_page=True)
        return True
    except Exception as e:
        print(f"   -> screenshot failed: {e}")
        return False

def build_scrolling_clip(png_path, w, h, duration, fps, seg_sec):
    """✅ UPDATED: Create 5-second scroll animation"""
    img = np.array(Image.open(png_path).convert("RGB"))
    img_h, img_w, _ = img.shape
    
    if img_h <= h:
        static = np.zeros((h, w, 3), dtype=np.uint8)
        static[:img_h, :min(img_w, w)] = img[:, :min(img_w, w)]
        return VideoClip(lambda t: static, duration=duration).set_fps(fps)
    
    scroll_dist = img_h - h
    
    def make_frame(t):
        if t < seg_sec:
            pos = 0
        elif t >= (duration - seg_sec):
            pos = scroll_dist
        else:
            frac = (t - seg_sec) / (duration - 2*seg_sec)
            pos = int(frac * scroll_dist)
        
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
    """✅ FIXED: Includes pixel format and better quality"""
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
        params = ["-preset","p4","-cq","18","-pix_fmt","yuv420p"]  # ✅ Better quality
    else:
        vcodec = "libx264"
        params = ["-preset","fast","-crf","18"]
    
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

            print(f"[{i}/{total}] {url} | {username} | niche: {niche}")

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
                print("   -> skipped (capture failed)")
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
            
            seg_sec = random.uniform(SEGMENT_MIN_SEC, SEGMENT_MAX_SEC)
            video_start = time.time()
            scroll = None
            face_layer = None
            comp = None
            face_full = None
            
            try:
                face_full = VideoFileClip(str(overlay_path_opt))
                overlay_duration = float(face_full.duration or 30)
                
                # ✅ NEW: Create 5-second scroll and loop it
                scroll_5sec = build_scrolling_clip(shot, WIDTH, HEIGHT, 5.0, FPS, seg_sec)
                num_loops = int(overlay_duration / 5) + 1
                scroll = scroll_5sec.loop(n=num_loops).set_duration(overlay_duration)
                
                layers = [scroll]

                if face_full is not None:
                    width_frac = OVERLAY_W_FRAC_BASE * (1.0 + random.uniform(-OVERLAY_W_JITTER, OVERLAY_W_JITTER))
                    face_w = max(120, int(WIDTH * width_frac))
                    scaled_h = int(face_full.h * (face_w / face_full.w))
                    dx = random.randint(-OVERLAY_POS_JITTER, OVERLAY_POS_JITTER)
                    dy = random.randint(-OVERLAY_POS_JITTER, OVERLAY_POS_JITTER)
                    x = max(SCROLL_MARGIN, min(WIDTH - face_w - SCROLL_MARGIN, WIDTH - face_w - SCROLL_MARGIN + dx))
                    y = max(SCROLL_MARGIN, min(HEIGHT - scaled_h - SCROLL_MARGIN, HEIGHT - scaled_h - SCROLL_MARGIN + dy))
                    face_layer = face_full.resize(width=face_w).set_position((x, y)).subclip(0, overlay_duration)
                    layers.append(face_layer)

                comp = CompositeVideoClip(layers, size=(WIDTH, HEIGHT)).set_duration(overlay_duration)
                if face_full is not None and face_full.audio is not None:
                    comp = comp.set_audio(face_full.audio.subclip(0, overlay_duration))

                final_path = write_video_atomic(comp, outvid, FPS, (face_full.audio if face_full else None), silent)

                # ✅ NEW: Extract thumbnail
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
                        landing_url = create_landing_page(r2_client, username, video_url, thumbnail_url or video_url)
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
