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
SEGMENT_MIN_SEC = 28
SEGMENT_MAX_SEC = 34
FPS             = 12
WIDTH, HEIGHT   = 854, 480

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
    
    # Read CSV and find unique niches
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

def create_landing_page(client, username, video_url):
    if client is None:
        return None
    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Video for {username}</title>
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
            <video controls>
                <source src="{video_url}" type="video/mp4">
                Your browser does not support the video tag.
            </video>
        </div>
        <div class="cta-section">
            <a href="{CALENDLY_URL}" class="cta-button" target="_blank">Book a FREE 10 Minute Call</a>
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
        return f"{R2_PUBLIC_URL}/{username}/"
    except Exception as e:
        print(f"   -> Landing page failed: {e}")
        return None

# ================== FILE PICKERS ==================
def pick_file(title, kinds):
    try:
        from tkinter import Tk, filedialog
        root = Tk(); root.withdraw()
        p = filedialog.askopenfilename(title=title, filetypes=kinds)
        root.destroy()
        if not p: raise KeyboardInterrupt
        return Path(p)
    except:
        return None

def pick_dir(title):
    try:
        from tkinter import Tk, filedialog
        root = Tk(); root.withdraw()
        p = filedialog.askdirectory(title=title)
        root.destroy()
        if not p: raise KeyboardInterrupt
        return Path(p)
    except:
        return None

# ================== CSV HELPERS ==================
def clean_url(u):
    if not u: return ""
    u = u.strip().replace('\xa0',' ')
    u = u.translate({ord(c): None for c in ZERO_WIDTH})
    u = re.sub(r'\s+','',u)
    u = u.replace("http://https://","https://").replace("https://http://","http://")
    if not (u.startswith("http://") or u.startswith("https://")):
        u = "https://" + u
    return u

def safe_slug(s):
    keep = "-_.() abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
    s = "".join(c if c in keep else "_" for c in (s or "").strip())
    return s[:150] or "output"

def domain_from_url(u):
    try: return re.sub(r'^https?://','',u).split('/')[0].replace('.','_')
    except: return "site"

def load_rows(csv_path):
    rows = []
    with open(csv_path, "r", encoding="utf-8-sig", newline="") as f:
        r = csv.reader(f)
        raw = [row for row in r if row and any((cell or "").strip() for cell in row)]
    if not raw: return rows
    header = [c.strip().lower() for c in raw[0]]
    def find_col(names, opts):
        for i,n in enumerate(names):
            if n in opts: return i
        return None
    
    # Find columns
    url_i = find_col(header, {"website url","url","website","link"})
    ig_i  = find_col(header, {"instagram username","username","instagram","handle"})
    niche_i = find_col(header, {"niche"})
    
    data = raw[1:]
    for row in data:
        url = clean_url(row[url_i]) if url_i is not None and url_i < len(row) else ""
        ig  = (row[ig_i].strip() if ig_i is not None and ig_i < len(row) else "")
        niche = (row[niche_i].strip() if niche_i is not None and niche_i < len(row) else "")
        if url:
            rows.append({"url":url,"username":ig,"niche":niche})
    
    if TOTAL_WORKERS > 1:
        rows = [r for i, r in enumerate(rows) if i % TOTAL_WORKERS == WORKER_ID]
        print(f"[WORKER {WORKER_ID}/{TOTAL_WORKERS}] Assigned {len(rows)} rows")
    
    return rows

# ================== FFMPEG CHECKS ==================
def probe_hwaccels():
    try:
        raw = subprocess.check_output(["ffmpeg","-hide_banner","-hwaccels"], stderr=subprocess.PIPE, text=True)
        return [l.strip() for l in raw.split('\n') if l.strip()]
    except: return []

def guess_encoder():
    if shutil.which("ffmpeg") is None: return "libx264"
    hws = probe_hwaccels()
    if any("nvenc" in h.lower() or "cuda" in h.lower() for h in hws):
        try:
            test = subprocess.run(
                ["ffmpeg","-f","lavfi","-i","nullsrc=s=64x64:d=0.1","-c:v","h264_nvenc","-f","null","-"],
                stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, timeout=2
            )
            if test.returncode == 0:
                print("[INFO] NVENC detected: h264_nvenc will be used.")
                return "h264_nvenc"
        except: pass
    print("[INFO] Falling back to CPU encoder: libx264")
    return "libx264"

ENCODER = guess_encoder()

# ================== OVERLAY OPTIMIZATION ==================
def ensure_overlay_optimized(src_path, cache_dir):
    if not DO_COMPRESS_OVERLAY: return src_path
    cache_dir.mkdir(parents=True, exist_ok=True)
    with open(src_path,"rb") as f: hsh = hashlib.md5(f.read()).hexdigest()
    cached = cache_dir / f"overlay_{hsh}.mp4"
    if cached.exists(): return cached
    try:
        probe = subprocess.check_output(
            ["ffprobe","-v","error","-select_streams","v:0",
             "-show_entries","stream=width,height,duration,codec_name",
             "-of","csv=p=0",str(src_path)],
            stderr=subprocess.PIPE, text=True
        ).strip().split(',')
        w, h = int(probe[0]), int(probe[1])
        dur  = float(probe[2]) if len(probe)>2 and probe[2] else 30.0
        codec= probe[3] if len(probe)>3 else ""
    except: return src_path
    if w > OVERLAY_TARGET_W:
        subprocess.run([
            "ffmpeg","-y","-i",str(src_path),
            "-vf",f"scale={OVERLAY_TARGET_W}:-2",
            "-c:v","libx264","-preset","fast","-b:v",f"{OVERLAY_V_KBPS}k",
            "-c:a","aac","-b:a",f"{OVERLAY_A_KBPS}k",
            "-movflags","+faststart",
            str(cached)
        ], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, check=False)
        if cached.exists(): return cached
    return src_path

# ================== PAGE CAPTURE ==================
def capture_fullpage_png(page, url, out_path, width, height, timeout=20000):
    try:
        page.goto(url, timeout=timeout, wait_until="domcontentloaded")
        page.wait_for_timeout(2000)
        body = page.locator("body").first
        try: body.screenshot(path=str(out_path), timeout=10000)
        except: pass
        if not out_path.exists() or out_path.stat().st_size < 500:
            page.screenshot(path=str(out_path), full_page=True, timeout=10000)
        return out_path.exists() and out_path.stat().st_size > 500
    except Exception as e:
        print(f"   -> capture error: {e}")
        return False

# ================== SCROLL CLIP ==================
def build_scrolling_clip(png_path, vid_w, vid_h, dur, fps, target_sec):
    img = Image.open(png_path).convert("RGB")
    arr = np.array(img, dtype=np.uint8)
    img_h = arr.shape[0]
    if img_h <= vid_h: return VideoClip(lambda t: arr, duration=dur).set_fps(fps)
    
    total_scroll = img_h - vid_h
    frames_total = int(target_sec * fps)
    scroll_per_frame = total_scroll / max(frames_total - 1, 1)
    
    def make_frame(t):
        idx = int(t * fps)
        y = min(int(idx * scroll_per_frame), total_scroll)
        return arr[y:y+vid_h, :, :]
    
    return VideoClip(make_frame, duration=dur).set_fps(fps)

# ================== ATOMIC WRITE ==================
def unique_path(p):
    base, ext = p.stem, p.suffix
    for i in range(1,10000):
        candidate = p.parent / f"{base}_{i}{ext}"
        if not candidate.exists(): return candidate
    return p.parent / f"{base}_{int(time.time())}{ext}"

def write_video_atomic(clip, target, fps, audio_clip, logger):
    tmp = target.parent / (target.stem + "_tmp" + target.suffix)
    
    audio_flag = (audio_clip is not None and hasattr(audio_clip, 'duration') and audio_clip.duration > 0)
    
    try:
        if ENCODER == "h264_nvenc":
            clip.write_videofile(
                str(tmp),
                codec="h264_nvenc",
                fps=fps,
                audio=audio_flag,
                audio_codec=("aac" if audio_flag else None),
                preset="p1",
                threads=1,
                temp_audiofile=str(target.parent / (target.stem + "_tmp.m4a")),
                remove_temp=True,
                ffmpeg_params=[
                    "-b:v", "1.2M",
                    "-maxrate", "1.8M",
                    "-bufsize", "3.6M",
                    "-movflags", "+faststart"
                ],
                verbose=False,
                logger=logger
            )
        else:
            clip.write_videofile(
                str(tmp),
                codec="libx264",
                fps=fps,
                audio=audio_flag,
                audio_codec=("aac" if audio_flag else None),
                preset="ultrafast",
                threads=max(1, (os.cpu_count() or 2) - 1),
                temp_audiofile=str(target.parent / (target.stem + "_tmp.m4a")),
                remove_temp=True,
                ffmpeg_params=[
                    "-b:v", "1.2M",
                    "-maxrate", "1.8M",
                    "-bufsize", "3.6M",
                    "-movflags", "+faststart"
                ],
                verbose=False,
                logger=logger
            )
    except Exception as e:
        raise RuntimeError(f"Video write failed: {e}") from e
    
    try:
        if target.exists():
            try:
                target.unlink()
            except Exception:
                target = unique_path(target)
        shutil.move(str(tmp), str(target))
    finally:
        try:
            if tmp.exists():
                tmp.unlink()
        except Exception:
            pass
    return target

# ================== MAIN ==================
def main():
    headless_mode = os.getenv("HEADLESS", "false").lower() == "true"
    
    if headless_mode:
        csv_path = Path(f"/tmp/{CSV_FILENAME}")
        outdir = Path(os.getenv("OUTPUT_PATH", "/output"))
        
        print(f"[HEADLESS MODE] Worker {WORKER_ID}/{TOTAL_WORKERS}")
        print(f"[INFO] Calendly URL: {CALENDLY_URL}")
        
        # Setup R2
        r2_client = setup_r2_client()
        if not r2_client:
            print("[ERROR] R2 client not configured")
            return
        
        # Download CSV from R2
        try:
            print(f"[INFO] Downloading {CSV_FILENAME} from R2...")
            r2_client.download_file(R2_BUCKET, CSV_FILENAME, str(csv_path))
            print("[SUCCESS] CSV downloaded")
        except Exception as e:
            print(f"[ERROR] CSV download failed: {e}")
            return
        
        # Download all required overlays
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

            print(f"[{i}/{total}] {url} | {username} | niche: {niche}")

            # Get overlay for this niche
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

            # Optimize overlay if needed
            overlay_path_opt = ensure_overlay_optimized(Path(overlay_path), outdir/"_cache")
            
            seg_sec = random.uniform(SEGMENT_MIN_SEC, SEGMENT_MAX_SEC)
            video_start = time.time()
            scroll = None
            face_layer = None
            comp = None
            face_full = None
            
            try:
                face_full = VideoFileClip(str(overlay_path_opt))
                overlay_duration = float(face_full.duration or SEGMENT_MIN_SEC)
                
                scroll = build_scrolling_clip(shot,WIDTH,HEIGHT,overlay_duration,FPS,seg_sec)
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

                video_url = None
                landing_url = None
                if r2_client:
                    video_url = upload_to_r2(r2_client, final_path, username)
                    if video_url:
                        landing_url = create_landing_page(r2_client, username, video_url)
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
                        continue
                else:
                    print(f"   -> render failed: {e}")
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
