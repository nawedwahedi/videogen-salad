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

def upload_to_r2(client, local_path, username):
    if client is None:
        return None
    try:
        key = f"lawap/{username}/video.mp4"
        client.upload_file(
            str(local_path),
            R2_BUCKET,
            key,
            ExtraArgs={'ContentType': 'video/mp4'}
        )
        return f"{R2_PUBLIC_URL}/lawap/{username}/video.mp4"
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
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
            background: #000;
            display: flex;
            justify-content: center;
            align-items: center;
            min-height: 100vh;
            padding: 20px;
        }}
        .container {{
            max-width: 1280px;
            width: 100%;
        }}
        video {{
            width: 100%;
            height: auto;
            border-radius: 8px;
            box-shadow: 0 10px 40px rgba(0,0,0,0.5);
        }}
        .info {{
            color: #fff;
            text-align: center;
            margin-top: 20px;
            font-size: 14px;
            opacity: 0.7;
        }}
    </style>
</head>
<body>
    <div class="container">
        <video controls autoplay muted loop playsinline>
            <source src="{video_url}" type="video/mp4">
        </video>
        <div class="info">@{username}</div>
    </div>
</body>
</html>"""
    try:
        key = f"lawap/{username}/index.html"
        client.put_object(
            Bucket=R2_BUCKET,
            Key=key,
            Body=html.encode('utf-8'),
            ContentType='text/html'
        )
        return f"{R2_PUBLIC_URL}/lawap/{username}/"
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
    if any(h in ("website url","url","website","link") for h in header):
        url_i = find_col(header, {"website url","url","website","link"})
        ig_i  = find_col(header, {"instagram username","username","instagram","handle"})
        data = raw[1:]
        for row in data:
            url = clean_url(row[url_i]) if url_i is not None and url_i < len(row) else ""
            ig  = (row[ig_i].strip() if ig_i is not None and ig_i < len(row) else "")
            if url: rows.append({"url":url,"username":ig})
    else:
        for row in raw:
            url = clean_url(row[0] if len(row)>0 else "")
            ig  = row[1].strip() if len(row)>1 else ""
            if url: rows.append({"url":url,"username":ig})
    
    if TOTAL_WORKERS > 1:
        rows = [r for i, r in enumerate(rows) if i % TOTAL_WORKERS == WORKER_ID]
        print(f"[WORKER {WORKER_ID}/{TOTAL_WORKERS}] Assigned {len(rows)} rows")
    
    return rows

# ================== FFmpeg helpers ==================
def has_ffmpeg():
    try:
        subprocess.run(["ffmpeg","-version"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, check=True)
        return True
    except Exception:
        return False

def ensure_overlay_optimized(src, cache_dir):
    if not DO_COMPRESS_OVERLAY or not has_ffmpeg():
        return src
    try:
        cache_dir.mkdir(parents=True, exist_ok=True)
        src = src.resolve()
        key = f"{src}|{src.stat().st_mtime}|{OVERLAY_TARGET_W}|{OVERLAY_V_KBPS}|{OVERLAY_A_KBPS}"
        slug = hashlib.sha1(key.encode("utf-8")).hexdigest()[:20]
        dst = cache_dir / f"overlay_{slug}_720p.mp4"
        if dst.exists(): return dst
        cmd = [
            "ffmpeg","-y","-i",str(src),
            "-vf", f"scale='min({OVERLAY_TARGET_W},iw)':-2",
            "-c:v","libx264","-preset","veryfast","-profile:v","main","-level","3.1",
            "-b:v",f"{OVERLAY_V_KBPS}k","-maxrate",f"{int(OVERLAY_V_KBPS*1.3)}k","-bufsize",f"{int(OVERLAY_V_KBPS*2)}k",
            "-c:a","aac","-b:a",f"{OVERLAY_A_KBPS}k","-movflags","+faststart",str(dst)
        ]
        subprocess.run(cmd, check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        return dst
    except Exception:
        return src

# ================== EASING ==================
def min_jerk(t):
    return t*t*t*(10 - 15*t + 6*t*t)

# ================== SLOW HUMAN SCROLL ==================
def build_slow_human_offsets(seg_frames, max_offset, fps):
    if max_offset <= 0:
        return np.zeros(seg_frames, dtype=np.int32)
    seg_frames = int(seg_frames * 1.6)
    f1, f2 = 0.65, 0.18
    f3 = 1.0 - f1 - f2
    n1 = max(1, int(seg_frames*f1))
    n2 = max(1, int(seg_frames*f2))
    n3 = max(1, seg_frames - n1 - n2)
    top, bottom = 0, max(0, max_offset - random.randint(120,260))
    middle = int(max_offset * (0.45 + random.uniform(-0.06, 0.06)))
    def ramp(A,B,N):
        t = np.linspace(0,1,N)
        return A + (B-A)*min_jerk(t)
    p1 = np.concatenate([ramp(top,bottom,n1), np.full(max(1,int(0.4*fps)), bottom)])
    p2 = np.concatenate([ramp(p1[-1],middle,n2), np.full(max(1,int(0.3*fps)), middle)])
    p3 = np.concatenate([ramp(p2[-1],top,n3), np.full(max(1,int(0.3*fps)), top)])
    path = np.concatenate([p1,p2,p3])
    if len(path) > 20:
        pause_count = max(2, len(path)//40)
        pause_idx = np.random.choice(len(path)-1, size=pause_count, replace=False)
        pause_idx.sort()
        inserts = 0
        for idx in pause_idx:
            idx += inserts
            hold_len = random.randint(int(0.2*fps), int(0.6*fps))
            hold = np.full(hold_len, path[idx])
            path = np.insert(path, idx, hold)
            inserts += hold_len
    jitter = np.random.normal(0,0.15,len(path))
    path = path + jitter
    return np.clip(path,0,max_offset).astype(np.int32)

def looped_offsets(total_frames,max_offset,fps,segment_sec):
    seg_frames = max(1, int(segment_sec*fps))
    seg = build_slow_human_offsets(seg_frames,max_offset,fps)
    seg = np.concatenate([seg, np.full(max(1,int(0.25*fps)), seg[-1], dtype=seg.dtype)])
    reps = int(np.ceil(total_frames/len(seg)))
    return np.tile(seg,reps)[:total_frames].astype(np.int32)

def build_scrolling_clip(tall_img, W,H,duration,fps,seg_sec):
    img = Image.open(tall_img).convert("RGB")
    iw,ih = img.size
    if iw != W:
        ih2 = int(round(ih*(W/iw)))
        img = img.resize((W,ih2), Image.LANCZOS)
    arr = np.array(img)
    max_off = max(arr.shape[0]-H,0)
    total_frames = max(1, int(round(duration*fps)))
    offsets = looped_offsets(total_frames,max_off,fps,seg_sec)
    def make_frame(t):
        idx = min(int(t*fps), total_frames-1)
        y = int(offsets[idx]) if max_off>0 else 0
        crop = arr[y:y+H, 0:W, :]
        if crop.shape[0]<H:
            pad = np.tile(crop[-1:,:,:], (H-crop.shape[0],1,1))
            crop = np.vstack([crop,pad])
        return crop
    return VideoClip(make_frame,duration=duration).set_fps(fps)

# ================== PLAYWRIGHT ==================
def capture_fullpage_png(page,url,out_png,w,h):
    try:
        page.set_viewport_size({"width":w,"height":h})
        page.set_extra_http_headers({"User-Agent":USER_AGENT})
        page.goto(url, wait_until="networkidle", timeout=45000)
        page.wait_for_timeout(1200)
        page.screenshot(path=str(out_png), full_page=True)
        return True
    except Exception:
        try:
            page.goto(url, wait_until="domcontentloaded", timeout=30000)
            page.wait_for_timeout(1400)
            page.screenshot(path=str(out_png), full_page=True)
            return True
        except Exception as e:
            print(f"   -> screenshot failed: {e}")
            return False

# ================== OUTPUT FILE SAFETY ==================
def unique_path(p):
    if not p.exists():
        return p
    stem, suf = p.stem, p.suffix
    for k in range(1, 1000):
        q = p.with_name(f"{stem}_{k}{suf}")
        if not q.exists():
            return q
    return p.with_name(f"{stem}_{int(time.time())}{suf}")

def write_video_atomic(clip, target, fps, audio_clip, logger):
    target = target.resolve()
    target.parent.mkdir(parents=True, exist_ok=True)
    tmp = target.with_name(target.stem + f".tmp_{int(time.time()*1000)}" + target.suffix)
    try:
        if tmp.exists():
            tmp.unlink()
    except Exception:
        pass
    
    audio_flag = audio_clip is not None
    
    # Check NVENC availability
    codec = "h264_nvenc"
    try:
        test_result = subprocess.run(
            ["ffmpeg", "-hide_banner", "-encoders"],
            capture_output=True,
            text=True,
            timeout=5
        )
        if "h264_nvenc" not in test_result.stdout:
            print("   [WARN] NVENC not available, using CPU")
            codec = "libx264"
    except Exception:
        codec = "libx264"
    
    try:
        if codec == "h264_nvenc":
            clip.write_videofile(
                str(tmp),
                fps=fps,
                codec="h264_nvenc",
                audio=audio_flag,
                audio_codec=("aac" if audio_flag else None),
                preset="fast",
                threads=max(1, (os.cpu_count() or 2) - 1),
                temp_audiofile=str(target.parent / (target.stem + "_tmp.m4a")),
                remove_temp=True,
                ffmpeg_params=[
                    "-preset", "p1",
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
                fps=fps,
                codec="libx264",
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
        csv_path = Path(os.getenv("CSV_PATH", "/input/leads.csv"))
        overlay_src = Path(os.getenv("OVERLAY_PATH", "/input/overlay.mp4"))
        outdir = Path(os.getenv("OUTPUT_PATH", "/output"))
        print(f"[HEADLESS MODE] Worker {WORKER_ID}/{TOTAL_WORKERS}")
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

    rows = load_rows(csv_path)
    if not rows:
        print("[ERROR] No valid rows in CSV.")
        return

    overlay_path = ensure_overlay_optimized(overlay_src, outdir/"_cache")
    r2_client = setup_r2_client()
    if r2_client:
        print("[INFO] R2 upload enabled")
    else:
        print("[INFO] R2 upload disabled")

    face_full = None
    try:
        face_full = VideoFileClip(str(overlay_path))
        overlay_duration = float(face_full.duration or SEGMENT_MIN_SEC)
    except Exception:
        print("[WARN] Could not open overlay")
        face_full = None
        overlay_duration = 30.0

    print(f"[INFO] {len(rows)} rows | {WIDTH}x{HEIGHT}@{FPS} | Overlay {overlay_duration:.1f}s")
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
            slug = safe_slug(username)
            shot = outdir/f"{slug}.png"
            outvid = outdir/f"{slug}.mp4"

            print(f"[{i}/{total}] {url}")

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

            seg_sec = random.uniform(SEGMENT_MIN_SEC, SEGMENT_MAX_SEC)
            video_start = time.time()
            scroll = None
            face_layer = None
            comp = None
            try:
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

            per_video = time.time() - video_start
            total_elapsed = time.time() - grand_start
            print(f"   -> saved {Path(final_path).name} | {per_video:.1f}s | ⏱ {timedelta(seconds=int(total_elapsed))}")

            result = {
                "Website URL": url,
                "Instagram Username": username,
                "Video Link": landing_url or Path(final_path).resolve().as_uri()
            }
            results.append(result)

        context.close()
        browser.close()

    try:
        if face_full: face_full.close()
    except: pass

    res_csv = outdir / f"RESULTS_worker{WORKER_ID}.csv"
    try:
        with open(res_csv, "w", encoding="utf-8", newline="") as f:
            w = csv.DictWriter(f, fieldnames=["Website URL","Instagram Username","Video Link"])
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
