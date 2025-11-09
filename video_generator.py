#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os, csv, time, hashlib, subprocess, re, random, shutil
from pathlib import Path
from datetime import timedelta

import numpy as np
from PIL import Image
from moviepy.editor import VideoFileClip, CompositeVideoClip, VideoClip, concatenate_videoclips
from proglog import ProgressBarLogger
from playwright.sync_api import sync_playwright
import logging
import sys
import uuid as uuid_module
from typing import Optional, Dict, Any

# ================== LOGGING SETUP ==================
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(__name__)

# ================== ENVIRONMENT UTILITIES ==================
def uuid_to_worker_id(uuid_str: str) -> int:
    """
    Convert UUID string to stable integer worker ID.
    Uses hash of UUID to ensure consistent conversion across restarts.
    Returns value in range 0-9999 to avoid overflow issues.
    """
    try:
        # Parse UUID and hash it to get stable integer
        uuid_obj = uuid_module.UUID(uuid_str)
        # Use first 64 bits of UUID as integer, then mod to reasonable range
        worker_id = (uuid_obj.int >> 64) % 10000
        logger.info(f"Converted UUID {uuid_str} to worker ID {worker_id}")
        return worker_id
    except Exception as e:
        logger.warning(f"Failed to parse UUID '{uuid_str}': {e}. Using hash fallback.")
        # Fallback: hash the string itself
        return abs(hash(uuid_str)) % 10000

def get_worker_id() -> int:
    """
    Robustly determine worker ID from environment.
    Priority: WORKER_ID (if numeric) > SALAD_MACHINE_ID (convert UUID) > default 0
    """
    # Try WORKER_ID first
    worker_id_str = os.getenv("WORKER_ID", "").strip()
    if worker_id_str:
        try:
            worker_id = int(worker_id_str)
            logger.info(f"Using WORKER_ID from environment: {worker_id}")
            return worker_id
        except ValueError:
            logger.warning(f"WORKER_ID '{worker_id_str}' is not numeric, trying UUID conversion")
            try:
                return uuid_to_worker_id(worker_id_str)
            except Exception as e:
                logger.error(f"Failed to convert WORKER_ID to integer: {e}")

    # Try SALAD_MACHINE_ID
    salad_id = os.getenv("SALAD_MACHINE_ID", "").strip()
    if salad_id:
        logger.info(f"Using SALAD_MACHINE_ID: {salad_id}")
        return uuid_to_worker_id(salad_id)

    # Default to 0
    logger.info("No worker ID found, defaulting to 0")
    return 0

def get_total_workers() -> int:
    """Get total workers with fallback to 1"""
    try:
        total = int(os.getenv("TOTAL_WORKERS", "1"))
        if total < 1:
            logger.warning(f"Invalid TOTAL_WORKERS {total}, defaulting to 1")
            return 1
        return total
    except ValueError:
        logger.warning(f"Invalid TOTAL_WORKERS value, defaulting to 1")
        return 1

def detect_headless_mode() -> bool:
    """
    Reliably detect if running in headless/container mode.
    Checks multiple indicators to ensure correct detection.
    """
    # Check for explicit headless flag
    if os.getenv("HEADLESS", "").lower() in ("true", "1", "yes"):
        logger.info("Headless mode: HEADLESS env var set")
        return True

    # Check for SaladCloud environment
    if os.getenv("SALAD_MACHINE_ID"):
        logger.info("Headless mode: SALAD_MACHINE_ID detected")
        return True

    # Check for worker ID (indicates batch processing)
    if os.getenv("WORKER_ID"):
        logger.info("Headless mode: WORKER_ID detected")
        return True

    # Check for container indicators
    if os.path.exists("/.dockerenv"):
        logger.info("Headless mode: Docker container detected")
        return True

    # Check if running in Kubernetes
    if os.getenv("KUBERNETES_SERVICE_HOST"):
        logger.info("Headless mode: Kubernetes detected")
        return True

    # Check for common CI/CD environments
    if os.getenv("CI") or os.getenv("CONTINUOUS_INTEGRATION"):
        logger.info("Headless mode: CI environment detected")
        return True

    logger.info("Interactive mode: No container/worker indicators found")
    return False

def validate_env_var(name: str, default: str = "", required: bool = False) -> str:
    """Validate and retrieve environment variable with logging"""
    value = os.getenv(name, default).strip()
    if required and not value:
        logger.error(f"CRITICAL: Required environment variable {name} is not set!")
        raise ValueError(f"Required environment variable {name} is missing")
    if value:
        # Mask sensitive values in logs
        if any(secret in name.upper() for secret in ["KEY", "SECRET", "PASSWORD", "TOKEN"]):
            logger.info(f"Environment variable {name}: ***{value[-4:] if len(value) > 4 else '***'}")
        else:
            logger.info(f"Environment variable {name}: {value}")
    else:
        logger.info(f"Environment variable {name}: using default '{default}'")
    return value

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

# ================== CONFIGURATION ==================
logger.info("="*60)
logger.info("VIDEO GENERATOR STARTUP - Environment Configuration")
logger.info("="*60)

# Worker configuration with bulletproof handling
WORKER_ID = get_worker_id()
TOTAL_WORKERS = get_total_workers()
HEADLESS_MODE = detect_headless_mode()

logger.info(f"Worker Configuration: {WORKER_ID}/{TOTAL_WORKERS}")
logger.info(f"Mode: {'HEADLESS (Container/Batch)' if HEADLESS_MODE else 'INTERACTIVE (Desktop)'}")

# R2 Configuration with validation
R2_ENDPOINT = validate_env_var("R2_ENDPOINT", "")
R2_ACCESS_KEY = validate_env_var("R2_ACCESS_KEY", "")
R2_SECRET_KEY = validate_env_var("R2_SECRET_KEY", "")
R2_BUCKET = validate_env_var("R2_BUCKET", "")
R2_PUBLIC_URL = validate_env_var("R2_PUBLIC_URL", "")

# File Configuration
CSV_FILENAME = validate_env_var("CSV_FILENAME", "master.csv")
CALENDLY_URL = validate_env_var("CALENDLY_URL", "https://calendly.com/heedeestudios/seo-strategy-session")

logger.info("="*60)

USER_AGENT = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
              "AppleWebKit/537.36 (KHTML, like Gecko) "
              "Chrome/124.0.0.0 Safari/537.36")

# ================== QUIET LOGGER ==================
class SilentLogger(ProgressBarLogger):
    def bars_callback(self, *a, **k):
        pass

# ================== RETRY DECORATOR ==================
def retry_on_failure(max_attempts=3, delay=2, backoff=2, exceptions=(Exception,)):
    """
    Retry decorator with exponential backoff.
    Makes network operations bulletproof against transient failures.
    """
    def decorator(func):
        def wrapper(*args, **kwargs):
            attempt = 1
            current_delay = delay
            while attempt <= max_attempts:
                try:
                    return func(*args, **kwargs)
                except exceptions as e:
                    if attempt == max_attempts:
                        logger.error(f"{func.__name__} failed after {max_attempts} attempts: {e}")
                        raise
                    logger.warning(f"{func.__name__} attempt {attempt}/{max_attempts} failed: {e}. Retrying in {current_delay}s...")
                    time.sleep(current_delay)
                    current_delay *= backoff
                    attempt += 1
        return wrapper
    return decorator

# ================== STARTUP VALIDATION ==================
def validate_system_requirements():
    """
    Validate system has all required dependencies and tools.
    Fails fast with clear error messages if something is missing.
    """
    logger.info("Validating system requirements...")

    errors = []

    # Check ffmpeg
    try:
        result = subprocess.run(
            ["ffmpeg", "-version"],
            capture_output=True,
            text=True,
            timeout=5
        )
        if result.returncode == 0:
            logger.info("✓ ffmpeg found")
        else:
            errors.append("ffmpeg not working properly")
    except FileNotFoundError:
        errors.append("ffmpeg not found in PATH")
    except Exception as e:
        errors.append(f"ffmpeg check failed: {e}")

    # Check Python dependencies
    required_modules = [
        ("numpy", "numpy"),
        ("PIL", "Pillow"),
        ("moviepy", "moviepy"),
        ("playwright", "playwright"),
    ]

    for module_name, package_name in required_modules:
        try:
            __import__(module_name)
            logger.info(f"✓ {package_name} imported successfully")
        except ImportError:
            errors.append(f"Missing Python package: {package_name}")

    # Check R2 configuration in headless mode
    if HEADLESS_MODE:
        if not all([R2_ENDPOINT, R2_ACCESS_KEY, R2_SECRET_KEY, R2_BUCKET]):
            missing = []
            if not R2_ENDPOINT: missing.append("R2_ENDPOINT")
            if not R2_ACCESS_KEY: missing.append("R2_ACCESS_KEY")
            if not R2_SECRET_KEY: missing.append("R2_SECRET_KEY")
            if not R2_BUCKET: missing.append("R2_BUCKET")
            errors.append(f"Headless mode requires R2 config. Missing: {', '.join(missing)}")

    if errors:
        logger.error("System validation FAILED:")
        for error in errors:
            logger.error(f"  ✗ {error}")
        raise RuntimeError(f"System validation failed with {len(errors)} error(s)")

    logger.info("✓ All system requirements validated successfully")
    return True

# ================== R2 UPLOAD ==================
def setup_r2_client():
    """Setup R2 client with comprehensive error handling"""
    if not all([R2_ENDPOINT, R2_ACCESS_KEY, R2_SECRET_KEY, R2_BUCKET]):
        logger.warning("R2 configuration incomplete, upload disabled")
        return None

    try:
        import boto3
        from botocore.config import Config

        # Configure with retries and timeout
        config = Config(
            retries={'max_attempts': 3, 'mode': 'adaptive'},
            connect_timeout=10,
            read_timeout=30
        )

        client = boto3.client(
            's3',
            endpoint_url=R2_ENDPOINT,
            aws_access_key_id=R2_ACCESS_KEY,
            aws_secret_access_key=R2_SECRET_KEY,
            region_name='auto',
            config=config
        )

        # Test connection
        try:
            client.head_bucket(Bucket=R2_BUCKET)
            logger.info("✓ R2 connection validated successfully")
        except Exception as e:
            logger.warning(f"R2 bucket check failed (may not have head permissions): {e}")

        return client
    except ImportError:
        logger.error("boto3 not installed - R2 upload disabled")
        return None
    except Exception as e:
        logger.error(f"R2 setup failed: {e}")
        return None

@retry_on_failure(max_attempts=3, delay=2)
def _download_single_overlay(r2_client, bucket, overlay_filename, local_path):
    """Helper function to download a single overlay with retry logic"""
    r2_client.download_file(bucket, overlay_filename, local_path)

def download_overlays_from_r2(csv_path, r2_client, bucket):
    """Download all unique niche overlays from R2 with robust error handling"""
    logger.info(f"Reading CSV to find required overlays: {csv_path}")

    unique_niches = set()
    try:
        with open(csv_path, 'r', encoding='utf-8-sig') as f:
            reader = csv.DictReader(f)
            for row in reader:
                niche = row.get('Niche', '').strip()
                if niche:
                    unique_niches.add(niche)
    except Exception as e:
        logger.error(f"Failed to read CSV file: {e}")
        raise

    logger.info(f"Found {len(unique_niches)} unique niches: {sorted(list(unique_niches))}")

    overlays = {}
    failed_downloads = []

    for niche in unique_niches:
        overlay_filename = f"{niche}.mp4"
        local_path = f"/tmp/{overlay_filename}"

        try:
            logger.info(f"Downloading {overlay_filename} from R2...")
            _download_single_overlay(r2_client, bucket, overlay_filename, local_path)

            # Verify file was downloaded and is not empty
            if os.path.exists(local_path) and os.path.getsize(local_path) > 0:
                overlays[niche] = local_path
                logger.info(f"✓ Downloaded {overlay_filename} ({os.path.getsize(local_path)} bytes)")
            else:
                logger.error(f"Downloaded {overlay_filename} but file is empty or missing")
                overlays[niche] = None
                failed_downloads.append(niche)
        except Exception as e:
            logger.error(f"Failed to download {overlay_filename}: {e}")
            overlays[niche] = None
            failed_downloads.append(niche)

    if failed_downloads:
        logger.warning(f"Failed to download overlays for niches: {failed_downloads}")

    return overlays

@retry_on_failure(max_attempts=3, delay=2)
def upload_to_r2(client, local_path, username):
    """Upload video to R2 with retry logic and proper content type"""
    if client is None:
        logger.warning("R2 client is None, skipping upload")
        return None

    if not os.path.exists(local_path):
        logger.error(f"Local file does not exist: {local_path}")
        return None

    file_size = os.path.getsize(local_path)
    if file_size == 0:
        logger.error(f"Local file is empty: {local_path}")
        return None

    key = f"{username}/video.mp4"
    logger.info(f"Uploading video to R2: {key} ({file_size} bytes)")

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
    logger.info(f"✓ Video uploaded successfully: {url}")
    return url

@retry_on_failure(max_attempts=3, delay=2)
def upload_thumbnail_to_r2(client, thumbnail_path, username):
    """Upload high-quality thumbnail to R2 with retry logic"""
    if client is None:
        logger.warning("R2 client is None, skipping thumbnail upload")
        return None

    if not os.path.exists(thumbnail_path):
        logger.error(f"Thumbnail file does not exist: {thumbnail_path}")
        return None

    key = f"{username}/thumbnail.jpg"
    logger.info(f"Uploading thumbnail to R2: {key}")

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
    logger.info(f"✓ Thumbnail uploaded successfully: {url}")
    return url

def extract_thumbnail(video_path, thumbnail_path):
    """Extract high-quality thumbnail from video with error handling"""
    try:
        if not os.path.exists(video_path):
            logger.error(f"Video file does not exist: {video_path}")
            return False

        cmd = [
            "ffmpeg", "-y", "-i", str(video_path),
            "-ss", "00:00:05",
            "-vframes", "1",
            "-vf", "scale=1280:-1",
            "-q:v", "1",
            str(thumbnail_path)
        ]
        result = subprocess.run(cmd, capture_output=True, timeout=30)

        if result.returncode == 0 and os.path.exists(thumbnail_path):
            logger.info(f"✓ Thumbnail extracted: {thumbnail_path}")
            return True
        else:
            logger.error(f"Thumbnail extraction failed: {result.stderr.decode()[:200]}")
            return False
    except subprocess.TimeoutExpired:
        logger.error("Thumbnail extraction timed out after 30s")
        return False
    except Exception as e:
        logger.error(f"Thumbnail extraction failed: {e}")
        return False

@retry_on_failure(max_attempts=2, delay=1)
def create_landing_page(client, username, video_url, thumbnail_url):
    """✅ FIXED: Button moved up, better spacing"""
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
        logger.info(f"Uploading landing page to R2: {key_html}")
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
        logger.info(f"✓ Landing page uploaded: {final_url}")
        return final_url
    except Exception as e:
        logger.error(f"Landing page creation failed: {e}")
        return None

# ================== HELPER FUNCTIONS ==================
def check_nvenc():
    """Check for NVIDIA hardware encoding support"""
    try:
        result = subprocess.run(
            ["ffmpeg","-hide_banner","-encoders"],
            capture_output=True, text=True, timeout=5
        )
        if "h264_nvenc" in result.stdout:
            logger.info("✓ NVENC detected: h264_nvenc will be used for encoding")
            return True
        else:
            logger.info("NVENC not found, using libx264 software encoding")
            return False
    except Exception as e:
        logger.warning(f"ffmpeg encoder check failed: {e}")
        return False

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

def capture_fullpage_png(page, url, out_png, width, height, max_retries=3):
    """
    Capture full-page screenshot with robust retry logic.
    Increased retries to 3 for better reliability in production.
    """
    for attempt in range(max_retries):
        try:
            logger.debug(f"Screenshot attempt {attempt + 1}/{max_retries} for {url}")
            page.goto(url, wait_until="domcontentloaded", timeout=60000)

            try:
                page.wait_for_load_state("networkidle", timeout=10000)
                logger.debug("Page reached networkidle state")
            except Exception as e:
                logger.debug(f"Network not idle after 10s, continuing anyway: {e}")

            page.wait_for_timeout(2000)
            page.screenshot(path=str(out_png), full_page=True)

            # Verify screenshot was created successfully
            if os.path.exists(out_png) and os.path.getsize(out_png) > 0:
                logger.info(f"✓ Screenshot captured successfully ({os.path.getsize(out_png)} bytes)")
                return True
            else:
                logger.error("Screenshot file is empty or missing")
                return False

        except Exception as e:
            logger.warning(f"Screenshot attempt {attempt + 1}/{max_retries} failed: {e}")
            if attempt < max_retries - 1:
                wait_time = 3 * (attempt + 1)  # Progressive backoff
                logger.info(f"Retrying in {wait_time} seconds...")
                time.sleep(wait_time)
            else:
                logger.error(f"All {max_retries} screenshot attempts failed for {url}")
                return False
    return False

def build_smooth_natural_scroll(png_path, w, h, duration, fps):
    """✅ PERFECT: Smooth down → up → settle middle → stay static"""
    img = np.array(Image.open(png_path).convert("RGB"))
    img_h, img_w, _ = img.shape
    
    if img_h <= h:
        static = np.zeros((h, w, 3), dtype=np.uint8)
        static[:img_h, :min(img_w, w)] = img[:, :min(img_w, w)]
        return VideoClip(lambda t: static, duration=duration).set_fps(fps)
    
    scroll_dist = img_h - h
    
    # ✅ NEW SCROLL PATTERN: Down → Up → Middle → Stay
    keyframes = [
        (0.0, 0.0),      # Start at top
        (1.0, 0.15),     # Scroll down slowly
        (2.0, 0.35),     # Continue down
        (3.0, 0.60),     # Keep scrolling down
        (4.0, 0.75),     # Reach bottom area
        (5.0, 0.60),     # Scroll back up
        (6.0, 0.40),     # Continue up
        (7.0, 0.50),     # Settle in middle
        (7.5, 0.48),     # Small adjustment
        (8.0, 0.50),     # Final position (middle)
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
    """
    Main execution function with comprehensive error handling.
    Bulletproof for 500+ replica deployment.
    """
    try:
        # Validate system before starting
        validate_system_requirements()
    except Exception as e:
        logger.critical(f"System validation failed: {e}")
        logger.critical("Cannot proceed - fix the above issues and retry")
        return 1

    # Check hardware encoding
    has_nvenc = check_nvenc()

    logger.info("="*60)
    logger.info("STARTING VIDEO GENERATION")
    logger.info("="*60)

    if HEADLESS_MODE:
        csv_path = Path(f"/tmp/{CSV_FILENAME}")
        outdir = Path(os.getenv("OUTPUT_PATH", "/output"))

        logger.info(f"HEADLESS MODE - Worker {WORKER_ID}/{TOTAL_WORKERS}")
        logger.info(f"Calendly URL: {CALENDLY_URL}")
        logger.info(f"R2 Public URL: {R2_PUBLIC_URL}")
        logger.info(f"Output directory: {outdir}")

        r2_client = setup_r2_client()
        if not r2_client:
            logger.critical("R2 client not configured - required for headless mode")
            return 1

        try:
            logger.info(f"Downloading {CSV_FILENAME} from R2...")
            _download_single_overlay(r2_client, R2_BUCKET, CSV_FILENAME, str(csv_path))

            if not os.path.exists(csv_path) or os.path.getsize(csv_path) == 0:
                logger.critical(f"CSV download failed or empty: {csv_path}")
                return 1

            logger.info(f"✓ CSV downloaded successfully ({os.path.getsize(csv_path)} bytes)")
        except Exception as e:
            logger.critical(f"CSV download failed: {e}")
            return 1

        try:
            overlays = download_overlays_from_r2(csv_path, r2_client, R2_BUCKET)
        except Exception as e:
            logger.critical(f"Overlay download failed: {e}")
            return 1

    else:
        logger.info("INTERACTIVE MODE")
        logger.info("👉 Select your CSV")
        csv_path = pick_file("Select CSV",[("CSV","*.csv"),("All files","*.*")])
        if not csv_path:
            logger.error("No CSV selected")
            return 1

        logger.info("👉 Select your overlay video (mp4)")
        overlay_src = pick_file("Select overlay",[("MP4","*.mp4"),("All files","*.*")])
        if not overlay_src:
            logger.error("No overlay selected")
            return 1

        logger.info("👉 Pick output folder")
        outdir = pick_dir("Select output folder")
        if not outdir:
            logger.error("No output folder selected")
            return 1

        r2_client = setup_r2_client()
        overlays = {"default": overlay_src}

    # Load and validate rows
    try:
        rows = load_rows(csv_path)
        if not rows:
            logger.error("No valid rows in CSV")
            return 1
    except Exception as e:
        logger.critical(f"Failed to load CSV: {e}")
        return 1

    if r2_client:
        logger.info("✓ R2 upload enabled")
    else:
        logger.warning("R2 upload disabled (no credentials)")

    logger.info(f"Processing {len(rows)} rows | {WIDTH}x{HEIGHT}@{FPS}")
    logger.info(f"Video encoder: {'h264_nvenc (NVIDIA GPU)' if has_nvenc else 'libx264 (CPU)'}")

    try:
        outdir.mkdir(parents=True, exist_ok=True)
    except Exception as e:
        logger.critical(f"Failed to create output directory {outdir}: {e}")
        return 1

    silent = SilentLogger()
    grand_start = time.time()
    results = []
    successful = 0
    failed = 0

    try:
        with sync_playwright() as pw:
            logger.info("Launching browser...")
            browser = pw.chromium.launch(
                headless=True,
                args=["--disable-gpu", "--no-sandbox", "--disable-dev-shm-usage"]
            )
            context = browser.new_context(
                viewport={"width": WIDTH, "height": HEIGHT},
                user_agent=USER_AGENT,
                java_script_enabled=True,
                ignore_https_errors=True,
                device_scale_factor=1.0
            )
            page = context.new_page()
            logger.info("✓ Browser ready")

            total = len(rows)
            for i, r in enumerate(rows, 1):
                url = clean_url(r["url"])
                username = (r.get("username") or "").strip() or domain_from_url(url)
                niche = r.get("niche", "").strip()
                slug = safe_slug(username)
                shot = outdir / f"{slug}.png"
                outvid = outdir / f"{slug}.mp4"
                thumbnail_file = outdir / f"{slug}.jpg"

                logger.info("")
                logger.info(f"[{i}/{total}] Processing: {username}")
                logger.info(f"  URL: {url}")
                logger.info(f"  Niche: {niche}")

                if HEADLESS_MODE:
                    overlay_path = overlays.get(niche)
                    if not overlay_path:
                        logger.warning(f"Skipped - no overlay for niche: {niche}")
                        results.append({
                            "Website URL": url,
                            "Instagram Username": username,
                            "Niche": niche,
                            "Video Link": "FAILED - Missing overlay"
                        })
                        failed += 1
                        continue
                else:
                    overlay_path = overlays.get("default")

                # Capture screenshot
                try:
                    if not capture_fullpage_png(page, url, shot, WIDTH, HEIGHT):
                        logger.error("Skipped - screenshot failed after retries")
                        results.append({
                            "Website URL": url,
                            "Instagram Username": username,
                            "Niche": niche,
                            "Video Link": "FAILED - Screenshot failed"
                        })
                        failed += 1
                        continue
                except Exception as e:
                    logger.error(f"Screenshot exception: {e}")
                    results.append({
                        "Website URL": url,
                        "Instagram Username": username,
                        "Niche": niche,
                        "Video Link": f"FAILED - {str(e)[:100]}"
                    })
                    failed += 1
                    continue

                # Browser restart every 50 videos to prevent memory issues
                if i % 50 == 0:
                    logger.info("Restarting browser to free memory...")
                    try:
                        context.close()
                        browser.close()
                        browser = pw.chromium.launch(
                            headless=True,
                            args=["--disable-gpu", "--no-sandbox", "--disable-dev-shm-usage"]
                        )
                        context = browser.new_context(
                            viewport={"width": WIDTH, "height": HEIGHT},
                            user_agent=USER_AGENT,
                            java_script_enabled=True,
                            ignore_https_errors=True,
                            device_scale_factor=1.0
                        )
                        page = context.new_page()
                        logger.info("✓ Browser restarted")
                    except Exception as e:
                        logger.error(f"Browser restart failed: {e}")

                # Video rendering
                overlay_path_opt = ensure_overlay_optimized(Path(overlay_path), outdir/"_cache")

                video_start = time.time()
                scroll = None
                face_layer = None
                comp = None
                face_full = None
                final_path = None

                try:
                    logger.info("Rendering video...")
                    face_full = VideoFileClip(str(overlay_path_opt))
                    overlay_duration = float(face_full.duration or 30)

                    # Build scroll animation
                    scroll_10sec = build_smooth_natural_scroll(shot, WIDTH, HEIGHT, 10.0, FPS)

                    final_frame = scroll_10sec.get_frame(9.9)
                    static_duration = max(0, overlay_duration - 10)

                    if static_duration > 0:
                        static_clip = VideoClip(lambda t: final_frame, duration=static_duration).set_fps(FPS)
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

                        # Bottom-left positioning (safe from social media crops)
                        x = SCROLL_MARGIN + dx
                        y = HEIGHT - scaled_h - SCROLL_MARGIN + dy
                        x = max(SCROLL_MARGIN, min(WIDTH - face_w - SCROLL_MARGIN, x))

                        face_layer = face_full.resize(width=face_w).set_position((x, y)).subclip(0, overlay_duration)
                        layers.append(face_layer)

                    comp = CompositeVideoClip(layers, size=(WIDTH, HEIGHT)).set_duration(overlay_duration)
                    if face_full is not None and face_full.audio is not None:
                        comp = comp.set_audio(face_full.audio.subclip(0, overlay_duration))

                    final_path = write_video_atomic(comp, outvid, FPS, (face_full.audio if face_full else None), silent)
                    logger.info(f"✓ Video rendered: {outvid.name}")

                    # Extract and upload thumbnail
                    thumbnail_url = None
                    try:
                        if extract_thumbnail(final_path, thumbnail_file):
                            if r2_client:
                                thumbnail_url = upload_thumbnail_to_r2(r2_client, thumbnail_file, username)
                    except Exception as e:
                        logger.warning(f"Thumbnail processing failed (non-critical): {e}")

                    # Upload video and create landing page
                    video_url = None
                    landing_url = None
                    if r2_client:
                        try:
                            video_url = upload_to_r2(r2_client, final_path, username)
                            if video_url:
                                landing_url = create_landing_page(r2_client, username, video_url, thumbnail_url)
                                if landing_url:
                                    logger.info(f"✓ Landing page: {landing_url}")
                        except Exception as e:
                            logger.error(f"Upload failed (continuing): {e}")

                    successful += 1

                except Exception as e:
                    msg = str(e)
                    logger.error(f"Video rendering failed: {msg[:200]}")

                    # Try alternative path if permission denied
                    if "Permission denied" in msg or "permission denied" in msg:
                        try:
                            alt = unique_path(outvid)
                            logger.warning(f"Target locked, writing to {alt.name} instead")
                            final_path = write_video_atomic(comp, alt, FPS, (face_full.audio if face_full else None), silent)
                            successful += 1
                        except Exception as e2:
                            logger.error(f"Alternative path failed: {e2}")
                            results.append({
                                "Website URL": url,
                                "Instagram Username": username,
                                "Niche": niche,
                                "Video Link": f"FAILED - {str(e2)[:100]}"
                            })
                            failed += 1
                            continue
                    else:
                        results.append({
                            "Website URL": url,
                            "Instagram Username": username,
                            "Niche": niche,
                            "Video Link": f"FAILED - {str(e)[:100]}"
                        })
                        failed += 1
                        continue
                finally:
                    # Cleanup resources
                    for obj in [comp, face_layer, scroll, face_full]:
                        try:
                            if obj:
                                obj.close()
                        except:
                            pass

                per_video = time.time() - video_start
                total_elapsed = time.time() - grand_start
                avg_per_video = total_elapsed / i
                eta_seconds = avg_per_video * (total - i)

                logger.info(f"✓ Saved: {Path(final_path).name if final_path else 'unknown'}")
                logger.info(f"  Time: {per_video:.1f}s | Total: {timedelta(seconds=int(total_elapsed))} | ETA: {timedelta(seconds=int(eta_seconds))}")
                logger.info(f"  Success: {successful} | Failed: {failed}")

                result = {
                    "Website URL": url,
                    "Instagram Username": username,
                    "Niche": niche,
                    "Video Link": landing_url or (Path(final_path).resolve().as_uri() if final_path else "FAILED")
                }
                results.append(result)

            logger.info("Closing browser...")
            context.close()
            browser.close()

    except KeyboardInterrupt:
        logger.warning("Process interrupted by user")
        return 130
    except Exception as e:
        logger.critical(f"Fatal error in main loop: {e}", exc_info=True)
        return 1

    # Write results CSV
    res_csv = outdir / f"RESULTS_worker{WORKER_ID}.csv"
    try:
        with open(res_csv, "w", encoding="utf-8", newline="") as f:
            w = csv.DictWriter(f, fieldnames=["Website URL","Instagram Username","Niche","Video Link"])
            w.writeheader()
            w.writerows(results)
        logger.info(f"✓ Results saved to: {res_csv}")
    except Exception as e:
        logger.error(f"Could not write results CSV: {e}")

    # Final summary
    total_time = time.time() - grand_start
    logger.info("="*60)
    logger.info("GENERATION COMPLETE")
    logger.info("="*60)
    logger.info(f"Total videos: {len(results)}/{len(rows)}")
    logger.info(f"Successful: {successful}")
    logger.info(f"Failed: {failed}")
    logger.info(f"Success rate: {(successful/len(rows)*100):.1f}%")
    logger.info(f"Total time: {timedelta(seconds=int(total_time))}")
    if successful > 0:
        logger.info(f"Average time per video: {total_time/successful:.1f}s")
    logger.info(f"Results CSV: {res_csv}")
    logger.info("="*60)

    return 0 if failed == 0 else 2

if __name__=="__main__":
    try:
        exit_code = main()
        sys.exit(exit_code if exit_code is not None else 0)
    except KeyboardInterrupt:
        logger.warning("\n[ABORTED BY USER]")
        sys.exit(130)
    except Exception as e:
        logger.critical(f"Unhandled exception: {e}", exc_info=True)
        sys.exit(1)
