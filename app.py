import subprocess
import os
import shutil
import logging
import threading
import json
import time
from datetime import datetime, timedelta
from collections import deque
from flask import Flask, render_template, jsonify, request, send_file
from flask_cors import CORS
import re
from dataclasses import dataclass, asdict
from typing import Optional, Dict, List
import csv
from io import StringIO

# --- Configuration ---
@dataclass
class Config:
    CHANNELS_FILE: str = "channels.txt"
    PROGRESS_FILE: str = "progress.json"
    ACTIVITY_LOG_FILE: str = "downloader_activity.log"
    SECRET_KEY: str = "your-very-secret-key-change-this"
    HOST: str = "0.0.0.0"
    PORT: int = 5000
    
    # Performance settings
    PROGRESS_SAVE_INTERVAL: int = 5  # seconds - frequent saves for real-time sync
    PROGRESS_SAVE_BATCH: int = 3  # videos - save after every 3 videos
    MAX_LOG_LINES: int = 100
    MAX_RECENT_TIMES: int = 10
    METRICS_CACHE_DURATION: int = 1  # seconds - real-time metrics
    
    # Download settings
    MAX_RETRIES: int = 3
    RETRY_DELAY: int = 5  # seconds
    RATE_LIMIT_DELAY: float = 0.5  # seconds between downloads
    
    # Subtitle settings
    SUB_LANGUAGES: List[str] = None
    
    def __post_init__(self):
        if self.SUB_LANGUAGES is None:
            self.SUB_LANGUAGES = ["en"]

config = Config()

# --- Flask App Setup ---
app = Flask(__name__)
app.config['SECRET_KEY'] = config.SECRET_KEY
CORS(app)  # Enable CORS for API access

# --- Global State ---
status_lock = threading.Lock()
app_status = {
    "running": False, "overall_status": "Idle", "channels": [], 
    "recent_download_times": deque(maxlen=config.MAX_RECENT_TIMES),
    "total_videos_to_process": 0, "total_videos_processed": 0, 
    "total_videos_remaining": 0, "total_errors": 0,
    "average_time_per_video": 0, "total_eta_seconds": 0, 
    "total_channels": 0, "channels_processed": 0,
    "current_phase": "Idle", "listing_progress": 0, 
    "downloading_progress": 0, "task_start_time": None, 
    "elapsed_seconds": 0, "total_videos_listed": 0, 
    "channels_listed": 0
}

# Process management per channel
channel_processes: Dict[str, subprocess.Popen] = {}
process_lock = threading.Lock()

# Metrics caching
metrics_cache = {"data": None, "timestamp": 0}
cache_lock = threading.Lock()

# Progress save batching
progress_dirty = False
progress_counter = 0
last_progress_save = time.time()

# --- Helper Functions ---
def get_name_from_url(url: str) -> str:
    """Provides a user-friendly default name by parsing the channel URL."""
    try:
        match = re.search(r'/(?:@|c/|user/|channel/)([^/]+)', url)
        if match:
            return match.group(1)
        return url
    except Exception:
        return url

def validate_youtube_url(url: str) -> bool:
    """Validate if the URL is a valid YouTube channel/playlist URL."""
    patterns = [
        r'youtube\.com/(?:@|c/|user/|channel/)',
        r'youtube\.com/playlist',
    ]
    return any(re.search(pattern, url) for pattern in patterns)

# --- Setup ---
def setup_logging():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=[
            logging.FileHandler(config.ACTIVITY_LOG_FILE, encoding="utf-8"),
            logging.StreamHandler()  # Also log to console
        ]
    )
    logging.getLogger('werkzeug').setLevel(logging.ERROR)

def check_dependencies():
    """Check if required dependencies are available."""
    deps = {
        "yt-dlp": shutil.which("yt-dlp"),
        "ffmpeg": shutil.which("ffmpeg")
    }
    return all(deps.values()), deps

def create_channels_file():
    """Create channels.txt file if it doesn't exist."""
    if not os.path.exists(config.CHANNELS_FILE):
        try:
            with open(config.CHANNELS_FILE, "w", encoding="utf-8") as f:
                f.write("# Add YouTube channel URLs here, one per line.\n")
                f.write("# Lines starting with # are comments and will be ignored.\n")
                f.write("# Example:\n")
                f.write("# https://www.youtube.com/@channelname\n")
            return True
        except IOError as e:
            logging.error(f"Could not create {config.CHANNELS_FILE}: {e}")
            return False
    return True

# --- Progress Management ---
def load_progress():
    """Load progress from disk with error handling."""
    global app_status
    with status_lock:
        if os.path.exists(config.PROGRESS_FILE):
            try:
                with open(config.PROGRESS_FILE, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    app_status["channels"] = data.get("channels", [])

                    # Don't restore recent_download_times - reset for current session only
                    # This ensures average time only reflects current session performance
                    app_status["recent_download_times"] = deque(maxlen=config.MAX_RECENT_TIMES)

                    # Cleanup: Reset any stuck "in-progress" videos from previous session
                    for channel in app_status["channels"]:
                        for video in channel.get("videos", []):
                            if video.get("status") == "in-progress":
                                video["status"] = "pending"
                                logging.info(f"Reset stuck in-progress video to pending: {video.get('title', 'Unknown')}")

                    logging.info(f"Loaded progress: {len(app_status['channels'])} channels")
            except (json.JSONDecodeError, IOError) as e:
                logging.error(f"Failed to load progress: {e}. Starting fresh.")
                app_status["channels"] = []
            except Exception as e:
                logging.error(f"Unexpected error loading progress: {e}. Starting fresh.")
                app_status["channels"] = []

    # Initial metrics calculation - OUTSIDE the lock to avoid deadlock
    try:
        calculate_metrics()
    except Exception as e:
        logging.error(f"Error calculating initial metrics: {e}")
        logging.error(f"Continuing anyway...")

def should_relist_channel(channel):
    """Check if a channel should be re-listed based on last listing time.
    Fixed to handle naive vs aware datetime comparison correctly.

    Returns True if:
    - Channel has never been listed
    - Channel was listed more than 7 days ago
    - Channel has no last_listed timestamp

    Returns False if:
    - Channel was listed within the last 7 days
    """
    last_listed = channel.get('last_listed')

    if not last_listed:
        return True

    try:
        # Clean the timestamp to ensure we have a naive string (remove Z or offset)
        # This ensures we interpret the time as "UTC time" but without the timezone object
        # so it is compatible with datetime.utcnow()
        clean_timestamp = last_listed.replace('Z', '').split('+')[0]

        last_listed_time = datetime.fromisoformat(clean_timestamp)

        # Calculate days difference
        # datetime.utcnow() is naive UTC; last_listed_time is now naive UTC
        days_since_listing = (datetime.utcnow() - last_listed_time).total_seconds() / 86400

        if days_since_listing < 7:
            logging.info(f"✓ Skipping listing for {channel['name']} - last listed {days_since_listing:.1f} days ago")
            return False
        else:
            logging.info(f"↻ Re-listing {channel['name']} - last listed {days_since_listing:.1f} days ago")
            return True

    except Exception as e:
        logging.warning(f"⚠ Error parsing last_listed timestamp for {channel['name']} ({last_listed}): {e}. Will re-list.")
        return True

def save_progress(force=False):
    """Save progress to disk with batching."""
    global progress_dirty, progress_counter, last_progress_save

    current_time = time.time()
    progress_counter += 1

    # Save if: forced, batch size reached, or time interval elapsed
    should_save = (
        force or
        progress_counter >= config.PROGRESS_SAVE_BATCH or
        (current_time - last_progress_save) >= config.PROGRESS_SAVE_INTERVAL
    )

    if not should_save:
        progress_dirty = True
        return

    with status_lock:
        try:
            # Create backup before saving
            if os.path.exists(config.PROGRESS_FILE):
                shutil.copy(config.PROGRESS_FILE, config.PROGRESS_FILE + ".bak")

            data = {
                "channels": app_status["channels"],
                "recent_download_times": list(app_status["recent_download_times"]),
                "last_saved": datetime.utcnow().isoformat(),
                "version": "2.0.0"  # Track schema version
            }

            with open(config.PROGRESS_FILE, 'w', encoding='utf-8') as f:
                json.dump(data, f, indent=2)

            progress_dirty = False
            progress_counter = 0
            last_progress_save = current_time

            logging.info(f"💾 Progress saved: {len(app_status['channels'])} channels")

        except IOError as e:
            logging.error(f"Failed to save progress: {e}")
        except Exception as e:
            logging.error(f"Unexpected error saving progress: {e}")

def calculate_metrics():
    """Calculate all metrics - OPTIMIZED VERSION.
    
    This should be called when data changes, not on every status poll.
    Uses incremental counting instead of nested loops.
    """
    with status_lock:
        channels = app_status.get("channels", [])
        app_status["total_channels"] = len(channels)
        
        if not channels:
            return

        # Update elapsed time
        if app_status.get("task_start_time"):
            try:
                # Clean timestamp to handle 'Z' suffix and timezone offsets
                clean_start = app_status["task_start_time"].replace('Z', '').split('+')[0]
                start_time = datetime.fromisoformat(clean_start)
                app_status["elapsed_seconds"] = (datetime.utcnow() - start_time).total_seconds()
            except Exception as e:
                logging.warning(f"Error calculating elapsed time: {e}")
                app_status["elapsed_seconds"] = 0

        # Calculate average time
        recent_times = app_status.get("recent_download_times", deque())
        avg_time = sum(recent_times) / len(recent_times) if recent_times else 0
        app_status["average_time_per_video"] = avg_time

        # Aggregate metrics
        metrics = {
            "channels_done": 0,
            "total_videos": 0,
            "videos_done": 0,
            "videos_remaining": 0,
            "total_errors": 0,
            "total_listed": 0,
            "channels_listed": 0
        }
        
        # Single pass through all channels and videos
        for channel in channels:
            videos = channel.get("videos", [])
            
            # Channel listing status
            if channel["status"] not in ["To Be Listed", "Listing"]:
                metrics["total_listed"] += len(videos)
                metrics["channels_listed"] += 1
            
            if not videos:
                continue
            
            # Count video statuses
            video_counts = {"downloaded": 0, "error": 0, "pending": 0, "in-progress": 0}
            for v in videos:
                status = v.get("status", "pending")
                video_counts[status] = video_counts.get(status, 0) + 1
            
            video_done = video_counts["downloaded"]
            video_errors = video_counts["error"]
            video_total = len(videos)
            videos_remaining = video_total - (video_done + video_errors)
            
            # Update channel metrics
            channel.update({
                "video_progress_count": video_done,
                "video_error_count": video_errors,
                "video_total_count": video_total,
                "video_remaining_count": videos_remaining,
                "progress": int(((video_done + video_errors) / video_total) * 100) if video_total > 0 else 0,
                "eta_seconds": videos_remaining * avg_time
            })
            
            # Check if channel is complete
            is_complete = videos_remaining == 0 and video_total > 0
            if is_complete and channel["status"] != "Done":
                channel["status"] = "Done"
            
            if channel["status"] == "Done":
                metrics["channels_done"] += 1
            
            # Aggregate totals
            metrics["total_videos"] += video_total
            metrics["videos_done"] += video_done
            metrics["videos_remaining"] += videos_remaining
            metrics["total_errors"] += video_errors
        
        # Update global status
        app_status.update({
            "channels_processed": metrics["channels_done"],
            "channels_listed": metrics["channels_listed"],
            "total_videos_listed": metrics["total_listed"],
            "total_videos_to_process": metrics["total_videos"],
            "total_videos_processed": metrics["videos_done"],
            "total_videos_remaining": metrics["videos_remaining"],
            "total_errors": metrics["total_errors"],
            "total_eta_seconds": metrics["videos_remaining"] * avg_time,
            "listing_progress": int((metrics["channels_listed"] / len(channels)) * 100) if len(channels) > 0 else 0,
            "downloading_progress": int((metrics["videos_done"] / metrics["total_videos"]) * 100) if metrics["total_videos"] > 0 else 0,
        })

def get_cached_metrics():
    """Get metrics from cache if fresh, otherwise recalculate."""
    global metrics_cache
    
    with cache_lock:
        current_time = time.time()
        
        # Return cached if still fresh
        if (metrics_cache["data"] is not None and 
            (current_time - metrics_cache["timestamp"]) < config.METRICS_CACHE_DURATION):
            return metrics_cache["data"]
        
        # Recalculate and cache
        calculate_metrics()
        
        with status_lock:
            # Create response optimized for frontend
            response = {
                "running": app_status["running"],
                "overall_status": app_status["overall_status"],
                "current_phase": app_status["current_phase"],
                "listing_progress": app_status["listing_progress"],
                "downloading_progress": app_status["downloading_progress"],
                "total_videos_to_process": app_status["total_videos_to_process"],
                "total_videos_processed": app_status["total_videos_processed"],
                "total_videos_remaining": app_status["total_videos_remaining"],
                "total_errors": app_status["total_errors"],
                "average_time_per_video": app_status["average_time_per_video"],
                "total_eta_seconds": app_status["total_eta_seconds"],
                "elapsed_seconds": app_status["elapsed_seconds"],
                "total_channels": app_status["total_channels"],
                "channels_processed": app_status["channels_processed"],
                "channels_listed": app_status["channels_listed"],
                "total_videos_listed": app_status["total_videos_listed"],
                "last_10_videos": [
                    {
                        "time_taken": t,
                        "title": f"Video {i+1}"  # TODO: Store actual titles
                    } 
                    for i, t in enumerate(app_status["recent_download_times"])
                ],
                "channels": app_status["channels"]
            }
        
        metrics_cache["data"] = response
        metrics_cache["timestamp"] = current_time
        
        return response

# --- Main Task ---
def run_downloader_task():
    """Main download orchestration with improved error handling and retry logic."""
    global app_status
    
    # Load channels
    try:
        with open(config.CHANNELS_FILE, 'r', encoding='utf-8') as f:
            channel_urls = sorted(list(set([
                line.strip() for line in f 
                if line.strip() and not line.startswith('#')
            ])))
        
        # Validate URLs
        invalid_urls = [url for url in channel_urls if not validate_youtube_url(url)]
        if invalid_urls:
            logging.warning(f"Invalid URLs found (will be skipped): {invalid_urls}")
            channel_urls = [url for url in channel_urls if validate_youtube_url(url)]
        
    except FileNotFoundError:
        logging.error(f"{config.CHANNELS_FILE} not found")
        return

    # Initialize task
    with status_lock:
        app_status.update({
            "running": True,
            "overall_status": "Initializing...",
            "current_phase": "Listing",
            "listing_progress": 0,
            "downloading_progress": 0,
            "task_start_time": datetime.utcnow().isoformat()
        })
        
        # Preserve existing progress
        existing_channels = {c["url"]: c for c in app_status["channels"]}
        app_status["channels"] = []
        
        for url in channel_urls:
            if url in existing_channels:
                # Preserve existing channel data
                channel = existing_channels[url]
                
                # Only reset status to "To Be Listed" if:
                # 1. Channel has never been listed (no last_listed timestamp)
                # 2. It's been more than 7 days since last listing
                if channel.get("status") == "To Be Listed" or should_relist_channel(channel):
                    channel["status"] = "To Be Listed"
                # Otherwise keep existing status (Listed, Downloading, Done, Error)
            else:
                # New channel
                channel = {
                    "name": get_name_from_url(url),
                    "url": url,
                    "status": "To Be Listed",
                    "videos": []
                }
            
            app_status["channels"].append(channel)
        
        # Reset error videos for retry - optimized to only process channels with errors
        for c in app_status["channels"]:
            if c.get("videos") and c.get("video_error_count", 0) > 0:
                for v in c["videos"]:
                    if v.get("status") == "error" and v.get("attempts", 0) < config.MAX_RETRIES:
                        v["status"] = "pending"

    # Save progress after initialization
    save_progress(force=True)

    # Phase 1: Listing
    logging.info("Starting Phase 1: Listing channels")

    # First, batch-process channels that don't need listing (instant handling)
    channels_to_list = []
    skipped_channels = []

    for channel in app_status["channels"]:
        if channel.get("status") == "To Be Listed" or should_relist_channel(channel):
            channels_to_list.append(channel)
        elif channel.get("status") in ["Listed", "Downloading", "Done"]:
            skipped_channels.append(channel)

    if skipped_channels:
        logging.info(f"📦 Loading {len(skipped_channels)} channels from cache (listed within 7 days):")
        for ch in skipped_channels:
            video_count = len(ch.get("videos", []))
            last_listed = ch.get("last_listed", "Unknown")
            logging.info(f"  ✓ {ch['name']}: {video_count} videos (cached from {last_listed})")

    if channels_to_list:
        logging.info(f"🔄 Need to list {len(channels_to_list)} channels (new or >7 days old)")

    logging.info(f"📊 Listing summary: {len(skipped_channels)} cached, {len(channels_to_list)} to list")

    # Update metrics once after batch processing skipped channels (fast path)
    if len(skipped_channels) > 0 and len(channels_to_list) == 0:
        # All channels skipped - update metrics and move to download phase immediately
        calculate_metrics()
        save_progress(force=True)
        with status_lock:
            app_status["current_phase"] = "Downloading"
            app_status["overall_status"] = "All channels already listed - starting downloads"
    elif len(channels_to_list) > 0:
        # Some channels need listing - update metrics once before starting
        calculate_metrics()
        save_progress(force=True)

    # Now process channels that need listing
    for channel in channels_to_list:
        if not app_status["running"]:
            break

        with status_lock:
            channel["status"] = "Listing"
            app_status["overall_status"] = f"Listing: {channel['name']}"

        calculate_metrics()
        threading.Thread(target=save_progress, daemon=True).start()

        try:
            command = ["yt-dlp", "--flat-playlist", "--dump-json", channel["url"]]
            logging.info(f"🔄 Listing channel: {channel['name']}")

            # Start timing
            start_time = time.time()

            proc = subprocess.run(
                command,
                capture_output=True,
                text=True,
                encoding='utf-8',
                check=True,
                timeout=300  # 5 minute timeout
            )

            videos_data = [
                json.loads(line)
                for line in proc.stdout.strip().split('\n')
                if line.strip()
            ]

            # Calculate elapsed time
            elapsed_time = time.time() - start_time

            with status_lock:
                if videos_data:
                    channel["name"] = (
                        videos_data[0].get("uploader") or
                        videos_data[0].get("channel") or
                        channel["name"]
                    )

                # Get existing video IDs to preserve progress
                existing_videos = {v["id"]: v for v in channel.get("videos", [])}

                # Update video list, preserving existing progress
                new_videos = []
                for v in videos_data:
                    if "id" not in v:
                        continue

                    video_id = v["id"]
                    if video_id in existing_videos:
                        # Keep existing video with its progress
                        existing_video = existing_videos[video_id]
                        # Update title in case it changed
                        existing_video["title"] = v.get("title", existing_video.get("title", "Unknown"))
                        new_videos.append(existing_video)
                    else:
                        # Add new video
                        new_videos.append({
                            "id": video_id,
                            "title": v.get("title", "Unknown"),
                            "url": f"https://www.youtube.com/watch?v={video_id}",
                            "status": "pending",
                            "attempts": 0
                        })

                channel["videos"] = new_videos
                channel["status"] = "Listed"
                channel["last_listed"] = datetime.utcnow().isoformat() + "Z"
                app_status["overall_status"] = f"Listed: {channel['name']} ({len(channel['videos'])} videos)"

            logging.info(f"✅ Listed {len(channel['videos'])} videos from {channel['name']} in {elapsed_time:.2f}s")
            logging.info(f"💾 Timestamp: {channel['last_listed']} (cached for 7 days)")

        except subprocess.TimeoutExpired:
            logging.error(f"Timeout listing channel {channel['name']}")
            with status_lock:
                channel["status"] = "Error"
        except Exception as e:
            logging.error(f"Failed to list channel {channel['name']}: {e}")
            with status_lock:
                channel["status"] = "Error"

        calculate_metrics()
        save_progress(force=True)  # Immediately save to progress.json after each channel

    # Phase 2: Downloading
    with status_lock:
        if app_status["running"]:
            app_status["current_phase"] = "Downloading"
            app_status["overall_status"] = "Starting downloads"

    # Force save before downloading phase
    save_progress(force=True)
    
    logging.info("Starting Phase 2: Downloading subtitles")
    
    for channel in app_status["channels"]:
        if not app_status["running"]:
            break
        
        if channel.get("status") not in ["Listed", "Downloading"]:
            continue
        
        with status_lock:
            channel["status"] = "Downloading"
        
        # Process each video in the channel
        while app_status["running"]:
            with status_lock:
                # Find next video to process (include in-progress from atomic transition)
                target_video = None
                target_idx = -1

                for idx, v in enumerate(channel.get("videos", [])):
                    if v.get("status") == "in-progress":
                        # Already marked by previous video's atomic transition
                        target_video = v
                        target_idx = idx
                        break
                    elif v.get("status") == "pending":
                        # Check retry limit
                        if v.get("attempts", 0) < config.MAX_RETRIES:
                            target_video = v
                            target_idx = idx
                            break
                    elif v.get("status") == "error":
                        # Check if we can retry
                        if v.get("attempts", 0) < config.MAX_RETRIES:
                            target_video = v
                            target_idx = idx
                            break

            if not target_video:
                with status_lock:
                    channel["status"] = "Done"
                logging.info(f"Completed channel: {channel['name']}")
                break

            # Find the next video to prepare for atomic transition
            # BUT only if no other video is already in-progress (prevents duplicates)
            next_video = None
            with status_lock:
                # First check: is there ANY in-progress video in the channel?
                has_in_progress = any(v.get("status") == "in-progress" for v in channel.get("videos", []))

                # Only find next video if current target will be the only in-progress video
                if not has_in_progress or target_video.get("status") == "in-progress":
                    for idx, v in enumerate(channel.get("videos", [])):
                        if idx > target_idx and v.get("status") in ["pending", "error"]:
                            if v.get("attempts", 0) < config.MAX_RETRIES:
                                next_video = v
                                break

            # Download video (will mark next as in-progress atomically when done)
            download_video(channel, target_video, target_idx, next_video)

            # Rate limiting
            time.sleep(config.RATE_LIMIT_DELAY)
        
        calculate_metrics()
        save_progress()
    
    # Finalize
    with status_lock:
        app_status.update({
            "running": False,
            "overall_status": "All tasks finished.",
            "current_phase": "Finished"
        })
    
    calculate_metrics()
    save_progress(force=True)
    logging.info("Download task completed")

def download_video(channel, video, video_idx, next_video=None):
    """Download a single video's subtitles with retry logic.

    Args:
        channel: The channel dict
        video: Current video to download
        video_idx: Index of current video
        next_video: Next video to process (for atomic transition)
    """
    # Only update status if not already in-progress (prevents double-increment from atomic transition)
    with status_lock:
        total_videos = len(channel.get("videos", []))
        video_num = video_idx + 1

        if video.get("status") != "in-progress":
            app_status["overall_status"] = f"Downloading [{video_num}/{total_videos}]: {video.get('title', '')[:40]}..."
            video["status"] = "in-progress"
            video["attempts"] = video.get("attempts", 0) + 1
            video["last_attempt"] = datetime.utcnow().isoformat() + "Z"
        else:
            # Already marked in-progress by previous video's atomic transition
            app_status["overall_status"] = f"Downloading [{video_num}/{total_videos}]: {video.get('title', '')[:40]}..."

    calculate_metrics()
    save_progress()
    
    # Prepare download directory
    safe_name = re.sub(r'[\\/*?:"<>|]', "", channel.get('name', channel.get('url')))
    os.makedirs(safe_name, exist_ok=True)
    
    # Build command
    command = [
        "yt-dlp",
        "-o", os.path.join(safe_name, "%(title)s [%(id)s].%(ext)s"),
        "--write-sub",
        "--write-auto-sub",
        "--sub-lang", ",".join(config.SUB_LANGUAGES),
        "--skip-download",
        "--convert-subs", "srt",
        "--no-warnings",
        video["url"]
    ]
    
    start_time = datetime.utcnow()
    process = None
    
    try:
        with process_lock:
            process = subprocess.Popen(
                command,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                encoding='utf-8',
                bufsize=1
            )
            channel_processes[video["url"]] = process
        
        # Read output
        if process.stdout:
            for line in iter(process.stdout.readline, ''):
                if not app_status["running"]:
                    process.terminate()
                    break
        
        process.wait()
        
        with status_lock:
            if process.returncode == 0:
                time_taken = (datetime.utcnow() - start_time).total_seconds()
                video["status"] = "downloaded"
                video["time_taken"] = time_taken
                app_status["recent_download_times"].append(time_taken)
                logging.info(f"Downloaded: {video['title']} ({time_taken:.2f}s)")
            else:
                video["status"] = "error"
                logging.error(f"Failed to download: {video['title']} (attempt {video['attempts']}/{config.MAX_RETRIES})")

            # Atomically mark next video as in-progress to prevent visual gap
            # Safety check: only if it's not already in-progress
            if next_video and next_video.get("status") != "in-progress":
                next_video["status"] = "in-progress"
                next_video["attempts"] = next_video.get("attempts", 0) + 1
                next_video["last_attempt"] = datetime.utcnow().isoformat() + "Z"

                # Find next video index for status display
                next_video_idx = channel.get("videos", []).index(next_video) if next_video in channel.get("videos", []) else -1
                if next_video_idx >= 0:
                    next_video_num = next_video_idx + 1
                    total_videos = len(channel.get("videos", []))
                    app_status["overall_status"] = f"Downloading [{next_video_num}/{total_videos}]: {next_video.get('title', '')[:40]}..."
                else:
                    app_status["overall_status"] = f"Downloading: {next_video.get('title', '')[:40]}..."

    except Exception as e:
        logging.error(f"Error processing video {video['title']}: {e}")
        with status_lock:
            video["status"] = "error"

            # Atomically mark next video as in-progress even on error
            # Safety check: only if it's not already in-progress
            if next_video and next_video.get("status") != "in-progress":
                next_video["status"] = "in-progress"
                next_video["attempts"] = next_video.get("attempts", 0) + 1
                next_video["last_attempt"] = datetime.utcnow().isoformat() + "Z"

                # Find next video index for status display
                next_video_idx = channel.get("videos", []).index(next_video) if next_video in channel.get("videos", []) else -1
                if next_video_idx >= 0:
                    next_video_num = next_video_idx + 1
                    total_videos = len(channel.get("videos", []))
                    app_status["overall_status"] = f"Downloading [{next_video_num}/{total_videos}]: {next_video.get('title', '')[:40]}..."
                else:
                    app_status["overall_status"] = f"Downloading: {next_video.get('title', '')[:40]}..."
    finally:
        with process_lock:
            if video["url"] in channel_processes:
                del channel_processes[video["url"]]
    
    calculate_metrics()
    save_progress()

# --- Routes ---
@app.route('/')
def index():
    return render_template('index.html')

@app.route('/start', methods=['POST'])
def start():
    with status_lock:
        if app_status["running"]:
            return jsonify({
                "status": "error",
                "message": "Task is already running."
            }), 400
        app_status["running"] = True
    
    threading.Thread(target=run_downloader_task, daemon=True).start()
    logging.info("Download task started")
    return jsonify({"status": "started"})

@app.route('/stop', methods=['POST'])
def stop():
    with status_lock:
        if not app_status["running"]:
            return jsonify({
                "status": "error",
                "message": "Task is not running."
            }), 400
        app_status["running"] = False
        app_status["overall_status"] = "Stopped by user."
    
    # Terminate all running processes
    with process_lock:
        for url, proc in list(channel_processes.items()):
            if proc.poll() is None:
                proc.terminate()
        channel_processes.clear()
    
    logging.info("Download task stopped by user")
    save_progress(force=True)
    return jsonify({"status": "stopped"})

@app.route('/status')
def status():
    """Optimized status endpoint with caching."""
    return jsonify(get_cached_metrics())

@app.route('/health')
def health():
    """Health check endpoint."""
    deps_ok, deps = check_dependencies()
    return jsonify({
        "status": "healthy" if deps_ok else "unhealthy",
        "dependencies": deps,
        "uptime": app_status.get("elapsed_seconds", 0),
        "version": "2.0.0"
    })

@app.route('/export/csv')
def export_csv():
    """Export progress as CSV."""
    output = StringIO()
    writer = csv.writer(output)
    
    # Header
    writer.writerow([
        "Channel", "Channel URL", "Video Title", "Video URL",
        "Status", "Attempts", "Time Taken"
    ])
    
    # Data
    with status_lock:
        for channel in app_status["channels"]:
            for video in channel.get("videos", []):
                writer.writerow([
                    channel["name"],
                    channel["url"],
                    video.get("title", ""),
                    video.get("url", ""),
                    video.get("status", ""),
                    video.get("attempts", 0),
                    video.get("time_taken", "")
                ])
    
    output.seek(0)
    return output.getvalue(), 200, {
        'Content-Type': 'text/csv',
        'Content-Disposition': 'attachment; filename=subtitle_download_progress.csv'
    }

@app.route('/export/json')
def export_json():
    """Export full progress as JSON."""
    with status_lock:
        data = {
            "exported_at": datetime.utcnow().isoformat(),
            "channels": app_status["channels"],
            "statistics": {
                "total_channels": app_status["total_channels"],
                "channels_processed": app_status["channels_processed"],
                "total_videos": app_status["total_videos_to_process"],
                "videos_processed": app_status["total_videos_processed"],
                "total_errors": app_status["total_errors"],
                "average_time": app_status["average_time_per_video"]
            }
        }
    
    return jsonify(data)

@app.route('/config', methods=['GET', 'POST'])
def configuration():
    """Get or update configuration."""
    if request.method == 'GET':
        return jsonify(asdict(config))
    
    elif request.method == 'POST':
        data = request.get_json()
        
        # Update config (add validation as needed)
        for key, value in data.items():
            if hasattr(config, key):
                setattr(config, key, value)
        
        return jsonify({"status": "updated", "config": asdict(config)})

# --- Main ---
if __name__ == "__main__":
    setup_logging()
    
    # Check dependencies
    deps_ok, deps = check_dependencies()
    if not deps_ok:
        logging.error("Missing dependencies:")
        for dep, path in deps.items():
            if not path:
                logging.error(f"  - {dep} not found in PATH")
        exit(1)
    
    # Create channels file
    if not create_channels_file():
        logging.error(f"Could not create {config.CHANNELS_FILE}")
        exit(1)
    
    # Load existing progress
    logging.info("Loading progress from disk...")
    load_progress()
    logging.info("Progress loaded successfully")

    logging.info(f"Starting Caption Downloader on {config.HOST}:{config.PORT}")
    logging.info(f"Configuration: {asdict(config)}")
    
    app.run(
        host=config.HOST,
        port=config.PORT,
        debug=False,
        use_reloader=False,
        threaded=True
    )