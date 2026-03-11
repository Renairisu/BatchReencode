#!/usr/bin/env python3
import argparse
import os
import json
import queue
import re
import subprocess
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Optional

from flask import Flask, jsonify, render_template_string, request, send_file

APP = Flask(__name__)

DEFAULT_FOLDER = Path("/media/jellyfin/anime")
VIDEO_EXTENSIONS = {".mkv", ".mp4", ".avi", ".mov", ".m4v", ".webm"}
ENCODED_TAG = "encoded_by=nvenc_batch_script"
APP_DIR = Path(__file__).resolve().parent
CONFIG_PATH = APP_DIR / "reencoder_config.json"
ALLOWED_VIDEO_ENCODERS = [
    "hevc_nvenc",
    "h264_nvenc",
    "libx265",
    "libx264",
]
ALLOWED_PRESETS = ["p1", "p2", "p3", "p4", "p5", "p6", "p7"]
ALLOWED_AUDIO_CODECS = ["aac", "copy", "libopus", "libmp3lame"]
DEFAULT_SCAN_WORKERS = max(2, min(32, os.cpu_count() or 4))
DEFAULT_PARALLEL_ENCODES = max(1, min(8, os.cpu_count() or 4))

config_lock = threading.Lock()
state_lock = threading.Lock()

config = {
    "folder": str(DEFAULT_FOLDER),
    "video_encoder": "hevc_nvenc",
    "preset": "p7",
    "cq": "23",
    "audio_codec": "aac",
    "audio_bitrate": "192k",
    "scan_workers": DEFAULT_SCAN_WORKERS,
    "parallel_encodes": DEFAULT_PARALLEL_ENCODES,
    "replace_original": True,
    "respect_skip_rules": True,
}


def _normalize_bool(value) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes", "on"}
    return bool(value)


def _normalize_int(value, default: int, min_value: int, max_value: int) -> int:
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        return default
    return max(min_value, min(max_value, parsed))


def save_config_to_disk() -> None:
    with config_lock:
        payload = dict(config)
    CONFIG_PATH.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def load_config_from_disk() -> None:
    if not CONFIG_PATH.exists():
        return
    try:
        payload = json.loads(CONFIG_PATH.read_text(encoding="utf-8"))
    except Exception:
        return

    if not isinstance(payload, dict):
        return

    with config_lock:
        folder = payload.get("folder")
        if isinstance(folder, str) and folder.strip():
            folder_path = Path(folder).expanduser().resolve()
            if folder_path.exists() and folder_path.is_dir():
                config["folder"] = str(folder_path)

        video_encoder = str(payload.get("video_encoder", config["video_encoder"]))
        if video_encoder in ALLOWED_VIDEO_ENCODERS:
            config["video_encoder"] = video_encoder

        preset = str(payload.get("preset", config["preset"]))
        if preset in ALLOWED_PRESETS:
            config["preset"] = preset

        audio_codec = str(payload.get("audio_codec", config["audio_codec"]))
        if audio_codec in ALLOWED_AUDIO_CODECS:
            config["audio_codec"] = audio_codec

        cq = payload.get("cq")
        if cq is not None and str(cq).strip():
            config["cq"] = str(cq).strip()

        audio_bitrate = payload.get("audio_bitrate")
        if audio_bitrate is not None and str(audio_bitrate).strip():
            config["audio_bitrate"] = str(audio_bitrate).strip()

        if "scan_workers" in payload:
            config["scan_workers"] = _normalize_int(payload["scan_workers"], DEFAULT_SCAN_WORKERS, 1, 64)
        if "parallel_encodes" in payload:
            config["parallel_encodes"] = _normalize_int(payload["parallel_encodes"], DEFAULT_PARALLEL_ENCODES, 1, 32)

        if "replace_original" in payload:
            config["replace_original"] = _normalize_bool(payload["replace_original"])
        if "respect_skip_rules" in payload:
            config["respect_skip_rules"] = _normalize_bool(payload["respect_skip_rules"])


@dataclass
class EncodeJob:
    id: int
    rel_path: str
    abs_path: str
    status: str = "queued"
    progress: float = 0.0
    message: str = "Queued"
    created_at: float = field(default_factory=time.time)
    started_at: Optional[float] = None
    ended_at: Optional[float] = None
    output_path: Optional[str] = None
    output_size_mb: Optional[float] = None
    input_size_mb: Optional[float] = None
    replace_original: bool = True
    respect_skip_rules: bool = True
    ffmpeg_cmd: list[str] = field(default_factory=list)


job_queue: queue.Queue[int] = queue.Queue()
jobs: dict[int, EncodeJob] = {}
next_job_id = 1

running_event = threading.Event()
shutdown_event = threading.Event()
running_processes: dict[int, subprocess.Popen] = {}
stop_requested_jobs: set[int] = set()

video_scan_lock = threading.Lock()
video_scan_state = {
    "in_progress": False,
    "processed": 0,
    "total": 0,
    "last_error": "",
    "last_scan_at": None,
}
video_cache: list[dict] = []


def run_ffprobe(args: list[str]) -> str:
    cmd = ["ffprobe", "-v", "error"] + args
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
    except subprocess.TimeoutExpired:
        return ""
    if result.returncode != 0:
        return ""
    return result.stdout.strip()


def run_ffprobe_json(args: list[str]) -> dict:
    output = run_ffprobe(args + ["-of", "json"])
    if not output:
        return {}
    try:
        parsed = json.loads(output)
    except json.JSONDecodeError:
        return {}
    return parsed if isinstance(parsed, dict) else {}


def is_within_root(target: Path, root: Path) -> bool:
    try:
        target.resolve().relative_to(root.resolve())
        return True
    except ValueError:
        return False


def get_duration(file_path: Path) -> float:
    output = run_ffprobe([
        "-select_streams", "v:0",
        "-show_entries", "format=duration",
        "-of", "default=noprint_wrappers=1:nokey=1",
        str(file_path),
    ])
    try:
        return float(output)
    except ValueError:
        return 0.0


def get_video_metadata(file_path: Path) -> dict:
    data = run_ffprobe_json([
        "-select_streams", "v:0",
        "-show_entries", "stream=codec_name:format=duration:format_tags=encoded_by",
        str(file_path),
    ])

    codec = ""
    streams = data.get("streams")
    if isinstance(streams, list) and streams:
        stream0 = streams[0]
        if isinstance(stream0, dict):
            codec = str(stream0.get("codec_name", "") or "")

    duration = 0.0
    fmt = data.get("format")
    if isinstance(fmt, dict):
        raw_duration = fmt.get("duration")
        try:
            duration = float(raw_duration)
        except (TypeError, ValueError):
            duration = 0.0

    tagged = False
    if isinstance(fmt, dict):
        tags = fmt.get("tags")
        if isinstance(tags, dict):
            tagged = str(tags.get("encoded_by", "")) == "nvenc_batch_script"

    return {
        "codec": codec,
        "duration": duration,
        "tagged": tagged,
    }


def get_video_codec(file_path: Path) -> str:
    return run_ffprobe([
        "-select_streams", "v:0",
        "-show_entries", "stream=codec_name",
        "-of", "default=noprint_wrappers=1:nokey=1",
        str(file_path),
    ])


def has_custom_tag(file_path: Path) -> bool:
    tags = run_ffprobe([
        "-show_entries", "format_tags",
        "-of", "default=noprint_wrappers=1",
        str(file_path),
    ])
    return ENCODED_TAG in tags


def parse_ffmpeg_time(line: str) -> Optional[float]:
    match = re.search(r"time=(\d+):(\d+):(\d+(?:\.\d+)?)", line)
    if not match:
        return None
    hours = int(match.group(1))
    minutes = int(match.group(2))
    seconds = float(match.group(3))
    return hours * 3600 + minutes * 60 + seconds


def normalize_and_validate_path(rel_path: str) -> Optional[Path]:
    with config_lock:
        root = Path(config["folder"]).resolve()
    target = (root / rel_path).resolve()
    if not is_within_root(target, root):
        return None
    if not target.exists() or not target.is_file():
        return None
    return target


def get_skip_reason(file_path: Path) -> Optional[str]:
    metadata = get_video_metadata(file_path)
    if metadata["tagged"]:
        return "Already tagged by this script"
    codec = metadata["codec"]
    if codec == "hevc":
        return "Already HEVC"
    duration = metadata["duration"]
    file_size_mb = file_path.stat().st_size / (1024 * 1024)
    if duration > 20 * 60 and file_size_mb < 400:
        return "Long (>20m) but already small (<400MB)"
    return None


def collect_video_info(file_path: Path, root: Path) -> dict:
    metadata = get_video_metadata(file_path)
    duration = metadata["duration"]
    size_mb = file_path.stat().st_size / (1024 * 1024)
    codec = metadata["codec"]
    tagged = metadata["tagged"]
    skip_reason = None
    if tagged:
        skip_reason = "Already tagged by this script"
    elif codec == "hevc":
        skip_reason = "Already HEVC"
    elif duration > 20 * 60 and size_mb < 400:
        skip_reason = "Long (>20m) but already small (<400MB)"

    return {
        "rel_path": str(file_path.relative_to(root)),
        "name": file_path.name,
        "codec": codec,
        "duration_seconds": round(duration, 2),
        "size_mb": round(size_mb, 2),
        "tagged": tagged,
        "skip_reason": skip_reason,
    }


def scan_videos() -> list[dict]:
    with config_lock:
        root = Path(config["folder"]).resolve()
    if not root.exists():
        return []

    result = []
    for path in root.rglob("*"):
        if path.is_file() and path.suffix.lower() in VIDEO_EXTENSIONS:
            try:
                result.append(collect_video_info(path, root))
            except Exception as exc:
                result.append({
                    "rel_path": str(path.relative_to(root)),
                    "name": path.name,
                    "codec": "unknown",
                    "duration_seconds": 0.0,
                    "size_mb": round(path.stat().st_size / (1024 * 1024), 2),
                    "tagged": False,
                    "skip_reason": f"Metadata read error: {exc}",
                })

    result.sort(key=lambda item: item["rel_path"].lower())
    return result


def get_video_scan_snapshot() -> dict:
    with video_scan_lock:
        return {
            "in_progress": bool(video_scan_state["in_progress"]),
            "processed": int(video_scan_state["processed"]),
            "total": int(video_scan_state["total"]),
            "last_error": str(video_scan_state["last_error"]),
            "last_scan_at": video_scan_state["last_scan_at"],
        }


def _run_video_scan_worker() -> None:
    global video_cache
    with config_lock:
        root = Path(config["folder"]).resolve()
        scan_workers = _normalize_int(config.get("scan_workers"), DEFAULT_SCAN_WORKERS, 1, 64)

    if not root.exists() or not root.is_dir():
        with video_scan_lock:
            video_scan_state["in_progress"] = False
            video_scan_state["processed"] = 0
            video_scan_state["total"] = 0
            video_scan_state["last_error"] = f"Folder not found: {root}"
            video_scan_state["last_scan_at"] = time.time()
        return

    paths = [
        path for path in root.rglob("*")
        if path.is_file() and path.suffix.lower() in VIDEO_EXTENSIONS
    ]

    with video_scan_lock:
        video_scan_state["total"] = len(paths)
        video_scan_state["processed"] = 0
        video_scan_state["last_error"] = ""

    result: list[dict] = []

    def collect_safe(path: Path) -> dict:
        try:
            return collect_video_info(path, root)
        except Exception as exc:
            return {
                "rel_path": str(path.relative_to(root)),
                "name": path.name,
                "codec": "unknown",
                "duration_seconds": 0.0,
                "size_mb": round(path.stat().st_size / (1024 * 1024), 2),
                "tagged": False,
                "skip_reason": f"Metadata read error: {exc}",
            }

    worker_count = min(scan_workers, max(1, len(paths)))
    with ThreadPoolExecutor(max_workers=worker_count) as executor:
        futures = [executor.submit(collect_safe, path) for path in paths]
        for index, future in enumerate(as_completed(futures), start=1):
            result.append(future.result())
            if index % 5 == 0 or index == len(paths):
                with video_scan_lock:
                    video_scan_state["processed"] = index

    result.sort(key=lambda item: item["rel_path"].lower())
    with video_scan_lock:
        video_cache = result
        video_scan_state["in_progress"] = False
        video_scan_state["processed"] = len(paths)
        video_scan_state["total"] = len(paths)
        video_scan_state["last_scan_at"] = time.time()


def start_video_scan(force: bool = False) -> bool:
    with video_scan_lock:
        if video_scan_state["in_progress"]:
            return False
        if (not force) and video_cache:
            return False

        video_scan_state["in_progress"] = True
        video_scan_state["processed"] = 0
        video_scan_state["total"] = 0
        video_scan_state["last_error"] = ""

    thread = threading.Thread(target=_run_video_scan_worker, daemon=True)
    thread.start()
    return True


def _set_job_state(job_id: int, **kwargs) -> None:
    with state_lock:
        job = jobs.get(job_id)
        if not job:
            return
        for key, value in kwargs.items():
            setattr(job, key, value)


def _build_output_path(input_path: Path) -> Path:
    return input_path.with_name(f"{input_path.stem}_reencoded{input_path.suffix}")


def get_parallel_encode_limit() -> int:
    with config_lock:
        return _normalize_int(config.get("parallel_encodes"), DEFAULT_PARALLEL_ENCODES, 1, 32)


def run_encode_job(job_id: int) -> None:
    with state_lock:
        job = jobs[job_id]

    input_path = Path(job.abs_path)
    if not input_path.exists():
        _set_job_state(
            job_id,
            status="failed",
            message="Input file no longer exists",
            ended_at=time.time(),
        )
        return

    if job.respect_skip_rules:
        reason = get_skip_reason(input_path)
        if reason:
            _set_job_state(
                job_id,
                status="skipped",
                message=f"Skipped: {reason}",
                progress=100.0,
                started_at=time.time(),
                ended_at=time.time(),
            )
            return

    total_duration = get_duration(input_path)
    output_path = _build_output_path(input_path)

    with config_lock:
        current_cfg = dict(config)

    ffmpeg_cmd = [
        "ffmpeg",
        "-y",
        "-i",
        str(input_path),
        "-c:v",
        current_cfg["video_encoder"],
        "-preset",
        current_cfg["preset"],
        "-cq",
        str(current_cfg["cq"]),
        "-c:a",
        current_cfg["audio_codec"],
        "-b:a",
        current_cfg["audio_bitrate"],
        "-metadata",
        ENCODED_TAG,
        str(output_path),
    ]

    _set_job_state(
        job_id,
        status="running",
        message="Encoding in progress",
        started_at=time.time(),
        ffmpeg_cmd=ffmpeg_cmd,
    )

    process = subprocess.Popen(
        ffmpeg_cmd,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.PIPE,
        text=True,
        bufsize=1,
    )

    with state_lock:
        running_processes[job_id] = process
        stop_requested_jobs.discard(job_id)

    if process.stderr:
        for line in process.stderr:
            parsed_time = parse_ffmpeg_time(line)
            if parsed_time is not None and total_duration > 0:
                progress = min(99.0, (parsed_time / total_duration) * 100)
                _set_job_state(job_id, progress=round(progress, 2), message="Encoding in progress")

    return_code = process.wait()

    with state_lock:
        was_stopped = job_id in stop_requested_jobs
        stop_requested_jobs.discard(job_id)
        running_processes.pop(job_id, None)

    if was_stopped:
        if output_path.exists():
            output_path.unlink(missing_ok=True)
        _set_job_state(
            job_id,
            status="stopped",
            message="Stopped by user",
            ended_at=time.time(),
        )
        return

    if return_code != 0:
        if output_path.exists():
            output_path.unlink(missing_ok=True)
        _set_job_state(
            job_id,
            status="failed",
            message=f"ffmpeg failed (exit code {return_code})",
            ended_at=time.time(),
        )
        return

    input_size_mb = input_path.stat().st_size / (1024 * 1024)
    output_size_mb = output_path.stat().st_size / (1024 * 1024)

    if job.replace_original and output_size_mb < input_size_mb:
        input_path.unlink()
        output_path.rename(input_path)
        final_output_path = input_path
        message = "Replaced original (smaller output)"
    elif job.replace_original and output_size_mb >= input_size_mb:
        output_path.unlink(missing_ok=True)
        final_output_path = input_path
        message = "Output not smaller, original kept"
    else:
        final_output_path = output_path
        message = "Encoding finished"

    _set_job_state(
        job_id,
        status="completed",
        progress=100.0,
        message=message,
        ended_at=time.time(),
        output_path=str(final_output_path),
        input_size_mb=round(input_size_mb, 2),
        output_size_mb=round(output_size_mb, 2),
    )


def worker_loop() -> None:
    active_futures: dict = {}
    max_executor_workers = max(4, min(64, os.cpu_count() or 8))

    def _collect_done_futures() -> None:
        done = [future for future in list(active_futures.keys()) if future.done()]
        for future in done:
            job_id = active_futures.pop(future)
            try:
                future.result()
            except Exception as exc:
                _set_job_state(job_id, status="failed", message=f"Unhandled error: {exc}", ended_at=time.time())

    with ThreadPoolExecutor(max_workers=max_executor_workers) as executor:
        while not shutdown_event.is_set():
            _collect_done_futures()

            if not running_event.is_set():
                time.sleep(0.2)
                continue

            target_parallel = get_parallel_encode_limit()

            while len(active_futures) < target_parallel:
                try:
                    job_id = job_queue.get_nowait()
                except queue.Empty:
                    break

                with state_lock:
                    job = jobs.get(job_id)
                    if not job or job.status != "queued":
                        continue

                future = executor.submit(run_encode_job, job_id)
                active_futures[future] = job_id

            if not active_futures:
                time.sleep(0.2)
            else:
                time.sleep(0.1)

        _collect_done_futures()


def get_queue_snapshot() -> dict:
    parallel_encodes = get_parallel_encode_limit()
    with state_lock:
        all_jobs = [asdict(job) for job in sorted(jobs.values(), key=lambda j: j.id)]
        running_job_ids = sorted(running_processes.keys())
        stats = {
            "queued": sum(1 for job in jobs.values() if job.status == "queued"),
            "running": sum(1 for job in jobs.values() if job.status == "running"),
            "completed": sum(1 for job in jobs.values() if job.status == "completed"),
            "failed": sum(1 for job in jobs.values() if job.status == "failed"),
            "skipped": sum(1 for job in jobs.values() if job.status == "skipped"),
            "stopped": sum(1 for job in jobs.values() if job.status == "stopped"),
        }
        return {
            "jobs": all_jobs,
            "stats": stats,
            "runner_active": running_event.is_set(),
            "current_job_id": running_job_ids[0] if running_job_ids else None,
            "running_job_ids": running_job_ids,
            "parallel_encodes": parallel_encodes,
            "queue_depth": job_queue.qsize(),
        }


HTML = """
<!doctype html>
<html>
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Anime Reencoder</title>
  <style>
    body { font-family: Arial, sans-serif; margin: 16px; background: #111; color: #eee; }
    h1, h2 { margin: 0 0 12px 0; }
    .grid { display: grid; grid-template-columns: 2fr 1fr; gap: 14px; }
    .card { background: #1a1a1a; border: 1px solid #333; border-radius: 8px; padding: 12px; }
    button, input, select { background: #2a2a2a; color: #eee; border: 1px solid #444; border-radius: 4px; padding: 6px; }
    button { cursor: pointer; }
    table { width: 100%; border-collapse: collapse; }
    th, td { border-bottom: 1px solid #2f2f2f; padding: 6px; text-align: left; font-size: 13px; }
    th { position: sticky; top: 0; background: #1a1a1a; }
    .scroll { max-height: 420px; overflow: auto; }
    .row-actions { display: flex; gap: 6px; flex-wrap: wrap; margin: 8px 0; }
    .muted { color: #bbb; font-size: 12px; }
    .tag { padding: 2px 6px; border: 1px solid #444; border-radius: 999px; font-size: 11px; }
    .status-queued { color: #f6c343; }
    .status-running { color: #59a9ff; }
    .status-completed { color: #62d26f; }
    .status-failed, .status-stopped { color: #ff6f6f; }
    .status-skipped { color: #bbb; }
    video { width: 100%; max-height: 280px; background: #000; }
        .loading { color: #8ec5ff; font-size: 12px; margin-bottom: 8px; }
  </style>
</head>
<body>
  <h1>Anime Reencoder Web App</h1>

  <div class="card" style="margin-bottom: 14px;">
    <div class="row-actions">
      <label>Folder <input id="folder" size="45"></label>
            <label>Encoder
                <select id="video_encoder">
                    <option value="hevc_nvenc">hevc_nvenc (NVIDIA HEVC)</option>
                    <option value="h264_nvenc">h264_nvenc (NVIDIA H.264)</option>
                    <option value="libx265">libx265 (CPU HEVC)</option>
                    <option value="libx264">libx264 (CPU H.264)</option>
                </select>
            </label>
            <label>Preset
                <select id="preset">
                    <option value="p1">p1 (fastest / lowest quality)</option>
                    <option value="p2">p2</option>
                    <option value="p3">p3</option>
                    <option value="p4">p4</option>
                    <option value="p5">p5</option>
                    <option value="p6">p6</option>
                    <option value="p7">p7 (slowest / best quality)</option>
                </select>
            </label>
      <label>CQ <input id="cq" value="23" size="4"></label>
            <label>Audio Codec
                <select id="audio_codec">
                    <option value="aac">aac</option>
                    <option value="copy">copy</option>
                    <option value="libopus">libopus</option>
                    <option value="libmp3lame">libmp3lame</option>
                </select>
            </label>
      <label>Scan Workers <input id="scan_workers" type="number" min="1" max="64" size="4"></label>
    <label>Parallel Encodes <input id="parallel_encodes" type="number" min="1" max="32" size="4"></label>
      <label>Audio Bitrate <input id="audio_bitrate" value="192k" size="6"></label>
      <label><input id="replace_original" type="checkbox" checked> Replace if smaller</label>
      <label><input id="respect_skip_rules" type="checkbox" checked> Respect skip rules</label>
      <button onclick="saveConfig()">Save Config</button>
            <button onclick="refreshVideos(true)">Rescan Videos</button>
    </div>
    <div class="muted">Skip rules: tagged, already HEVC, or long episodes that are already small.</div>
  </div>

  <div class="grid">
    <div class="card">
      <h2>Videos</h2>
            <div id="videosLoading" class="loading">Loading video list...</div>
      <div class="row-actions">
        <input id="search" placeholder="Filter by name/path" oninput="renderVideos()">
        <button onclick="selectAllVisible()">Select Visible</button>
                <button onclick="selectEncodableVisible()">Select Encodable</button>
        <button onclick="clearSelection()">Clear Selection</button>
        <button onclick="enqueueSelected()">Add Selected to Queue</button>
      </div>
      <div class="scroll">
        <table>
          <thead>
            <tr><th></th><th>File</th><th>Codec</th><th>Size</th><th>Duration</th><th>Notes</th></tr>
          </thead>
          <tbody id="videosBody"></tbody>
        </table>
      </div>
    </div>

    <div class="card">
      <h2>Preview</h2>
      <video id="player" controls></video>
      <div id="previewText" class="muted">Click a file row to preview.</div>
    </div>
  </div>

  <div class="card" style="margin-top: 14px;">
    <h2>Queue</h2>
    <div class="row-actions">
      <button onclick="startQueue()">Start</button>
      <button onclick="pauseQueue()">Pause</button>
      <button onclick="stopCurrent()">Stop Current Job</button>
      <button onclick="clearFinished()">Clear Finished</button>
      <span id="queueStats" class="muted"></span>
            <span id="runnerState" class="tag"></span>
    </div>
    <div class="scroll">
      <table>
        <thead>
          <tr><th>ID</th><th>File</th><th>Status</th><th>Progress</th><th>Message</th><th>Actions</th></tr>
        </thead>
        <tbody id="queueBody"></tbody>
      </table>
    </div>
  </div>

  <script>
    let videos = [];
    let selected = new Set();
        let scanPollTimer = null;

    async function api(url, method='GET', body=null) {
      const response = await fetch(url, {
        method,
        headers: { 'Content-Type': 'application/json' },
        body: body ? JSON.stringify(body) : null
      });
      return response.json();
    }

    function formatDuration(seconds) {
      const sec = Math.floor(seconds || 0);
      const h = Math.floor(sec / 3600);
      const m = Math.floor((sec % 3600) / 60);
      const s = sec % 60;
      return `${h}:${String(m).padStart(2,'0')}:${String(s).padStart(2,'0')}`;
    }

    function escapeHtml(text) {
      return text
        .replaceAll('&', '&amp;')
        .replaceAll('<', '&lt;')
        .replaceAll('>', '&gt;')
        .replaceAll('"', '&quot;')
        .replaceAll("'", '&#039;');
    }

    function renderVideos() {
      const q = document.getElementById('search').value.toLowerCase().trim();
      const body = document.getElementById('videosBody');
      body.innerHTML = '';
      videos
        .filter(v => !q || v.rel_path.toLowerCase().includes(q))
        .forEach(v => {
          const tr = document.createElement('tr');
          tr.innerHTML = `
            <td><input type="checkbox" ${selected.has(v.rel_path) ? 'checked' : ''} onchange="toggleSelection('${encodeURIComponent(v.rel_path)}', this.checked)"></td>
            <td><a href="#" onclick="previewVideo('${encodeURIComponent(v.rel_path)}'); return false;">${escapeHtml(v.rel_path)}</a></td>
            <td>${escapeHtml(v.codec || 'unknown')}</td>
            <td>${v.size_mb.toFixed(2)} MB</td>
            <td>${formatDuration(v.duration_seconds)}</td>
            <td>${v.skip_reason ? `<span class="tag">${escapeHtml(v.skip_reason)}</span>` : ''}</td>
          `;
          body.appendChild(tr);
        });
    }

    function toggleSelection(encodedRelPath, checked) {
      const relPath = decodeURIComponent(encodedRelPath);
      if (checked) selected.add(relPath); else selected.delete(relPath);
    }

    function selectAllVisible() {
      const q = document.getElementById('search').value.toLowerCase().trim();
      videos.filter(v => !q || v.rel_path.toLowerCase().includes(q)).forEach(v => selected.add(v.rel_path));
      renderVideos();
    }

        function selectEncodableVisible() {
            const q = document.getElementById('search').value.toLowerCase().trim();
            videos
                .filter(v => (!q || v.rel_path.toLowerCase().includes(q)) && !v.skip_reason)
                .forEach(v => selected.add(v.rel_path));
            renderVideos();
        }

    function clearSelection() {
      selected.clear();
      renderVideos();
    }

    async function previewVideo(encodedRelPath) {
      const relPath = decodeURIComponent(encodedRelPath);
      const player = document.getElementById('player');
      player.src = `/api/video?rel_path=${encodeURIComponent(relPath)}`;
      document.getElementById('previewText').innerText = relPath;
      player.load();
    }

    async function loadConfig() {
      const data = await api('/api/config');
      for (const [k, v] of Object.entries(data)) {
        const el = document.getElementById(k);
        if (!el) continue;
        if (el.type === 'checkbox') el.checked = !!v;
        else el.value = v;
      }
    }

    async function saveConfig() {
      const payload = {
        folder: document.getElementById('folder').value,
        video_encoder: document.getElementById('video_encoder').value,
        preset: document.getElementById('preset').value,
        cq: document.getElementById('cq').value,
        audio_codec: document.getElementById('audio_codec').value,
                scan_workers: Number(document.getElementById('scan_workers').value || 0),
        parallel_encodes: Number(document.getElementById('parallel_encodes').value || 0),
        audio_bitrate: document.getElementById('audio_bitrate').value,
        replace_original: document.getElementById('replace_original').checked,
        respect_skip_rules: document.getElementById('respect_skip_rules').checked
      };
      const out = await api('/api/config', 'POST', payload);
      alert(out.message || 'Config updated');
      await refreshVideos();
    }

        function updateVideosLoading(scan, count) {
            const el = document.getElementById('videosLoading');
            if (!scan) {
                el.innerText = `Loaded ${count} video(s).`;
                return;
            }
            if (scan.in_progress) {
                const total = scan.total || '?';
                const processed = scan.processed || 0;
                el.innerText = `Scanning videos... ${processed}/${total}`;
                return;
            }
            if (scan.last_error) {
                el.innerText = `Scan error: ${scan.last_error}`;
                return;
            }
            el.innerText = `Loaded ${count} video(s).`;
        }

        function scheduleScanPoll() {
            if (scanPollTimer) return;
            scanPollTimer = setTimeout(async () => {
                scanPollTimer = null;
                const data = await api('/api/videos');
                if (Array.isArray(data.videos)) {
                    videos = data.videos;
                    renderVideos();
                }
                updateVideosLoading(data.scan, videos.length);
                if (data.scan && data.scan.in_progress) {
                    scheduleScanPoll();
                }
            }, 1200);
        }

        async function refreshVideos(force=false) {
            updateVideosLoading({ in_progress: true, processed: 0, total: '?' }, videos.length);
            const data = await api(`/api/videos${force ? '?force=1' : ''}`);
            if (Array.isArray(data.videos)) {
                videos = data.videos;
                renderVideos();
            }
            updateVideosLoading(data.scan, videos.length);
            if (data.scan && data.scan.in_progress) {
                scheduleScanPoll();
            }
    }

    async function enqueueSelected() {
      const files = Array.from(selected);
      if (files.length === 0) {
        alert('No videos selected.');
        return;
      }
      const out = await api('/api/queue/enqueue', 'POST', { files });
      alert(out.message);
      await loadQueue();
    }

    async function loadQueue() {
      const data = await api('/api/queue');
      const body = document.getElementById('queueBody');
      body.innerHTML = '';
      document.getElementById('queueStats').innerText =
                `queued=${data.stats.queued}, running=${data.stats.running}, completed=${data.stats.completed}, failed=${data.stats.failed}, skipped=${data.stats.skipped}, stopped=${data.stats.stopped}, queue_depth=${data.queue_depth}, parallel=${data.parallel_encodes}`;
            const runnerState = document.getElementById('runnerState');
            runnerState.innerText = data.runner_active ? 'Runner: Active' : 'Runner: Paused';

      data.jobs.forEach(job => {
        const tr = document.createElement('tr');
                const actionBtn = ['failed', 'stopped', 'skipped'].includes(job.status)
                    ? `<button onclick="retryJob(${job.id})">Retry</button>`
                    : `<button onclick="removeJob(${job.id})">Remove</button>`;
        tr.innerHTML = `
          <td>${job.id}</td>
          <td>${escapeHtml(job.rel_path)}</td>
          <td class="status-${job.status}">${job.status}</td>
          <td>${job.progress.toFixed(1)}%</td>
          <td>${escapeHtml(job.message || '')}</td>
                    <td>${actionBtn}</td>
        `;
        body.appendChild(tr);
      });
    }

    async function removeJob(jobId) {
      await api(`/api/queue/remove/${jobId}`, 'POST');
      await loadQueue();
    }

        async function retryJob(jobId) {
            const out = await api(`/api/queue/retry/${jobId}`, 'POST');
            if (out.message) {
                alert(out.message);
            }
            await loadQueue();
        }

    async function startQueue() { await api('/api/queue/start', 'POST'); await loadQueue(); }
    async function pauseQueue() { await api('/api/queue/pause', 'POST'); await loadQueue(); }
    async function stopCurrent() { await api('/api/queue/stop-current', 'POST'); await loadQueue(); }
    async function clearFinished() { await api('/api/queue/clear-finished', 'POST'); await loadQueue(); }

    async function init() {
      await loadConfig();
            await refreshVideos(true);
      await loadQueue();
      setInterval(loadQueue, 1500);
    }
    init();
  </script>
</body>
</html>
"""


@APP.get("/")
def index():
    return render_template_string(HTML)


@APP.get("/api/config")
def api_get_config():
    with config_lock:
        return jsonify(config)


@APP.post("/api/config")
def api_set_config():
    payload = request.get_json(force=True, silent=True) or {}
    with config_lock:
        folder = payload.get("folder", config["folder"])
        folder_path = Path(folder).expanduser().resolve()
        if not folder_path.exists() or not folder_path.is_dir():
            return jsonify({"message": "Folder does not exist or is not a directory"}), 400

        new_video_encoder = str(payload.get("video_encoder", config["video_encoder"]))
        new_preset = str(payload.get("preset", config["preset"]))
        new_audio_codec = str(payload.get("audio_codec", config["audio_codec"]))

        if new_video_encoder not in ALLOWED_VIDEO_ENCODERS:
            return jsonify({"message": f"Invalid video_encoder. Allowed: {', '.join(ALLOWED_VIDEO_ENCODERS)}"}), 400
        if new_preset not in ALLOWED_PRESETS:
            return jsonify({"message": f"Invalid preset. Allowed: {', '.join(ALLOWED_PRESETS)}"}), 400
        if new_audio_codec not in ALLOWED_AUDIO_CODECS:
            return jsonify({"message": f"Invalid audio_codec. Allowed: {', '.join(ALLOWED_AUDIO_CODECS)}"}), 400

        config["folder"] = str(folder_path)
        config["video_encoder"] = new_video_encoder
        config["preset"] = new_preset
        config["audio_codec"] = new_audio_codec

        for key in ["cq", "audio_bitrate"]:
            if key in payload and str(payload[key]).strip():
                config[key] = str(payload[key]).strip()
        if "scan_workers" in payload:
            config["scan_workers"] = _normalize_int(payload["scan_workers"], DEFAULT_SCAN_WORKERS, 1, 64)
        if "parallel_encodes" in payload:
            config["parallel_encodes"] = _normalize_int(payload["parallel_encodes"], DEFAULT_PARALLEL_ENCODES, 1, 32)
        for key in ["replace_original", "respect_skip_rules"]:
            if key in payload:
                config[key] = _normalize_bool(payload[key])
    save_config_to_disk()
    return jsonify({"message": "Config updated", "config": config})


@APP.get("/api/videos")
def api_videos():
    force = request.args.get("force", "0").strip().lower() in {"1", "true", "yes", "on"}
    if force:
        start_video_scan(force=True)
    else:
        start_video_scan(force=False)

    with video_scan_lock:
        cached = list(video_cache)
    return jsonify({"videos": cached, "scan": get_video_scan_snapshot()})


@APP.get("/api/video")
def api_video_stream():
    rel_path = request.args.get("rel_path", "")
    target = normalize_and_validate_path(rel_path)
    if not target:
        return jsonify({"message": "Invalid file"}), 400
    return send_file(target, conditional=True)


@APP.post("/api/queue/enqueue")
def api_enqueue():
    global next_job_id
    payload = request.get_json(force=True, silent=True) or {}
    files = payload.get("files", [])
    if not isinstance(files, list) or not files:
        return jsonify({"message": "No files provided"}), 400

    with config_lock:
        replace_original = bool(config["replace_original"])
        respect_skip_rules = bool(config["respect_skip_rules"])
        root = Path(config["folder"]).resolve()

    added = 0
    duplicates = 0
    invalid = 0

    with state_lock:
        existing_paths = {job.rel_path for job in jobs.values() if job.status in {"queued", "running"}}
        for rel_path in files:
            if rel_path in existing_paths:
                duplicates += 1
                continue
            abs_path = (root / rel_path).resolve()
            if not is_within_root(abs_path, root) or not abs_path.exists() or not abs_path.is_file():
                invalid += 1
                continue

            job = EncodeJob(
                id=next_job_id,
                rel_path=rel_path,
                abs_path=str(abs_path),
                replace_original=replace_original,
                respect_skip_rules=respect_skip_rules,
            )
            jobs[next_job_id] = job
            job_queue.put(next_job_id)
            next_job_id += 1
            added += 1

    message = f"Queued {added} file(s)."
    if duplicates:
        message += f" Ignored {duplicates} duplicate(s)."
    if invalid:
        message += f" Ignored {invalid} invalid path(s)."
    return jsonify({"message": message})


@APP.get("/api/queue")
def api_queue():
    return jsonify(get_queue_snapshot())


@APP.post("/api/queue/start")
def api_queue_start():
    running_event.set()
    return jsonify({"message": "Queue started"})


@APP.post("/api/queue/pause")
def api_queue_pause():
    running_event.clear()
    return jsonify({"message": "Queue paused (current job continues)"})


@APP.post("/api/queue/stop-current")
def api_queue_stop_current():
    with state_lock:
        if not running_processes:
            return jsonify({"message": "No active job"})
        target_job_id = sorted(running_processes.keys())[0]
        process = running_processes[target_job_id]
        stop_requested_jobs.add(target_job_id)
        process.terminate()
    return jsonify({"message": f"Stop signal sent to job {target_job_id}"})


@APP.post("/api/queue/remove/<int:job_id>")
def api_queue_remove(job_id: int):
    with state_lock:
        job = jobs.get(job_id)
        if not job:
            return jsonify({"message": "Job not found"}), 404
        if job.status == "running":
            return jsonify({"message": "Cannot remove running job"}), 400
        del jobs[job_id]
    return jsonify({"message": "Job removed"})


@APP.post("/api/queue/retry/<int:job_id>")
def api_queue_retry(job_id: int):
    global next_job_id
    with config_lock:
        replace_original = bool(config["replace_original"])
        respect_skip_rules = bool(config["respect_skip_rules"])

    with state_lock:
        source_job = jobs.get(job_id)
        if not source_job:
            return jsonify({"message": "Job not found"}), 404
        if source_job.status not in {"failed", "stopped", "skipped"}:
            return jsonify({"message": "Only failed/stopped/skipped jobs can be retried"}), 400

        duplicate_active = any(
            j.rel_path == source_job.rel_path and j.status in {"queued", "running"}
            for j in jobs.values()
        )
        if duplicate_active:
            return jsonify({"message": "An active job for this file already exists"}), 400

        retry_job = EncodeJob(
            id=next_job_id,
            rel_path=source_job.rel_path,
            abs_path=source_job.abs_path,
            replace_original=replace_original,
            respect_skip_rules=respect_skip_rules,
        )
        jobs[next_job_id] = retry_job
        job_queue.put(next_job_id)
        next_job_id += 1

    return jsonify({"message": "Retry job added to queue"})


@APP.post("/api/queue/clear-finished")
def api_queue_clear_finished():
    with state_lock:
        removable = [
            job_id for job_id, job in jobs.items()
            if job.status in {"completed", "failed", "skipped", "stopped"}
        ]
        for job_id in removable:
            del jobs[job_id]
    return jsonify({"message": f"Cleared {len(removable)} finished job(s)"})


def preflight_check() -> tuple[bool, str]:
    checks = ["ffmpeg", "ffprobe"]
    missing = []
    for binary in checks:
        result = subprocess.run(["which", binary], capture_output=True, text=True)
        if result.returncode != 0:
            missing.append(binary)
    if missing:
        return False, f"Missing required binaries: {', '.join(missing)}"
    return True, "ok"


def start_web(host: str, port: int, debug: bool) -> None:
    ok, message = preflight_check()
    if not ok:
        raise SystemExit(message)

    worker = threading.Thread(target=worker_loop, daemon=True)
    worker.start()
    APP.run(host=host, port=port, debug=debug)


def run_batch_once() -> None:
    with config_lock:
        root = Path(config["folder"]).resolve()
    if not root.exists():
        raise SystemExit(f"Folder not found: {root}")

    print(f"Scanning {root} ...")
    for file_path in root.rglob("*"):
        if not file_path.is_file() or file_path.suffix.lower() not in VIDEO_EXTENSIONS:
            continue

        print(f"\nChecking: {file_path}")
        reason = get_skip_reason(file_path)
        if reason:
            print(f"Skipping ({reason})")
            continue

        output_file = _build_output_path(file_path)
        ffmpeg_cmd = [
            "ffmpeg", "-y", "-i", str(file_path),
            "-c:v", config["video_encoder"],
            "-preset", config["preset"],
            "-cq", str(config["cq"]),
            "-c:a", config["audio_codec"], "-b:a", config["audio_bitrate"],
            "-metadata", ENCODED_TAG,
            str(output_file),
        ]
        result = subprocess.run(ffmpeg_cmd)
        if result.returncode != 0:
            print("Error during encoding, skipping...")
            continue

        if output_file.stat().st_size < file_path.stat().st_size:
            print("New file is smaller. Replacing original...")
            file_path.unlink()
            output_file.rename(file_path)
        else:
            print("New file not smaller. Keeping original...")
            output_file.unlink()


def main() -> None:
    parser = argparse.ArgumentParser(description="Local web reencoder app")
    parser.add_argument("--folder", help="Video library folder")
    parser.add_argument("--host", default="127.0.0.1", help="Web host (default: 127.0.0.1)")
    parser.add_argument("--port", type=int, default=5000, help="Web port (default: 5000)")
    parser.add_argument("--debug", action="store_true", help="Enable Flask debug mode")
    parser.add_argument("--batch-once", action="store_true", help="Run non-web one-pass batch mode")
    args = parser.parse_args()

    if args.folder:
        folder = Path(args.folder).expanduser().resolve()
        if not folder.exists() or not folder.is_dir():
            raise SystemExit(f"Invalid folder: {folder}")
        with config_lock:
            config["folder"] = str(folder)
        save_config_to_disk()

    if args.batch_once:
        run_batch_once()
        return

    start_web(args.host, args.port, args.debug)


if __name__ == "__main__":
    load_config_from_disk()
    main()
