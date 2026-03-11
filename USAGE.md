# Anime Reencoder — Usage

Lightweight local tool to browse a video library, queue batch re-encodes, and monitor progress.

## Quick start

1. Clone or copy this repository to your machine.
2. Create and activate a virtual environment, then install dependencies:

```bash
cd /media/jellyfin/reencoder
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

3. Ensure `ffmpeg` and `ffprobe` are installed and on `PATH`.

Note: this tool was primarily made for encoding anime using NVIDIA NVENC in batches. An NVIDIA GPU with NVENC support (for example an RTX 3060) is recommended for best performance.

## Run (web UI)

Start the web interface to browse and queue jobs:

```bash
python3 reencode_anime.py --folder /path/to/your/library --host 127.0.0.1 --port 5000
```

Open the UI in a browser at the address printed (e.g. http://127.0.0.1:5000).

## Run (one-pass / CLI)

To run a single batch job (scan + encode and exit):

```bash
python3 reencode_anime.py --folder /path/to/your/library --batch-once
```

Note: the exact CLI flags supported are printed by the script `--help` output. For details run:

```bash
python3 reencode_anime.py --help
```

## Configuration

- Settings are persisted to `reencoder_config.json` in the repo folder. Edit that file to change defaults or use the UI.
- By default, the tool will avoid replacing originals when the output is larger; change that behavior via config or UI.

## Requirements

- Linux (tested)
- Python 3.10+
- `ffmpeg`, `ffprobe` available in `PATH`

## Troubleshooting

- If the web UI doesn't start, check for missing dependencies and confirm the chosen `--port` is available.
- For encoding failures, inspect the job logs shown in the UI or run the script with `--batch-once` to see console output.

## Examples

- Start UI on localhost port 5000:

```bash
python3 reencode_anime.py --folder /media/jellyfin/anime --host 127.0.0.1 --port 5000
```

- Run one batch and exit:

```bash
python3 reencode_anime.py --folder /media/jellyfin/anime --batch-once
```
