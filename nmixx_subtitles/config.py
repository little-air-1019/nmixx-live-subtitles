"""Env-based config. Loads .env from the repo root if present."""
import os
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent

def _load_dotenv() -> None:
    env = ROOT / ".env"
    if not env.exists():
        return
    for line in env.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, value = line.partition("=")
        os.environ.setdefault(key.strip(), value.strip())

_load_dotenv()

DISCORD_WEBHOOK_URL = os.environ.get("DISCORD_WEBHOOK_URL", "")
GEMINI_API_KEY = os.environ.get("GEMINI_API_KEY", "")
YOUTUBE_API_KEY = os.environ.get("YOUTUBE_API_KEY", "")
PUBLIC_BASE_URL = os.environ.get("PUBLIC_BASE_URL", "")
WEBSUB_VERIFY_TOKEN = os.environ.get("WEBSUB_VERIFY_TOKEN", "change-me")
YOUTUBE_CHANNEL_ID = os.environ.get("YOUTUBE_CHANNEL_ID", "UCnUAyD4t2LkvW68YrDh7fDg")
GEMINI_LIVE_MODEL = os.environ.get(
    "GEMINI_LIVE_MODEL",
    "gemini-3.5-live-translate-preview",
)
SUBTITLE_PROVIDER = os.environ.get("SUBTITLE_PROVIDER", "gemini").strip().lower()
SONIOX_API_KEY = os.environ.get("SONIOX_API_KEY", "")
SONIOX_MODEL = os.environ.get("SONIOX_MODEL", "stt-rt-v5")
SONIOX_WEBSOCKET_URL = os.environ.get(
    "SONIOX_WEBSOCKET_URL", "wss://stt-rt.soniox.com/transcribe-websocket"
)
SONIOX_TRANSLATE_ENGLISH = os.environ.get("SONIOX_TRANSLATE_ENGLISH", "false").lower() in (
    "true", "1", "yes",
)
TEXT_TRANSLATION_MODEL = os.environ.get("TEXT_TRANSLATION_MODEL", "gemini-3.5-flash-lite")
QUALITY_CAPTURE_ENABLED = os.environ.get("QUALITY_CAPTURE_ENABLED", "true").lower() in ("true", "1", "yes")
QUALITY_CAPTURE_DIR = Path(os.environ.get(
    "QUALITY_CAPTURE_DIR", str(Path.home() / "Library/Application Support/nmixx-subtitles/sessions"),
)).expanduser()
QUALITY_RETENTION_DAYS = int(os.environ.get("QUALITY_RETENTION_DAYS", "7"))
QUALITY_MAX_BYTES = int(os.environ.get("QUALITY_MAX_BYTES", str(2 * 1024**3)))
STATE_FILE = ROOT / "state.json"
