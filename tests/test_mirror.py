"""Mirror session regressions, without capture, APIs, or Discord requests."""
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from nmixx_subtitles import config
from nmixx_subtitles.mirror import SessionPoster


class MirrorDestinationTests(unittest.IsolatedAsyncioTestCase):
    async def test_project_dotenv_wins_over_inherited_webhook(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            expected = "https://discord.com/api/webhooks/project/test-token"
            (root / ".env").write_text(f"DISCORD_WEBHOOK_URL={expected}\n")
            with patch.object(config, "ROOT", root), patch.object(
                config, "DISCORD_WEBHOOK_URL", "http://localhost:9000/local-webhook"
            ):
                poster = SessionPoster(discord=True, provider="gemini")
                try:
                    self.assertEqual(poster.webhook_url, expected)
                finally:
                    await poster.close()

    async def test_missing_project_webhook_never_falls_back(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            (root / ".env").write_text("SUBTITLE_PROVIDER=gemini\n")
            with patch.object(config, "ROOT", root), patch.object(
                config, "DISCORD_WEBHOOK_URL", "http://localhost:9000/local-webhook"
            ):
                with self.assertRaises(ValueError):
                    SessionPoster(discord=True, provider="gemini")


if __name__ == "__main__":
    unittest.main()
