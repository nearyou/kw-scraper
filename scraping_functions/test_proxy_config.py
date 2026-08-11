import json
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from sqlalchemy import create_engine

from scraping_functions.proxy_config import load_proxy_settings
from scraping_functions.scraper import Scraper, create_proxy_auth_extension
from www.models import Proxy


class ProxyConfigurationTests(unittest.TestCase):
    def test_returns_none_when_proxy_is_not_configured(self):
        self.assertIsNone(load_proxy_settings({}))

    def test_uses_bright_data_default_port(self):
        settings = load_proxy_settings(
            {
                "PROXY_HOST": "brd.superproxy.io",
                "PROXY_USERNAME": "customer-zone",
                "PROXY_PASSWORD": "secret",
            }
        )

        self.assertEqual(settings.port, "33335")
        self.assertEqual(settings.scheme, "http")

    def test_rejects_partial_credentials_without_exposing_values(self):
        with self.assertRaisesRegex(ValueError, "PROXY_PASSWORD") as raised:
            load_proxy_settings(
                {"PROXY_HOST": "proxy.example", "PROXY_USERNAME": "sensitive-user"}
            )
        self.assertNotIn("sensitive-user", str(raised.exception))

    def test_writes_manifest_v3_authentication_extension(self):
        extension = Path(
            create_proxy_auth_extension(
                "proxy.example", "33335", "user-with-quotes\"", "secret", "http"
            )
        )
        manifest = json.loads((extension / "manifest.json").read_text(encoding="utf-8"))
        background = (extension / "background.js").read_text(encoding="utf-8")

        self.assertEqual(manifest["manifest_version"], 3)
        self.assertIn("webRequestAuthProvider", manifest["permissions"])
        self.assertIn("asyncBlocking", background)
        self.assertIn(json.dumps('user-with-quotes"'), background)
        self.assertIn(json.dumps("secret"), background)

    def test_scraper_seeds_environment_proxy_idempotently(self):
        with tempfile.TemporaryDirectory() as directory:
            database_url = f"sqlite:///{Path(directory) / 'proxy.db'}"
            engine = create_engine(database_url)
            Proxy.__table__.create(engine)
            proxy_environment = {
                "PROXY_HOST": "brd.superproxy.io",
                "PROXY_PORT": "33335",
                "PROXY_USERNAME": "customer-zone",
                "PROXY_PASSWORD": "secret",
            }

            with patch.dict(os.environ, proxy_environment, clear=False):
                first = Scraper(db_url=database_url)
                second = Scraper(db_url=database_url)

            proxies = second.get_all_proxies()
            self.assertEqual(len(proxies), 1)
            self.assertEqual(proxies[0].host, "brd.superproxy.io")
            self.assertEqual(proxies[0].port, "33335")

            first.Session.remove()
            second.Session.remove()
            first.engine.dispose()
            second.engine.dispose()
            engine.dispose()


if __name__ == "__main__":
    unittest.main()
