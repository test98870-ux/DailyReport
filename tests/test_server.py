import tempfile
import threading
import unittest
from pathlib import Path
from unittest.mock import patch
from urllib.error import HTTPError
from urllib.request import urlopen

import main


class ServerTests(unittest.TestCase):
    def test_period_filter_uses_publish_time_or_first_collection_for_date_only_items(self):
        news = {"source_type": "뉴스", "published_at": "2026-11-16T00:00:00+09:00", "created_at": "2026-11-16 04:30:00"}
        report = {"source_type": "증권사 리포트", "published_at": "2026-11-16T00:00:00+09:00", "created_at": "2026-11-16 04:30:00"}
        filing = {"source_type": "공시", "published_at": "2026-11-16T10:15:00+09:00", "created_at": "2026-11-16 04:30:00"}
        self.assertEqual(main.filter_timestamp_ms(news), 1794754800000)  # 2026-11-15T15:00:00Z
        self.assertEqual(main.filter_timestamp_ms(report), 1794803400000)  # 2026-11-16T04:30:00Z
        self.assertEqual(main.filter_timestamp_ms(filing), 1794791700000)  # 2026-11-16T01:15:00Z
        self.assertEqual(main.filter_timestamp_ms({"source_type": "뉴스", "published_at": "?"}), 0)
        card = main.render_card({**report, "title": "제목", "source_name": "한경 컨센서스", "url": "https://example.com/a"})
        self.assertIn('data-filter-ts="1794803400000"', card)

    def test_static_files_cannot_escape_static_directory(self):
        with tempfile.TemporaryDirectory() as directory:
            base = Path(directory)
            static = base / "static"
            static.mkdir()
            (static / "styles.css").write_text("body {}")
            (base / "private.txt").write_text("private")
            escape_paths = ["/static/../private.txt", "/static/", "/static/missing.css"]
            try:
                (static / "link.txt").symlink_to(base / "private.txt")
                escape_paths.append("/static/link.txt")
            except OSError:
                # Windows needs Developer Mode or admin rights to create symlinks.
                pass
            with patch.object(main, "STATIC_DIR", static):
                server = main.ReusableThreadingHTTPServer(("127.0.0.1", 0), main.DashboardHandler)
                thread = threading.Thread(target=server.serve_forever, daemon=True)
                thread.start()
                root = f"http://127.0.0.1:{server.server_port}"
                try:
                    with urlopen(root + "/static/styles.css") as response:
                        self.assertEqual(response.read(), b"body {}")
                    for path in escape_paths:
                        with self.subTest(path=path), self.assertRaises(HTTPError) as error:
                            urlopen(root + path)
                        self.assertEqual(error.exception.code, 404)
                        error.exception.close()
                finally:
                    server.shutdown()
                    thread.join()
                    server.server_close()

    def test_server_keeps_loopback_default_and_accepts_lan_address(self):
        for arguments, expected in [([], ("127.0.0.1", 8000)),
                                    (["--host", "192.168.0.10", "--port", "8010"], ("192.168.0.10", 8010))]:
            with self.subTest(arguments=arguments), \
                    patch("sys.argv", ["main.py", *arguments]), \
                    patch.object(main, "init_db"), \
                    patch.object(main, "load_env_file"), \
                    patch.object(main, "start_auto_collection_scheduler"), \
                    patch.object(main, "ReusableThreadingHTTPServer") as server, \
                    patch("builtins.print"):
                main.main()
                server.assert_called_once_with(expected, main.DashboardHandler)
