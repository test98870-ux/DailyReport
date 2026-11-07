import tempfile
import threading
import unittest
from pathlib import Path
from unittest.mock import patch
from urllib.error import HTTPError
from urllib.request import urlopen

import main


class ServerTests(unittest.TestCase):
    def test_static_files_cannot_escape_static_directory(self):
        with tempfile.TemporaryDirectory() as directory:
            base = Path(directory)
            static = base / "static"
            static.mkdir()
            (static / "styles.css").write_text("body {}")
            (base / "private.txt").write_text("private")
            (static / "link.txt").symlink_to(base / "private.txt")
            with patch.object(main, "STATIC_DIR", static):
                server = main.ReusableThreadingHTTPServer(("127.0.0.1", 0), main.DashboardHandler)
                thread = threading.Thread(target=server.serve_forever, daemon=True)
                thread.start()
                root = f"http://127.0.0.1:{server.server_port}"
                try:
                    with urlopen(root + "/static/styles.css") as response:
                        self.assertEqual(response.read(), b"body {}")
                    for path in ["/static/../private.txt", "/static/link.txt", "/static/", "/static/missing.css"]:
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
