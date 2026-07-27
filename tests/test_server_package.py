from __future__ import annotations

import subprocess
import sys
from pathlib import Path


def test_importing_server_package_does_not_initialize_application() -> None:
    repository_root = Path(__file__).resolve().parents[1]
    result = subprocess.run(
        [
            sys.executable,
            "-c",
            (
                "import sys; import server.services.task_queue; "
                "loaded = 'server.app' in sys.modules or 'server.config' in sys.modules; "
                "raise SystemExit(int(loaded))"
            ),
        ],
        cwd=repository_root,
        check=False,
    )

    assert result.returncode == 0


def test_explicit_server_app_export_remains_compatible() -> None:
    repository_root = Path(__file__).resolve().parents[1]
    result = subprocess.run(
        [
            sys.executable,
            "-c",
            "from server import app; raise SystemExit(int(not hasattr(app, 'title')))",
        ],
        cwd=repository_root,
        check=False,
    )

    assert result.returncode == 0
