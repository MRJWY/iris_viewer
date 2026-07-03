from __future__ import annotations

"""Compatibility wrapper for the consolidated public viewer runtime.

The real runtime now lives in the repository-root `app.py`.
This module remains only so existing local commands or imports that point to
`iris_viewer_repo.public_viewer_app` continue to work.
"""

try:
    from .root_app_proxy import run_root_viewer
except ImportError:
    from root_app_proxy import run_root_viewer


def main() -> None:
    run_root_viewer()


def _legacy_public_viewer_main() -> None:
    run_root_viewer()


if __name__ == "__main__":
    main()
