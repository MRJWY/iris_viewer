from __future__ import annotations

"""Shim entrypoint for the public viewer repo.

This repository intentionally delegates runtime behavior to the sibling
`iris_crawling/app.py` so that viewer logic has a single source of truth.
"""

try:
    from .root_app_proxy import run_root_viewer
except ImportError:
    from root_app_proxy import run_root_viewer


def main() -> None:
    run_root_viewer()


if __name__ == "__main__":
    main()
