from __future__ import annotations

"""Alias shim entrypoint for the public viewer repo."""

try:
    from .root_app_proxy import run_root_viewer
except ImportError:
    from root_app_proxy import run_root_viewer


def main() -> None:
    run_root_viewer()


if __name__ == "__main__":
    main()
