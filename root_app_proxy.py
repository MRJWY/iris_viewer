from __future__ import annotations

import importlib.util
import sys
from functools import lru_cache
from pathlib import Path
from types import ModuleType


ROOT_DIR = Path(__file__).resolve().parents[1]
ROOT_APP_PATH = ROOT_DIR / "app.py"


@lru_cache(maxsize=1)
def load_root_app_module() -> ModuleType:
    if not ROOT_APP_PATH.exists():
        raise RuntimeError(
            "iris_viewer_repo is configured as a shim repo, but the repository-root "
            f"viewer app was not found at {ROOT_APP_PATH}. Check out iris_crawling "
            "next to iris_viewer_repo or restore the root app path."
        )

    root_dir_text = str(ROOT_DIR)
    if root_dir_text not in sys.path:
        sys.path.insert(0, root_dir_text)

    spec = importlib.util.spec_from_file_location("iris_root_app", ROOT_APP_PATH)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Unable to load root app module from {ROOT_APP_PATH}")

    module = importlib.util.module_from_spec(spec)
    sys.modules.setdefault("iris_root_app", module)
    spec.loader.exec_module(module)
    return module


def run_root_viewer() -> None:
    root_app = load_root_app_module()
    root_app.main(app_mode="viewer")
