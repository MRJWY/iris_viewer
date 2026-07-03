from __future__ import annotations

import pandas as pd

try:
    from .root_app_proxy import load_root_app_module
except ImportError:
    from root_app_proxy import load_root_app_module


def render_public_notice_queue_page(
    datasets: dict[str, pd.DataFrame],
    source_datasets: dict[str, object] | None,
) -> None:
    root_app = load_root_app_module()
    root_app.render_notice_queue_page(datasets, source_datasets)


def render_public_opportunity_page(
    df: pd.DataFrame,
    *,
    page_key: str | None = None,
    title: str | None = None,
    archive: bool = False,
    all_df: pd.DataFrame | None = None,
) -> None:
    root_app = load_root_app_module()
    root_app.render_opportunity_page(
        df,
        page_key=page_key,
        title=title,
        archive=archive,
        all_df=all_df,
    )
