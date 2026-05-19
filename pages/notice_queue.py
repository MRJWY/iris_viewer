from __future__ import annotations

import pandas as pd


def render_page(st, datasets: dict[str, pd.DataFrame], source_datasets: dict[str, object] | None, *, api) -> None:
    source_df = api.build_crawled_notice_collection(datasets, source_datasets)
    api._render_notice_queue_screen(
        source_df,
        datasets.get("opportunity", pd.DataFrame()),
        datasets["opportunity_all"],
    )


def render_source(
    st,
    source_config,
    mode_config,
    datasets: dict[str, pd.DataFrame],
    source_datasets: dict[str, object] | None,
    *,
    show_internal_tabs: bool = True,
    api,
) -> None:
    del st, source_config, mode_config, show_internal_tabs
    source_df = api.build_crawled_notice_collection(datasets, source_datasets)
    api._render_notice_queue_screen(
        source_df,
        datasets.get("opportunity", pd.DataFrame()),
        datasets["opportunity_all"],
    )
