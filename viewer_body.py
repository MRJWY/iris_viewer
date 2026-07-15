from __future__ import annotations

import pandas as pd

import shared_app as core


def render_public_notice_queue_page(datasets: dict[str, pd.DataFrame], source_datasets: dict[str, object] | None) -> None:
    core.render_notice_queue_page(datasets, source_datasets)


def render_public_opportunity_page(
    df: pd.DataFrame,
    *,
    page_key: str | None = None,
    title: str | None = None,
    archive: bool = False,
    all_df: pd.DataFrame | None = None,
) -> None:
    core.render_opportunity_page(
        df,
        page_key=page_key,
        title=title,
        archive=archive,
        all_df=all_df,
    )
