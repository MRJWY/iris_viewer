from __future__ import annotations


def render_page(st, datasets, source_datasets, *, api) -> None:
    del st, source_datasets
    api.render_iris_page("rfp_queue", datasets)
