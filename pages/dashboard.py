from __future__ import annotations


def render_page(
    st,
    source_config,
    mode_config,
    datasets,
    source_datasets,
    *,
    show_internal_tabs=True,
    api,
) -> None:
    del st, source_config, mode_config, show_internal_tabs
    api._inject_opportunity_workspace_styles()
    api._render_public_dashboard_workspace_impl(datasets, source_datasets)
