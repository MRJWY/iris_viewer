from __future__ import annotations

"""Legacy dashboard page shim.

The active public viewer runtime now delegates to the repository-root `app.py`.
Prefer updating the root app first when changing Dashboard behavior or styling.
"""


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
