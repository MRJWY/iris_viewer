from __future__ import annotations

"""Legacy RFP queue page shim.

The active public viewer runtime now delegates to the repository-root `app.py`.
Prefer updating the root app first when changing RFP Queue behavior or UI.
"""


def render_page(st, datasets, source_datasets, *, api) -> None:
    del st, source_datasets
    api.render_iris_page("rfp_queue", datasets)
