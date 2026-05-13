from __future__ import annotations

import streamlit as st

import shared_app as core


PUBLIC_VIEWER_ROUTE_MAP: dict[str, tuple[str, str]] = {
    "opportunity": ("iris", "opportunity"),
    "notice": ("iris", "notice"),
    "summary": ("iris", "summary"),
    "opportunity_archive": ("iris", "opportunity_archive"),
    "favorites": ("favorites", "favorites"),
}


def inject_public_viewer_styles() -> None:
    st.markdown(
        """
        <style>
        [data-testid="stSidebar"],
        section[data-testid="stSidebar"],
        [data-testid="collapsedControl"] {
          display: none !important;
        }
        .main .block-container {
          max-width: min(1680px, calc(100vw - 2rem));
        }
        </style>
        """,
        unsafe_allow_html=True,
    )


def load_public_viewer_runtime() -> tuple[core.AppModeConfig, dict[str, object], dict[str, object]]:
    core.load_dotenv()

    mode_config = core.build_app_mode_config(
        "viewer",
        nipa_view_columns=tuple(core.NIPA_VIEW_COLUMNS),
    )

    st.set_page_config(
        page_title=mode_config.page_title,
        layout="wide",
    )
    core.inject_page_styles()
    core.inject_opportunity_detail_alignment_styles()
    inject_public_viewer_styles()
    core.require_login(mode_config)

    sheet_names = {
        "notice_master": core.resolve_canonical_notice_master_sheet(core.get_env),
        "notice_current": core.resolve_notice_current_view_sheet(core.get_env),
        "pending": core.resolve_notice_pending_view_sheet(core.get_env),
        "notice_archive": core.resolve_notice_archive_view_sheet(core.get_env),
        "opportunity": core.resolve_iris_opportunity_current_sheet(core.get_env),
        "opportunity_archive": core.resolve_iris_opportunity_archive_sheet(core.get_env),
        "summary": core.get_env("SUMMARY_SHEET", "SUMMARY"),
        "errors": core.get_env("ERROR_SHEET", "OPPORTUNITY_ERRORS"),
    }

    datasets = core.load_app_datasets(
        sheet_names["notice_master"],
        sheet_names["notice_current"],
        sheet_names["pending"],
        sheet_names["notice_archive"],
        sheet_names["opportunity"],
        sheet_names["opportunity_archive"],
        sheet_names["summary"],
        sheet_names["errors"],
    )
    source_datasets = core.build_source_datasets()
    if core.is_user_scoped_operations_enabled():
        datasets, source_datasets = core.apply_user_review_statuses(
            datasets,
            source_datasets,
            core.get_current_operation_scope_key(),
        )
    return mode_config, datasets, source_datasets


def render_public_viewer_body(
    mode_config: core.AppModeConfig,
    datasets: dict[str, object],
    source_datasets: dict[str, object],
) -> None:
    core.render_workspace_header(mode_config)

    current_page = core.normalize_route_page_key(core.get_query_param("page")) or "opportunity"
    if current_page not in PUBLIC_VIEWER_ROUTE_MAP:
        current_page = "opportunity"

    selected_page = core.render_page_tabs(
        current_page,
        [
            ("opportunity", "RFP Queue"),
            ("notice", "Notice Queue"),
            ("summary", "Summary"),
            ("opportunity_archive", "Archive"),
            ("favorites", "관심공고"),
        ],
        key="public_viewer_primary_tabs",
    )
    if selected_page != current_page:
        target_source, target_page = PUBLIC_VIEWER_ROUTE_MAP[selected_page]
        core.navigate_to_route(target_source, target_page)

    if current_page == "notice":
        core.render_notice_queue_page(datasets, source_datasets)
        return
    if current_page == "summary":
        core.render_summary_page(datasets["summary"], datasets["opportunity_all"])
        return
    if current_page == "opportunity_archive":
        core.render_opportunity_page(
            datasets["opportunity_all"],
            page_key="opportunity_archive",
            title="Opportunity Archive",
            archive=True,
        )
        return
    if current_page == "favorites":
        core.render_favorite_notice_page(
            datasets["notice_view"],
            datasets["opportunity_all"],
            source_datasets,
        )
        return

    core.render_opportunity_page(
        datasets["opportunity"],
        page_key="opportunity",
        title="RFP Queue",
        archive=False,
    )


def main() -> None:
    try:
        mode_config, datasets, source_datasets = load_public_viewer_runtime()
    except Exception as exc:
        st.error(f"시트 로딩 실패: {exc}")
        st.stop()

    render_public_viewer_body(mode_config, datasets, source_datasets)


if __name__ == "__main__":
    main()
