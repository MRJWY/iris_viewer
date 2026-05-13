from __future__ import annotations

import pandas as pd
import streamlit as st

import shared_app as core
import viewer_body


PUBLIC_VIEWER_ROUTE_MAP: dict[str, tuple[str, str]] = {
    "opportunity": ("iris", "opportunity"),
    "notice": ("iris", "notice"),
    "summary": ("iris", "summary"),
    "opportunity_archive": ("iris", "opportunity_archive"),
    "favorites": ("favorites", "favorites"),
}

HEAVY_NOTICE_PAGES = {"notice", "favorites"}


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


@st.cache_data(ttl=300, show_spinner=False)
def load_public_rfp_datasets(
    notice_master_sheet_name: str,
    opportunity_sheet_name: str,
    opportunity_archive_sheet_name: str,
) -> dict[str, object]:
    notice_master_df = core.filter_notice_dataframe_by_source(
        core.enrich_notice_df(core.load_sheet_as_dataframe(notice_master_sheet_name)),
        "IRIS",
    )
    opportunity_df = core.enrich_opportunity_df(core.load_optional_sheet_as_dataframe(opportunity_sheet_name))
    opportunity_df = core.enrich_opportunity_with_notice_meta(opportunity_df, notice_master_df)
    opportunity_archive_df = core.enrich_opportunity_df(core.load_optional_sheet_as_dataframe(opportunity_archive_sheet_name))
    opportunity_archive_df = core.enrich_opportunity_with_notice_meta(opportunity_archive_df, notice_master_df)
    return core.build_app_datasets(
        notice_master_df=notice_master_df,
        notice_current_df=pd.DataFrame(),
        pending_df=pd.DataFrame(),
        notice_archive_df=pd.DataFrame(),
        opportunity_df=opportunity_df,
        opportunity_archive_df=opportunity_archive_df,
        summary_df=pd.DataFrame(),
        errors_df=pd.DataFrame(),
    )


@st.cache_data(ttl=1800, show_spinner=False)
def load_public_source_notice_datasets() -> dict[str, object]:
    mss_current_df, mss_current_origin = core.load_mss_notice_df()
    mss_past_df, mss_past_origin = core.load_mss_past_df()
    nipa_current_df, nipa_current_origin = core.load_nipa_notice_df()
    nipa_past_df, nipa_past_origin = core.load_nipa_past_df()
    return {
        "mss_current": mss_current_df,
        "mss_current_origin": mss_current_origin,
        "mss_past": mss_past_df,
        "mss_past_origin": mss_past_origin,
        "nipa_current": nipa_current_df,
        "nipa_current_origin": nipa_current_origin,
        "nipa_past": nipa_past_df,
        "nipa_past_origin": nipa_past_origin,
    }


def load_public_viewer_runtime(current_page: str) -> tuple[core.AppModeConfig, dict[str, object], dict[str, object] | None]:
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

    if current_page in HEAVY_NOTICE_PAGES:
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
        source_datasets: dict[str, object] | None = load_public_source_notice_datasets()
    else:
        datasets = load_public_rfp_datasets(
            sheet_names["notice_master"],
            sheet_names["opportunity"],
            sheet_names["opportunity_archive"],
        )
        source_datasets = None

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
        viewer_body.render_public_notice_queue_page(datasets, source_datasets)
        return
    if current_page == "summary":
        viewer_body.render_public_summary_page(datasets["summary"], datasets["opportunity_all"])
        return
    if current_page == "opportunity_archive":
        viewer_body.render_public_opportunity_page(
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

    viewer_body.render_public_opportunity_page(
        datasets["opportunity"],
        page_key="opportunity",
        title="RFP Queue",
        archive=False,
    )


def main() -> None:
    current_page = core.normalize_route_page_key(core.get_query_param("page")) or "opportunity"
    try:
        mode_config, datasets, source_datasets = load_public_viewer_runtime(current_page)
    except Exception as exc:
        if "429" in str(exc) or "Read requests per minute per user" in str(exc):
            st.error("시트 읽기 한도를 잠시 초과했습니다. 잠시 후 새로고침해 주세요.")
            st.caption("초기 로딩 시 필요한 시트만 읽도록 줄였지만, 같은 시점의 반복 새로고침이 겹치면 잠시 제한될 수 있습니다.")
        else:
            st.error(f"시트 로딩 실패: {exc}")
        st.stop()

    render_public_viewer_body(mode_config, datasets, source_datasets)


if __name__ == "__main__":
    main()
