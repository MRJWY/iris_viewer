from __future__ import annotations

import pandas as pd
import streamlit as st

import viewer_core as core


NOTICE_COLUMNS = [
    "공고일자",
    "공고기간",
    "전문기관",
    "공고명",
    "대표추천도",
    "대표과제명",
    "대표예산",
    "공고상태",
]

SUMMARY_COLUMNS = [
    "공고일자",
    "공고기간",
    "전문기관",
    "공고명",
    "대표추천도",
    "대표점수",
    "해당 과제명",
    "예산",
    "공고상태",
]

OPPORTUNITY_COLUMNS = [
    "공고일자",
    "공고기간",
    "전문기관",
    "공고명",
    "과제명",
    "추천도",
    "점수",
    "예산",
    "공고상태",
]


def clean(value) -> str:
    return core.clean(value)


def first_non_empty(row: dict, *keys: str) -> str:
    for key in keys:
        value = clean(row.get(key))
        if value:
            return value
    return ""


def render_metric_row(items: list[tuple[str, str]]) -> None:
    columns = st.columns(len(items))
    for col, (label, value) in zip(columns, items):
        col.metric(label, value)


def filter_df(
    df: pd.DataFrame,
    *,
    prefix: str,
    search_columns: list[str],
    agency_column: str = "",
    ministry_column: str = "",
    recommendation_column: str = "",
    current_column: str = "is_current",
) -> pd.DataFrame:
    if df.empty:
        return df

    working = df.copy()

    st.sidebar.markdown(f"## {prefix.title()} Filters")
    search_text = st.sidebar.text_input("검색", "", key=f"{prefix}_search")
    current_only = st.sidebar.checkbox("현재 공고만", value=True, key=f"{prefix}_current")
    if agency_column and agency_column in working.columns:
        agencies = sorted(
            x
            for x in working[agency_column].fillna("").astype(str).str.strip().unique().tolist()
            if clean(x)
        )
        agency_value = st.sidebar.selectbox("전문기관", ["전체"] + agencies, key=f"{prefix}_agency")
    else:
        agency_value = "전체"

    if recommendation_column and recommendation_column in working.columns:
        recommendation_values = sorted(
            x
            for x in working[recommendation_column].fillna("").astype(str).str.strip().unique().tolist()
            if clean(x)
        )
        recommendation_value = st.sidebar.selectbox("추천도", ["전체"] + recommendation_values, key=f"{prefix}_recommendation")
    else:
        recommendation_value = "전체"

    if ministry_column and ministry_column in working.columns:
        ministries = sorted(
            x
            for x in working[ministry_column].fillna("").astype(str).str.strip().unique().tolist()
            if clean(x)
        )
        ministry_value = st.sidebar.selectbox("소관부처", ["전체"] + ministries, key=f"{prefix}_ministry")
    else:
        ministry_value = "전체"

    if current_only and current_column in working.columns:
        working = working[working[current_column].fillna("").astype(str).str.strip().eq("Y")]

    if agency_column and agency_column in working.columns and agency_value != "전체":
        working = working[working[agency_column].fillna("").astype(str).str.strip().eq(agency_value)]

    if ministry_column and ministry_column in working.columns and ministry_value != "전체":
        working = working[working[ministry_column].fillna("").astype(str).str.strip().eq(ministry_value)]

    if recommendation_column and recommendation_column in working.columns and recommendation_value != "전체":
        working = working[working[recommendation_column].fillna("").astype(str).str.strip().eq(recommendation_value)]

    if search_text:
        working = working[core.build_contains_mask(working, search_columns, search_text)]

    return working


def add_period_alias(df: pd.DataFrame) -> pd.DataFrame:
    working = df.copy()
    if "공고기간" not in working.columns:
        working["공고기간"] = core.series_from_candidates(working, ["접수기간", "공고기간", "period"])
    return working


def add_viewer_id(df: pd.DataFrame, *, kind: str) -> pd.DataFrame:
    working = df.copy()
    if kind == "notice":
        primary = core.series_from_candidates(working, ["공고ID"])
        fallback = core.series_from_candidates(working, ["공고번호", "공고명"])
    elif kind == "summary":
        primary = core.series_from_candidates(working, ["공고ID"])
        fallback = core.series_from_candidates(working, ["공고번호", "공고명"])
    else:
        primary = core.series_from_candidates(working, ["document_id", "문서ID"])
        fallback_base = core.series_from_candidates(working, ["notice_id", "공고ID"])
        fallback_name = core.series_from_candidates(working, ["project_name", "과제명", "rfp_title"])
        fallback = fallback_base.str.cat(fallback_name, sep="|")
    working["_viewer_id"] = primary.where(primary.ne(""), fallback).fillna("").astype(str).str.strip()
    return working


def build_opportunity_table_df(df: pd.DataFrame) -> pd.DataFrame:
    working = add_viewer_id(add_period_alias(df), kind="opportunity")
    working["전문기관"] = core.series_from_candidates(working, ["전문기관명", "전문기관", "agency"])
    working["과제명"] = core.series_from_candidates(working, ["project_name", "과제명"])
    working["추천도"] = core.series_from_candidates(working, ["recommendation", "추천도"])
    working["점수"] = core.series_from_candidates(working, ["rfp_score", "점수"])
    working["예산"] = core.series_from_candidates(working, ["budget", "예산"])
    return working


def render_notice_detail(row: dict, opportunity_df: pd.DataFrame) -> None:
    related = core.find_related_opportunities_for_notice(row, opportunity_df)
    top_related = related.iloc[0].to_dict() if not related.empty else {}

    core.render_detail_header(
        title=clean(row.get("공고명")),
        kicker="IRIS / Notice",
        chips=[
            (clean(row.get("대표추천도")), "accent"),
            (clean(row.get("전문기관")), "neutral"),
            (clean(row.get("공고상태")), "neutral"),
        ],
    )

    left, right = st.columns([2, 1])
    with left:
        core.render_detail_card(
            "공고 정보",
            [
                ("공고명", row.get("공고명")),
                ("공고번호", row.get("공고번호")),
                ("공고일자", row.get("공고일자")),
                ("공고기간", first_non_empty(row, "공고기간", "접수기간")),
                ("전문기관", row.get("전문기관")),
                ("소관부처", row.get("소관부처")),
            ],
        )
    with right:
        core.render_detail_card(
            "대표 분석",
            [
                ("대표추천도", row.get("대표추천도")),
                ("대표과제명", row.get("대표과제명")),
                ("대표예산", row.get("대표예산")),
                ("대표키워드", row.get("대표키워드")),
            ],
        )

    core.render_detail_card(
        "분석 요약",
        [
            ("추천 이유", first_non_empty(top_related, "reason", "대표추천이유")),
            ("개념 및 개발 내용", first_non_empty(top_related, "concept_and_development", "development_content")),
            ("지원필요성", first_non_empty(top_related, "support_need", "support_necessity", "technical_background")),
            ("활용분야", first_non_empty(top_related, "application_field")),
        ],
    )

    detail_link = clean(row.get("상세링크"))
    if detail_link:
        st.link_button("IRIS 상세 바로가기", detail_link, use_container_width=True)

    st.markdown("### Related Opportunity")
    if related.empty:
        st.info("연결된 Opportunity 데이터가 없습니다.")
        return

    display = build_opportunity_table_df(related)
    st.dataframe(
        display[[col for col in OPPORTUNITY_COLUMNS if col in display.columns]],
        use_container_width=True,
        hide_index=True,
    )


def render_summary_detail(row: dict, opportunity_df: pd.DataFrame) -> None:
    related = pd.DataFrame()
    notice_id = clean(row.get("공고ID"))
    if notice_id and not opportunity_df.empty and "notice_id" in opportunity_df.columns:
        related = opportunity_df[opportunity_df["notice_id"].fillna("").astype(str).str.strip().eq(notice_id)].copy()
        if not related.empty:
            related = related.sort_values(by=["rfp_score", "project_name"], ascending=[False, True], na_position="last")
    top_related = related.iloc[0].to_dict() if not related.empty else {}

    core.render_detail_header(
        title=clean(row.get("공고명")),
        kicker="IRIS / Summary",
        chips=[
            (clean(row.get("대표추천도")), "accent"),
            (f"점수 {clean(row.get('대표점수'))}" if clean(row.get("대표점수")) else "", "neutral"),
            (clean(row.get("전문기관")), "neutral"),
        ],
    )

    left, right = st.columns([2, 1])
    with left:
        core.render_detail_card(
            "공고 요약",
            [
                ("공고명", row.get("공고명")),
                ("해당 과제명", row.get("해당 과제명")),
                ("예산", row.get("예산")),
                ("공고일자", row.get("공고일자")),
                ("공고기간", first_non_empty(row, "공고기간", "접수기간")),
                ("전문기관", row.get("전문기관")),
            ],
        )
    with right:
        core.render_detail_card(
            "대표 분석",
            [
                ("대표추천도", row.get("대표추천도")),
                ("대표점수", row.get("대표점수")),
                ("과제 수", row.get("과제수")),
                ("문서 수", row.get("문서수")),
            ],
        )

    core.render_detail_card(
        "대표 RFP 분석",
        [
            ("추천 이유", first_non_empty(top_related, "reason", "대표추천이유")),
            ("개념 및 개발 내용", first_non_empty(top_related, "concept_and_development", "development_content")),
            ("지원필요성", first_non_empty(top_related, "support_need", "support_necessity", "technical_background")),
            ("활용분야", first_non_empty(top_related, "application_field")),
            ("지원계획", first_non_empty(top_related, "support_plan")),
        ],
    )

    detail_link = clean(row.get("상세링크"))
    if detail_link:
        st.link_button("IRIS 상세 바로가기", detail_link, use_container_width=True)


def render_opportunity_detail(row: dict) -> None:
    core.render_detail_header(
        title=first_non_empty(row, "project_name", "rfp_title", "공고명"),
        kicker="IRIS / Opportunity",
        chips=[
            (clean(row.get("recommendation")), "accent"),
            (f"점수 {clean(row.get('rfp_score'))}" if clean(row.get("rfp_score")) else "", "neutral"),
            (clean(first_non_empty(row, "전문기관명", "agency")), "neutral"),
            (clean(row.get("공고상태")), "neutral"),
        ],
    )

    left, right = st.columns([2, 1])
    with left:
        core.render_detail_card(
            "기본 정보",
            [
                ("공고명", first_non_empty(row, "notice_title", "공고명")),
                ("과제명", row.get("project_name")),
                ("RFP 제목", row.get("rfp_title")),
                ("공고일자", first_non_empty(row, "공고일자", "ancm_de")),
                ("공고기간", first_non_empty(row, "공고기간", "접수기간", "period")),
                ("전문기관", first_non_empty(row, "전문기관명", "agency")),
                ("소관부처", first_non_empty(row, "소관부처", "ministry")),
            ],
        )
    with right:
        core.render_detail_card(
            "평가 결과",
            [
                ("추천도", row.get("recommendation")),
                ("점수", row.get("rfp_score")),
                ("예산", row.get("budget")),
                ("키워드", first_non_empty(row, "keywords")),
            ],
        )

    core.render_detail_card(
        "분석 내용",
        [
            ("추천 이유", first_non_empty(row, "reason")),
            ("개념 및 개발 내용", first_non_empty(row, "concept_and_development", "development_content")),
            ("지원필요성", first_non_empty(row, "support_need", "support_necessity", "technical_background")),
            ("활용분야", first_non_empty(row, "application_field")),
            ("지원계획", first_non_empty(row, "support_plan")),
        ],
    )

    detail_link = first_non_empty(row, "상세링크", "detail_link")
    if detail_link:
        st.link_button("IRIS 상세 바로가기", detail_link, use_container_width=True)


def render_notice_table(notice_df: pd.DataFrame, opportunity_df: pd.DataFrame) -> None:
    filtered = add_viewer_id(
        add_period_alias(
        filter_df(
            notice_df,
            prefix="notice",
            search_columns=["공고명", "공고번호", "전문기관", "소관부처", "공고ID", "대표과제명"],
            agency_column="전문기관",
            ministry_column="소관부처",
            recommendation_column="대표추천도",
        )
        ),
        kind="notice",
    )

    render_metric_row(
        [
            ("공고 수", str(len(filtered))),
            ("현재 공고", str(int((filtered["is_current"] == "Y").sum()) if "is_current" in filtered.columns else 0)),
            ("추천 공고", str(int((filtered["대표추천도"] == "추천").sum()) if "대표추천도" in filtered.columns else 0)),
            ("전문기관 수", str(filtered["전문기관"].nunique() if "전문기관" in filtered.columns else 0)),
        ]
    )

    st.caption(f"전체 공고 {len(filtered)}건")
    core.render_clickable_table(filtered, NOTICE_COLUMNS, page_key="notice", id_column="_viewer_id")


def render_summary_table(summary_df: pd.DataFrame) -> None:
    filtered = add_viewer_id(
        add_period_alias(
        filter_df(
            summary_df,
            prefix="summary",
            search_columns=["공고명", "공고번호", "해당 과제명", "예산", "공고ID"],
            agency_column="전문기관",
            ministry_column="소관부처",
            recommendation_column="대표추천도",
        )
        ),
        kind="summary",
    )

    render_metric_row(
        [
            ("요약 공고 수", str(len(filtered))),
            ("추천 공고", str(int((filtered["대표추천도"] == "추천").sum()) if "대표추천도" in filtered.columns else 0)),
            (
                "평균 과제수",
                f"{pd.to_numeric(filtered['과제수'], errors='coerce').fillna(0).mean():.1f}"
                if "과제수" in filtered.columns and len(filtered) > 0
                else "-",
            ),
        ]
    )

    st.caption(f"요약 공고 {len(filtered)}건")
    core.render_clickable_table(filtered, SUMMARY_COLUMNS, page_key="summary", id_column="_viewer_id")


def render_opportunity_table(opportunity_df: pd.DataFrame) -> None:
    filtered = build_opportunity_table_df(
        filter_df(
            opportunity_df,
            prefix="opportunity",
            search_columns=["notice_title", "공고명", "project_name", "rfp_title", "keywords", "budget", "notice_id"],
            agency_column="전문기관명",
            ministry_column="소관부처",
            recommendation_column="recommendation",
        )
    )

    render_metric_row(
        [
            ("Opportunity 수", str(len(filtered))),
            ("추천 건수", str(int((filtered["추천도"] == "추천").sum()) if "추천도" in filtered.columns else 0)),
            ("공고 수", str(filtered["notice_id"].nunique() if "notice_id" in filtered.columns else 0)),
        ]
    )

    st.caption(f"Opportunity {len(filtered)}건")
    core.render_clickable_table(filtered, OPPORTUNITY_COLUMNS, page_key="opportunity", id_column="_viewer_id")


def render_other_crawlers_tab() -> None:
    st.subheader("Other Crawlers")
    st.info("다른 크롤러 소스는 여기로 확장할 수 있습니다.")


def render_detail_page(page: str, notice_df: pd.DataFrame, summary_df: pd.DataFrame, opportunity_df: pd.DataFrame) -> None:
    nav1, nav2, nav3 = st.columns(3)
    with nav1:
        if st.button("Notice 목록", use_container_width=True):
            core.switch_to_table("notice")
    with nav2:
        if st.button("Summary 목록", use_container_width=True):
            core.switch_to_table("summary")
    with nav3:
        if st.button("Opportunity 목록", use_container_width=True):
            core.switch_to_table("opportunity")

    if page == "notice":
        selected_id = core.get_query_param("id")
        working = add_viewer_id(add_period_alias(notice_df), kind="notice")
        row = core.get_row_by_column_value(working, "_viewer_id", selected_id)
        render_notice_detail(add_period_alias(pd.DataFrame([row])).iloc[0].to_dict() if row else {}, opportunity_df)
        return

    if page == "summary":
        selected_id = core.get_query_param("id")
        working = add_viewer_id(add_period_alias(summary_df), kind="summary")
        row = core.get_row_by_column_value(working, "_viewer_id", selected_id)
        render_summary_detail(add_period_alias(pd.DataFrame([row])).iloc[0].to_dict() if row else {}, opportunity_df)
        return

    if page == "opportunity":
        selected_id = core.get_query_param("id")
        working = build_opportunity_table_df(opportunity_df)
        row = core.get_row_by_column_value(working, "_viewer_id", selected_id)
        if row:
            row = add_period_alias(pd.DataFrame([row])).iloc[0].to_dict()
        render_opportunity_detail(row or {})
        return

    st.info("선택한 상세 페이지를 찾지 못했습니다.")


def load_viewer_data() -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    notice_sheet = core.get_env("NOTICE_MASTER_SHEET", "IRIS_NOTICE_MASTER")
    summary_sheet = core.get_env("SUMMARY_SHEET", "SUMMARY")
    opportunity_sheet = core.get_env("OPPORTUNITY_MASTER_SHEET", "OPPORTUNITY_MASTER")

    notice_df = core.enrich_notice_df(core.load_sheet_as_dataframe(notice_sheet))
    opportunity_df = core.enrich_opportunity_df(core.load_optional_sheet_as_dataframe(opportunity_sheet))
    opportunity_df = core.enrich_opportunity_with_notice_meta(opportunity_df, notice_df)
    summary_df = core.enrich_summary_df(core.load_optional_sheet_as_dataframe(summary_sheet))
    summary_df = core.enrich_summary_with_notice_meta(summary_df, notice_df)
    notice_df = core.merge_notice_with_analysis(notice_df, opportunity_df)
    return notice_df, summary_df, opportunity_df


def main() -> None:
    st.set_page_config(page_title="IRIS Public Viewer", page_icon="IRIS", layout="wide")
    core.inject_page_styles()

    st.title("IRIS Public Viewer")
    st.caption("IRIS / SUMMARY / OPPORTUNITY 시트를 읽기 전용으로 조회합니다.")

    try:
        notice_df, summary_df, opportunity_df = load_viewer_data()
    except Exception as exc:
        st.error(str(exc))
        st.stop()

    current_source = core.get_query_param("source") or "iris"
    source_index = 0 if current_source == "iris" else 1
    selected_source = st.radio(
        "Source",
        ["IRIS", "Other Crawlers"],
        horizontal=True,
        index=source_index,
    )
    selected_source_key = "iris" if selected_source == "IRIS" else "other"

    if selected_source_key != current_source:
        st.query_params.clear()
        st.query_params.update({
            "source": selected_source_key,
            "page": "notice",
            "view": "table",
        })
        st.rerun()

    current_page = core.get_query_param("page") or "notice"
    current_view = core.get_query_param("view") or "table"

    if current_view == "detail":
        render_detail_page(current_page, notice_df, summary_df, opportunity_df)
        return

    if selected_source_key == "other":
        render_other_crawlers_tab()
        return

    st.caption("기본 진입은 Notice이며, Summary와 Opportunity는 탭으로 이동해 확인할 수 있습니다.")
    notice_tab, summary_tab, opportunity_tab = st.tabs(["Notice", "Summary", "Opportunity"])
    with notice_tab:
        render_notice_table(notice_df, opportunity_df)
    with summary_tab:
        render_summary_table(summary_df)
    with opportunity_tab:
        render_opportunity_table(opportunity_df)


if __name__ == "__main__":
    main()
