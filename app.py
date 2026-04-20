from __future__ import annotations

import uuid

import pandas as pd
import streamlit as st

import viewer_core as core

REVIEW_OPTIONS = ["", "검토전", "관심공고", "보류", "검토완료"]
FAVORITE_REVIEW_STATUS = "관심공고"


NOTICE_COLUMNS = [
    "공고일자",
    "공고기간",
    "전문기관",
    "공고명",
    "대표추천도",
    "대표과제명",
    "대표예산",
    "공고상태",
    "검토 여부",
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
    "검토 여부",
]


def clean(value) -> str:
    return core.clean(value)


MSS_COLUMNS = [
    "등록일",
    "신청기간",
    "담당부서",
    "공고명",
    "공고번호",
    "상태",
    "검토 여부",
]

NIPA_COLUMNS = [
    "등록일",
    "신청기간",
    "사업명",
    "공고명",
    "공고번호",
    "상태",
    "검토 여부",
]

FAVORITE_COLUMNS = [
    "매체",
    "공고일자",
    "접수기간",
    "전문기관",
    "담당부서",
    "공고명",
    "공고번호",
    "공고상태",
    "검토 여부",
]

COMMENT_COLUMNS = [
    "comment_id",
    "created_at",
    "source",
    "notice_id",
    "notice_title",
    "author",
    "comment",
]


def first_non_empty(row: dict, *keys: str) -> str:
    for key in keys:
        value = clean(row.get(key))
        if value:
            return value
    return ""


def get_spreadsheet():
    gc = core.get_gspread_client()
    sheet_id = core.get_env("GOOGLE_SHEET_ID")
    if not sheet_id:
        raise RuntimeError("GOOGLE_SHEET_ID is not set.")
    return gc.open_by_key(sheet_id)


def get_or_create_worksheet(sheet_name: str, headers: list[str], rows: int = 1000, cols: int | None = None):
    sh = get_spreadsheet()
    try:
        ws = sh.worksheet(sheet_name)
    except Exception:
        ws = sh.add_worksheet(title=sheet_name, rows=rows, cols=cols or len(headers))
        ws.update([headers])
        return ws

    values = ws.get_all_values()
    if not values:
        ws.update([headers])
        return ws

    header = [clean(x) for x in values[0]]
    missing_headers = [column for column in headers if column not in header]
    for offset, column in enumerate(missing_headers, start=len(header) + 1):
        ws.update_cell(1, offset, column)
    return ws


def get_comment_sheet_name() -> str:
    return core.get_env("NOTICE_COMMENT_SHEET", "NOTICE_COMMENTS")


def load_notice_comments() -> pd.DataFrame:
    df = core.load_optional_sheet_as_dataframe(get_comment_sheet_name())
    if df.empty:
        return pd.DataFrame(columns=COMMENT_COLUMNS)

    working = df.copy()
    for column in COMMENT_COLUMNS:
        if column not in working.columns:
            working[column] = ""
    working["created_at_sort"] = pd.to_datetime(working["created_at"], errors="coerce")
    return working.sort_values(by=["created_at_sort"], ascending=False, na_position="last")


def append_notice_comment(
    *,
    source_key: str,
    notice_id: str,
    notice_title: str,
    author: str,
    comment: str,
) -> None:
    notice_id = clean(notice_id)
    comment = clean(comment)
    if not notice_id:
        raise RuntimeError("공고ID가 없어 댓글을 저장할 수 없습니다.")
    if not comment:
        raise RuntimeError("댓글 내용을 입력해 주세요.")

    ws = get_or_create_worksheet(get_comment_sheet_name(), COMMENT_COLUMNS, rows=1000, cols=len(COMMENT_COLUMNS))
    row = {
        "comment_id": str(uuid.uuid4()),
        "created_at": pd.Timestamp.now(tz="Asia/Seoul").strftime("%Y-%m-%d %H:%M:%S"),
        "source": clean(source_key) or "iris",
        "notice_id": notice_id,
        "notice_title": clean(notice_title),
        "author": clean(author) or "익명",
        "comment": comment[:5000],
    }
    ws.append_row([row[column] for column in COMMENT_COLUMNS], value_input_option="USER_ENTERED")
    core.load_sheet_as_dataframe.clear()


def resolve_notice_source_key(row: dict | None) -> str:
    if row:
        source_key = clean(row.get("_source_key"))
        if source_key and source_key != "favorites":
            return source_key
    current_source = core.get_query_param("source") or "iris"
    if current_source in {"tipa", "nipa"}:
        return current_source
    return "iris"


def render_metric_row(items: list[tuple[str, str]]) -> None:
    columns = st.columns(len(items))
    for col, (label, value) in zip(columns, items):
        col.metric(label, value)


def update_review_status_in_sheets(notice_id: str, review_status: str, sheet_names: list[str], *, source_label: str) -> None:
    notice_id = clean(notice_id)
    if not notice_id:
        raise RuntimeError("공고ID가 없어 검토 여부를 저장할 수 없습니다.")

    checked_sheets: list[str] = []
    for sheet_name in dict.fromkeys([name for name in sheet_names if clean(name)]):
        checked_sheets.append(sheet_name)
        try:
            ws = core.get_worksheet(sheet_name)
        except Exception:
            continue

        values = ws.get_all_values()
        if not values:
            continue

        header = [clean(x) for x in values[0]]
        notice_id_col = None
        for candidate in ["공고ID", "notice_id"]:
            if candidate in header:
                notice_id_col = header.index(candidate) + 1
                break
        if not notice_id_col:
            continue

        if "검토 여부" in header:
            review_col = header.index("검토 여부") + 1
        elif "검토여부" in header:
            review_col = header.index("검토여부") + 1
        elif "review_status" in header:
            review_col = header.index("review_status") + 1
        else:
            review_col = len(header) + 1
            ws.update_cell(1, review_col, "검토 여부")

        for row_index, row in enumerate(values[1:], start=2):
            current_notice_id = clean(row[notice_id_col - 1] if notice_id_col - 1 < len(row) else "")
            if current_notice_id == notice_id:
                ws.update_cell(row_index, review_col, clean(review_status))
                core.load_sheet_as_dataframe.clear()
                return

    raise RuntimeError(f"{source_label} 시트({', '.join(checked_sheets)})에서 공고ID {notice_id}를 찾지 못했습니다.")


def update_notice_review_status(notice_id: str, review_status: str, source_key: str = "iris") -> None:
    source_key = clean(source_key) or "iris"
    if source_key == "tipa":
        update_review_status_in_sheets(
            notice_id,
            review_status,
            [
                core.get_env("MSS_CURRENT_SHEET") or core.get_env("MSS_NOTICE_SHEET", "MSS_CURRENT"),
                core.get_env("MSS_PAST_SHEET", "MSS_PAST"),
            ],
            source_label="중소기업벤처부",
        )
        return
    if source_key == "nipa":
        update_review_status_in_sheets(
            notice_id,
            review_status,
            [
                core.get_env("NIPA_CURRENT_SHEET", "NIPA_CURRENT"),
                core.get_env("NIPA_PAST_SHEET", "NIPA_PAST"),
                core.get_env("NIPA_NOTICE_MASTER_SHEET", "NIPA_NOTICE_MASTER"),
            ],
            source_label="NIPA",
        )
        return

    update_review_status_in_sheets(
        notice_id,
        review_status,
        [core.get_env("NOTICE_MASTER_SHEET", "IRIS_NOTICE_MASTER")],
        source_label="IRIS",
    )


def render_review_editor(notice_id: str, current_value: str, form_key: str, source_key: str = "iris") -> None:
    st.markdown("### 검토 여부 수정")
    normalized_value = clean(current_value)
    options = REVIEW_OPTIONS.copy()
    if normalized_value and normalized_value not in options:
        options.append(normalized_value)
    default_index = options.index(normalized_value) if normalized_value in options else 0

    with st.form(form_key):
        review_value = st.selectbox("검토 여부", options=options, index=default_index)
        submitted = st.form_submit_button("저장")
        if submitted:
            try:
                update_notice_review_status(notice_id, review_value, source_key=source_key)
                st.success("검토 여부를 저장했습니다.")
                st.rerun()
            except Exception as exc:
                st.error(f"저장 실패: {exc}")


def render_notice_comments(row: dict, section_key: str) -> None:
    notice_id = clean(row.get("공고ID") or row.get("notice_id"))
    notice_title = clean(row.get("공고명") or row.get("notice_title"))
    source_key = resolve_notice_source_key(row)

    st.markdown("### 댓글")
    if not notice_id:
        st.info("공고ID가 없어 댓글을 연결할 수 없습니다.")
        return

    try:
        comments_df = load_notice_comments()
    except Exception as exc:
        st.warning(f"댓글 이력을 불러오지 못했습니다: {exc}")
        comments_df = pd.DataFrame(columns=COMMENT_COLUMNS)

    matched = comments_df[
        comments_df["source"].fillna("").astype(str).str.strip().eq(source_key)
        & comments_df["notice_id"].fillna("").astype(str).str.strip().eq(notice_id)
    ].copy()

    with st.form(f"{section_key}_comment_form"):
        author = st.text_input("작성자", value=core.get_env("DEFAULT_COMMENT_AUTHOR", ""), key=f"{section_key}_comment_author")
        comment = st.text_area("의견", key=f"{section_key}_comment_text", height=110)
        submitted = st.form_submit_button("댓글 저장")
        if submitted:
            try:
                append_notice_comment(
                    source_key=source_key,
                    notice_id=notice_id,
                    notice_title=notice_title,
                    author=author,
                    comment=comment,
                )
                st.success("댓글을 저장했습니다.")
                st.rerun()
            except Exception as exc:
                st.error(f"댓글 저장 실패: {exc}")

    if matched.empty:
        st.info("아직 등록된 댓글이 없습니다.")
        return

    display_df = matched[["created_at", "author", "comment"]].rename(
        columns={
            "created_at": "작성일시",
            "author": "작성자",
            "comment": "의견",
        }
    )
    st.dataframe(display_df, use_container_width=True, hide_index=True)


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
    if "검토 여부" not in working.columns:
        working["검토 여부"] = core.series_from_candidates(working, ["검토여부", "review_status"])
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
    current_source = core.get_query_param("source") or "iris"
    source_key = resolve_notice_source_key(row)
    is_mss = source_key == "tipa" or current_source == "tipa"
    is_nipa = source_key == "nipa" or current_source == "nipa"
    if is_mss:
        detail_kicker = "중소기업벤처부 / Notice"
        detail_button_label = "중소기업벤처부 상세 바로가기"
    elif is_nipa:
        detail_kicker = "NIPA / Notice"
        detail_button_label = "NIPA 상세 바로가기"
    else:
        detail_kicker = "IRIS / Notice"
        detail_button_label = "IRIS 상세 바로가기"

    core.render_detail_header(
        title=clean(row.get("공고명")),
        kicker=detail_kicker,
        chips=[
            (clean(row.get("대표추천도")), "accent"),
            (first_non_empty(row, "전문기관", "담당부서"), "neutral"),
            (clean(row.get("공고상태")), "neutral"),
            (f"검토: {clean(row.get('검토 여부') or '미지정')}", "neutral"),
        ],
    )

    left, right = st.columns([2, 1])
    with left:
        core.render_detail_card(
            "공고 정보",
            [
                ("공고명", row.get("공고명")),
                ("공고번호", row.get("공고번호")),
                ("사업명", row.get("사업명")),
                ("공고일자", row.get("공고일자")),
                ("공고기간", first_non_empty(row, "공고기간", "접수기간", "신청기간")),
                ("전문기관", first_non_empty(row, "전문기관", "담당부서")),
                ("소관부처", row.get("소관부처")),
                ("검토 여부", row.get("검토 여부")),
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
        st.link_button(detail_button_label, detail_link, use_container_width=True)

    st.markdown("### 검토 상태")
    left_review, right_review = st.columns([1, 1])
    with left_review:
        core.render_detail_card("현재 상태", [("검토 여부", row.get("검토 여부"))])
    with right_review:
        render_review_editor(
            clean(row.get("공고ID") or row.get("notice_id")),
            clean(row.get("검토 여부")),
            form_key=f"notice_review_{source_key}_{clean(row.get('공고ID') or row.get('notice_id'))}",
            source_key=source_key,
        )

    render_notice_comments(row, section_key=f"notice_{source_key}_{clean(row.get('공고ID') or row.get('notice_id'))}")

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


def render_notice_table_with_scope(
    notice_df: pd.DataFrame,
    opportunity_df: pd.DataFrame,
    *,
    page_key: str,
    title: str,
    status_scope: str,
    current_only_default: bool,
) -> None:
    working = notice_df.copy()
    st.subheader(title)

    st.sidebar.markdown("## Notice Filters")
    search_text = st.sidebar.text_input("검색", "", key=f"{page_key}_search")
    current_only = st.sidebar.checkbox("현재 공고만", value=current_only_default, key=f"{page_key}_current")

    if current_only and "is_current" in working.columns:
        working = working[working["is_current"].fillna("").astype(str).str.strip().eq("Y")]

    if "공고상태" in working.columns and status_scope:
        working = working[working["공고상태"].fillna("").astype(str).str.strip().eq(status_scope)]

    agencies = sorted(
        value
        for value in core.series_from_candidates(working, ["전문기관"]).fillna("").astype(str).str.strip().unique().tolist()
        if clean(value)
    )
    agency_value = st.sidebar.selectbox("전문기관", ["전체"] + agencies, key=f"{page_key}_agency")
    if agency_value != "전체" and "전문기관" in working.columns:
        working = working[working["전문기관"].fillna("").astype(str).str.strip().eq(agency_value)]

    ministries = sorted(
        value
        for value in core.series_from_candidates(working, ["소관부처"]).fillna("").astype(str).str.strip().unique().tolist()
        if clean(value)
    )
    ministry_value = st.sidebar.selectbox("소관부처", ["전체"] + ministries, key=f"{page_key}_ministry")
    if ministry_value != "전체" and "소관부처" in working.columns:
        working = working[working["소관부처"].fillna("").astype(str).str.strip().eq(ministry_value)]

    if search_text:
        working = working[
            core.build_contains_mask(
                working,
                ["공고명", "공고번호", "전문기관", "소관부처", "공고ID", "대표과제명"],
                search_text,
            )
        ]

    filtered = add_viewer_id(add_period_alias(working), kind="notice")

    render_metric_row(
        [
            ("공고 수", str(len(filtered))),
            ("현재 공고", str(int((filtered["is_current"] == "Y").sum()) if "is_current" in filtered.columns else 0)),
            ("전문기관 수", str(filtered["전문기관"].nunique() if "전문기관" in filtered.columns else 0)),
            ("추천 공고", str(int((filtered["대표추천도"] == "추천").sum()) if "대표추천도" in filtered.columns else 0)),
        ]
    )

    st.caption(f"{title} {len(filtered)}건")
    core.render_clickable_table(filtered, NOTICE_COLUMNS, page_key=page_key, id_column="_viewer_id")


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



def normalize_mss_notice_df(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df

    working = df.copy()
    working["registered_at"] = core.series_from_candidates(working, ["registered_at", "ancm_de", "등록일"])
    working["period"] = core.series_from_candidates(working, ["period", "신청기간"])
    working["agency"] = core.series_from_candidates(working, ["agency", "department", "담당부서"])
    working["notice_title"] = core.series_from_candidates(working, ["notice_title", "title", "공고명"])
    working["notice_no"] = core.series_from_candidates(working, ["notice_no", "ancm_no", "공고번호"])
    working["status"] = core.series_from_candidates(working, ["status", "상태", "공고상태"])
    working["views"] = core.series_from_candidates(working, ["views", "조회"])
    working["detail_link"] = core.series_from_candidates(working, ["detail_link", "상세링크"])
    working["review_status"] = core.series_from_candidates(working, ["review_status", "검토 여부", "검토여부"])
    working["notice_id"] = core.series_from_candidates(working, ["notice_id", "공고ID"])
    working["_sort_date"] = core.parse_date_column(working["registered_at"])
    working["등록일"] = working["registered_at"]
    working["신청기간"] = working["period"]
    working["담당부서"] = working["agency"]
    working["공고명"] = working["notice_title"]
    working["공고번호"] = working["notice_no"]
    working["상태"] = working["status"]
    working["조회"] = working["views"]
    working["상세링크"] = working["detail_link"]
    working["검토 여부"] = working["review_status"]
    working["공고ID"] = working["notice_id"]
    working["_source_key"] = "tipa"
    return working.sort_values(by=["_sort_date", "공고번호", "공고명"], ascending=[False, False, True], na_position="last")


def normalize_nipa_notice_df(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df

    working = df.copy()
    working["registered_at"] = core.series_from_candidates(working, ["registered_at", "ancm_de", "등록일"])
    working["period"] = core.series_from_candidates(working, ["period", "신청기간"])
    working["business_name"] = core.series_from_candidates(working, ["business_name", "project_name", "사업명"])
    working["agency"] = core.series_from_candidates(working, ["agency", "department", "담당부서", "전문기관"])
    working["notice_title"] = core.series_from_candidates(working, ["notice_title", "title", "공고명"])
    working["notice_no"] = core.series_from_candidates(working, ["notice_no", "ancm_no", "공고번호", "row_number"])
    working["status"] = core.series_from_candidates(working, ["status", "상태", "공고상태"])
    working["detail_link"] = core.series_from_candidates(working, ["detail_link", "상세링크"])
    working["review_status"] = core.series_from_candidates(working, ["review_status", "검토 여부", "검토여부"])
    working["notice_id"] = core.series_from_candidates(working, ["notice_id", "공고ID"])
    working["d_day"] = core.series_from_candidates(working, ["d_day", "남은신청기간"])
    working["author"] = core.series_from_candidates(working, ["author", "작성자"])
    working["_sort_date"] = core.parse_date_column(working["registered_at"])

    working["등록일"] = working["registered_at"]
    working["신청기간"] = working["period"]
    working["사업명"] = working["business_name"]
    working["담당부서"] = working["agency"]
    working["전문기관"] = working["agency"]
    working["공고명"] = working["notice_title"]
    working["공고번호"] = working["notice_no"]
    working["상태"] = working["status"]
    working["공고상태"] = working["status"]
    working["상세링크"] = working["detail_link"]
    working["검토 여부"] = working["review_status"]
    working["공고ID"] = working["notice_id"]
    working["작성자"] = working["author"]
    working["남은신청기간"] = working["d_day"]
    working["_source_key"] = "nipa"
    return working.sort_values(by=["_sort_date", "공고번호", "공고명"], ascending=[False, False, True], na_position="last")


def load_mss_notice_data() -> tuple[pd.DataFrame, pd.DataFrame]:
    current_sheet = core.get_env("MSS_CURRENT_SHEET", "MSS_CURRENT")
    past_sheet = core.get_env("MSS_PAST_SHEET", "MSS_PAST")
    current_df = normalize_mss_notice_df(core.load_optional_sheet_as_dataframe(current_sheet))
    past_df = normalize_mss_notice_df(core.load_optional_sheet_as_dataframe(past_sheet))
    return current_df, past_df


def load_nipa_notice_data() -> tuple[pd.DataFrame, pd.DataFrame]:
    current_sheet = core.get_env("NIPA_CURRENT_SHEET", "NIPA_CURRENT")
    past_sheet = core.get_env("NIPA_PAST_SHEET", "NIPA_PAST")
    current_df = normalize_nipa_notice_df(core.load_optional_sheet_as_dataframe(current_sheet))
    past_df = normalize_nipa_notice_df(core.load_optional_sheet_as_dataframe(past_sheet))
    return current_df, past_df


def render_source_notice_table(
    df: pd.DataFrame,
    *,
    prefix: str,
    title: str,
    source_label: str,
    display_columns: list[str],
) -> None:
    st.markdown(f"### {title}")
    if df.empty:
        st.info(f"표시할 {source_label} 공고가 없습니다.")
        return

    current_view, selected_notice_id = core.get_route_state(prefix)
    if current_view == "detail":
        selected_row = core.get_row_by_column_value(df, "공고ID", selected_notice_id)
        back_col, info_col = st.columns([1, 5])
        with back_col:
            if st.button("목록으로 돌아가기", key=f"{prefix}_back_to_table", use_container_width=True):
                core.switch_to_table(prefix)
        with info_col:
            st.caption("브라우저 뒤로가기를 눌러도 목록으로 돌아갈 수 있습니다.")
        render_notice_detail(selected_row or {}, pd.DataFrame())
        return

    filtered = df.copy()
    st.sidebar.markdown(f"## {prefix} Filters")
    search_text = st.sidebar.text_input("검색", "", key=f"{prefix}_search")
    agencies = sorted(
        value
        for value in filtered["담당부서"].fillna("").astype(str).str.strip().unique().tolist()
        if clean(value)
    ) if "담당부서" in filtered.columns else []
    agency_value = st.sidebar.selectbox("담당부서", ["전체"] + agencies, key=f"{prefix}_agency")
    statuses = sorted(
        value
        for value in filtered["상태"].fillna("").astype(str).str.strip().unique().tolist()
        if clean(value)
    ) if "상태" in filtered.columns else []
    status_value = st.sidebar.selectbox("상태", ["전체"] + statuses, key=f"{prefix}_status")

    if search_text:
        filtered = filtered[core.build_contains_mask(filtered, ["공고명", "공고번호", "사업명", "담당부서"], search_text)]
    if agency_value != "전체" and "담당부서" in filtered.columns:
        filtered = filtered[filtered["담당부서"].fillna("").astype(str).str.strip().eq(agency_value)]
    if status_value != "전체" and "상태" in filtered.columns:
        filtered = filtered[filtered["상태"].fillna("").astype(str).str.strip().eq(status_value)]

    render_metric_row(
        [
            ("공고 수", str(len(filtered))),
            ("접수중", str(int(filtered["상태"].fillna("").astype(str).str.strip().eq("접수중").sum()) if "상태" in filtered.columns else 0)),
            ("담당부서 수", str(filtered["담당부서"].nunique() if "담당부서" in filtered.columns else 0)),
        ]
    )
    st.caption(f"행을 클릭하면 상세 페이지로 이동합니다. 현재 {len(filtered)}건")
    core.render_clickable_table(filtered, display_columns, page_key=prefix, id_column="공고ID")


def render_mss_table(df: pd.DataFrame, *, prefix: str, title: str) -> None:
    render_source_notice_table(
        df,
        prefix=prefix,
        title=title,
        source_label="MSS",
        display_columns=MSS_COLUMNS,
    )


def render_nipa_table(df: pd.DataFrame, *, prefix: str, title: str) -> None:
    render_source_notice_table(
        df,
        prefix=prefix,
        title=title,
        source_label="NIPA",
        display_columns=NIPA_COLUMNS,
    )


def render_other_crawlers_tab() -> None:
    st.subheader("Other Crawlers")
    st.info("다른 크롤러 소스는 여기에 확장할 수 있습니다.")



def render_mss_tab() -> None:
    st.subheader("중소기업벤처부")
    current_df, past_df = load_mss_notice_data()
    current_page = core.get_query_param("page") or "mss_current"
    page_options = {
        "mss_current": "중소기업벤처부 진행/예정",
        "mss_past": "중소기업벤처부 마감",
    }
    if current_page not in page_options:
        current_page = "mss_current"

    selected_label = st.radio(
        "Page",
        list(page_options.values()),
        horizontal=True,
        index=list(page_options.keys()).index(current_page),
    )
    selected_page = next(page for page, label in page_options.items() if label == selected_label)
    if selected_page != current_page:
        st.query_params.update({"source": "tipa", "page": selected_page, "view": "table"})
        st.rerun()

    if current_page == "mss_past":
        render_mss_table(past_df, prefix="mss_past", title="중소기업벤처부 마감")
    else:
        render_mss_table(current_df, prefix="mss_current", title="중소기업벤처부 진행/예정")


def render_nipa_tab() -> None:
    st.subheader("NIPA")
    current_df, past_df = load_nipa_notice_data()
    current_page = core.get_query_param("page") or "nipa_current"
    page_options = {
        "nipa_current": "NIPA 진행/예정",
        "nipa_past": "NIPA 마감",
    }
    if current_page not in page_options:
        current_page = "nipa_current"

    selected_label = st.radio(
        "Page",
        list(page_options.values()),
        horizontal=True,
        index=list(page_options.keys()).index(current_page),
    )
    selected_page = next(page for page, label in page_options.items() if label == selected_label)
    if selected_page != current_page:
        st.query_params.update({"source": "nipa", "page": selected_page, "view": "table"})
        st.rerun()

    if current_page == "nipa_past":
        render_nipa_table(past_df, prefix="nipa_past", title="NIPA 마감")
    else:
        render_nipa_table(current_df, prefix="nipa_current", title="NIPA 진행/예정")


def normalize_favorite_notice_df(df: pd.DataFrame, *, source_key: str, source_label: str) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame()

    working = df.copy()
    working["매체"] = source_label
    working["_source_key"] = source_key
    working["공고ID"] = core.series_from_candidates(working, ["공고ID", "notice_id"])
    working["공고명"] = core.series_from_candidates(working, ["공고명", "notice_title", "title"])
    working["공고번호"] = core.series_from_candidates(working, ["공고번호", "notice_no", "ancm_no"])
    working["공고일자"] = core.series_from_candidates(working, ["공고일자", "등록일", "registered_at", "ancm_de"])
    working["접수기간"] = core.series_from_candidates(working, ["접수기간", "신청기간", "공고기간", "period"])
    working["전문기관"] = core.series_from_candidates(working, ["전문기관", "전문기관명", "agency"])
    working["담당부서"] = core.series_from_candidates(working, ["담당부서", "department", "agency"])
    working["소관부처"] = core.series_from_candidates(working, ["소관부처", "ministry"])
    working["공고상태"] = core.series_from_candidates(working, ["공고상태", "상태", "status", "rcve_status"])
    working["검토 여부"] = core.series_from_candidates(working, ["검토 여부", "검토여부", "review_status"])
    working["상세링크"] = core.series_from_candidates(working, ["상세링크", "detail_link"])
    working["_favorite_id"] = working.apply(
        lambda row: f"{source_key}::{clean(row.get('공고ID'))}",
        axis=1,
    )
    working["_sort_date"] = core.parse_date_column(working["공고일자"])
    return working


def build_favorite_notice_df(notice_df: pd.DataFrame, opportunity_df: pd.DataFrame) -> pd.DataFrame:
    frames = []
    iris_df = normalize_favorite_notice_df(
        notice_df,
        source_key="iris",
        source_label="IRIS",
    )
    if not iris_df.empty:
        frames.append(iris_df)

    mss_current_df, mss_past_df = load_mss_notice_data()
    mss_df = pd.concat([mss_current_df, mss_past_df], ignore_index=True) if not mss_current_df.empty or not mss_past_df.empty else pd.DataFrame()
    if not mss_df.empty:
        mss_df = mss_df.drop_duplicates(subset=["공고ID"], keep="first")
        frames.append(normalize_favorite_notice_df(mss_df, source_key="tipa", source_label="중소기업벤처부"))

    nipa_current_df, nipa_past_df = load_nipa_notice_data()
    nipa_df = pd.concat([nipa_current_df, nipa_past_df], ignore_index=True) if not nipa_current_df.empty or not nipa_past_df.empty else pd.DataFrame()
    if not nipa_df.empty:
        nipa_df = nipa_df.drop_duplicates(subset=["공고ID"], keep="first")
        frames.append(normalize_favorite_notice_df(nipa_df, source_key="nipa", source_label="NIPA"))

    if not frames:
        return pd.DataFrame()

    combined = pd.concat(frames, ignore_index=True)
    combined = combined[
        combined["검토 여부"].fillna("").astype(str).str.strip().eq(FAVORITE_REVIEW_STATUS)
    ]
    if combined.empty:
        return combined
    return combined.sort_values(
        by=["_sort_date", "매체", "공고명"],
        ascending=[False, True, True],
        na_position="last",
    )


def render_favorite_notice_page(notice_df: pd.DataFrame, opportunity_df: pd.DataFrame) -> None:
    st.subheader("관심 공고")
    page_key = "favorites"
    source_df = build_favorite_notice_df(notice_df, opportunity_df)

    current_view, selected_id = core.get_route_state(page_key)
    if current_view == "detail":
        selected_row = core.get_row_by_column_value(source_df, "_favorite_id", selected_id)
        action_col, info_col = st.columns([1, 5])
        with action_col:
            if st.button("목록으로 돌아가기", key="favorites_back_to_table", use_container_width=True):
                core.switch_to_table(page_key)
        with info_col:
            st.caption("'관심공고'로 지정된 공고만 모아보는 화면입니다.")
        render_notice_detail(selected_row or {}, opportunity_df)
        return

    if source_df.empty:
        st.info("'관심공고'로 지정된 공고가 아직 없습니다.")
        return

    filtered = source_df.copy()
    st.sidebar.markdown("## 관심 공고 Filters")
    search_text = st.sidebar.text_input("통합 검색", "", key="favorites_search")

    sources = sorted(value for value in filtered["매체"].fillna("").astype(str).str.strip().unique().tolist() if clean(value))
    source_value = st.sidebar.selectbox("매체", ["전체"] + sources, key="favorites_source")
    statuses = sorted(value for value in filtered["공고상태"].fillna("").astype(str).str.strip().unique().tolist() if clean(value))
    status_value = st.sidebar.selectbox("공고상태", ["전체"] + statuses, key="favorites_status")

    if search_text:
        filtered = filtered[core.build_contains_mask(filtered, ["공고명", "공고번호", "전문기관", "담당부서", "매체"], search_text)]
    if source_value != "전체":
        filtered = filtered[filtered["매체"].fillna("").astype(str).str.strip().eq(source_value)]
    if status_value != "전체":
        filtered = filtered[filtered["공고상태"].fillna("").astype(str).str.strip().eq(status_value)]

    render_metric_row(
        [
            ("관심 공고 수", str(len(filtered))),
            ("IRIS", str(int(filtered["매체"].fillna("").astype(str).str.strip().eq("IRIS").sum()))),
            ("중소기업벤처부", str(int(filtered["매체"].fillna("").astype(str).str.strip().eq("중소기업벤처부").sum()))),
            ("NIPA", str(int(filtered["매체"].fillna("").astype(str).str.strip().eq("NIPA").sum()))),
        ]
    )
    st.caption(f"행을 클릭하면 상세 페이지로 이동합니다. 현재 {len(filtered)}건")
    core.render_clickable_table(filtered, FAVORITE_COLUMNS, page_key=page_key, id_column="_favorite_id")


def render_detail_page(page: str, notice_df: pd.DataFrame, summary_df: pd.DataFrame, opportunity_df: pd.DataFrame) -> None:
    if page in {"mss_current", "mss_past"}:
        current_df, past_df = load_mss_notice_data()
        source_df = past_df if page == "mss_past" else current_df
        selected_id = core.get_query_param("id")
        row = core.get_row_by_column_value(source_df, "공고ID", selected_id)
        back_label = "중소기업벤처부 마감 목록" if page == "mss_past" else "중소기업벤처부 진행/예정 목록"
        if st.button(back_label, use_container_width=True):
            core.switch_to_table(page)
        render_notice_detail(row or {}, pd.DataFrame())
        return

    if page in {"nipa_current", "nipa_past"}:
        current_df, past_df = load_nipa_notice_data()
        source_df = past_df if page == "nipa_past" else current_df
        selected_id = core.get_query_param("id")
        row = core.get_row_by_column_value(source_df, "공고ID", selected_id)
        back_label = "NIPA 마감 목록" if page == "nipa_past" else "NIPA 진행/예정 목록"
        if st.button(back_label, use_container_width=True):
            core.switch_to_table(page)
        render_notice_detail(row or {}, pd.DataFrame())
        return

    if page in {"notice", "notice_scheduled", "notice_closed"}:
        notice_back_labels = {
            "notice": "진행 공고 테이블로 돌아가기",
            "notice_scheduled": "예정 공고 테이블로 돌아가기",
            "notice_closed": "마감 공고 테이블로 돌아가기",
        }
        if st.button(notice_back_labels.get(page, "테이블로 돌아가기"), use_container_width=True):
            core.switch_to_table(page)
        selected_id = core.get_query_param("id")
        working = add_viewer_id(add_period_alias(notice_df), kind="notice")
        row = core.get_row_by_column_value(working, "_viewer_id", selected_id)
        render_notice_detail(add_period_alias(pd.DataFrame([row])).iloc[0].to_dict() if row else {}, opportunity_df)
        return

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

    if page == "summary":
        if st.button("Summary 테이블로 돌아가기", key="summary_back_to_table", use_container_width=True):
            core.switch_to_table("summary")
        selected_id = core.get_query_param("id")
        working = add_viewer_id(add_period_alias(summary_df), kind="summary")
        row = core.get_row_by_column_value(working, "_viewer_id", selected_id)
        render_summary_detail(add_period_alias(pd.DataFrame([row])).iloc[0].to_dict() if row else {}, opportunity_df)
        return

    if page == "opportunity":
        if st.button("Opportunity 테이블로 돌아가기", key="opportunity_back_to_table", use_container_width=True):
            core.switch_to_table("opportunity")
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
    st.set_page_config(page_title="Crawler Hub", page_icon="IRIS", layout="wide")
    core.inject_page_styles()

    st.title("Crawler Hub")
    st.caption("IRIS / SUMMARY / OPPORTUNITY 시트를 읽기 전용으로 조회합니다.")

    try:
        notice_df, summary_df, opportunity_df = load_viewer_data()
    except Exception as exc:
        st.error(str(exc))
        st.stop()

    current_source = core.get_query_param("source") or "iris"
    source_index_map = {"iris": 0, "tipa": 1, "nipa": 2, "favorites": 3, "other": 4, "other_crawlers": 4}
    source_index = source_index_map.get(current_source, 0)
    selected_source = st.radio(
        "Source",
        ["IRIS", "중소기업벤처부", "NIPA", "관심 공고", "Other Crawlers"],
        horizontal=True,
        index=source_index,
    )
    if selected_source == "IRIS":
        selected_source_key = "iris"
    elif selected_source == "중소기업벤처부":
        selected_source_key = "tipa"
    elif selected_source == "NIPA":
        selected_source_key = "nipa"
    elif selected_source == "관심 공고":
        selected_source_key = "favorites"
    else:
        selected_source_key = "other_crawlers"

    if selected_source_key != current_source:
        if selected_source_key == "tipa":
            default_page = "mss_current"
        elif selected_source_key == "nipa":
            default_page = "nipa_current"
        elif selected_source_key == "favorites":
            default_page = "favorites"
        else:
            default_page = "notice"
        st.query_params.clear()
        st.query_params.update({
            "source": selected_source_key,
            "page": default_page,
            "view": "table",
        })
        st.rerun()

    current_page = core.get_query_param("page") or "notice"
    current_view = core.get_query_param("view") or "table"

    if current_view == "detail":
        render_detail_page(current_page, notice_df, summary_df, opportunity_df)
        return

    if selected_source_key == "tipa":
        render_mss_tab()
        return

    if selected_source_key == "nipa":
        render_nipa_tab()
        return

    if selected_source_key == "favorites":
        render_favorite_notice_page(notice_df, opportunity_df)
        return

    if selected_source_key == "other_crawlers":
        render_other_crawlers_tab()
        return

    st.caption("선택한 화면 하나만 렌더링해서 사이드바를 app과 비슷하게 유지합니다.")
    page_options = {
        "notice": "진행 공고",
        "notice_scheduled": "예정 공고",
        "notice_closed": "마감 공고",
        "summary": "Summary",
        "opportunity": "Opportunity",
    }
    if current_page not in page_options:
        current_page = "notice"

    selected_label = st.radio(
        "Page",
        list(page_options.values()),
        horizontal=True,
        index=list(page_options.keys()).index(current_page),
    )
    selected_page = next(page for page, label in page_options.items() if label == selected_label)
    if selected_page != current_page:
        st.query_params.update({"source": "iris", "page": selected_page, "view": "table"})
        st.rerun()

    if current_page == "notice_scheduled":
        render_notice_table_with_scope(
            notice_df,
            opportunity_df,
            page_key="notice_scheduled",
            title="예정 공고",
            status_scope="예정",
            current_only_default=True,
        )
    elif current_page == "notice_closed":
        render_notice_table_with_scope(
            notice_df,
            opportunity_df,
            page_key="notice_closed",
            title="마감 공고",
            status_scope="마감",
            current_only_default=False,
        )
    elif current_page == "summary":
        render_summary_table(summary_df)
    elif current_page == "opportunity":
        render_opportunity_table(opportunity_df)
    else:
        render_notice_table_with_scope(
            notice_df,
            opportunity_df,
            page_key="notice",
            title="진행 공고",
            status_scope="접수중",
            current_only_default=True,
        )


if __name__ == "__main__":
    main()
