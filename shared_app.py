import json
import hashlib
import hmac
import os
import re
import sys
import time
import base64
import uuid
from html import escape
from pathlib import Path
from urllib.parse import urlencode

import gspread
import pandas as pd
import streamlit as st
from pages.dashboard import render_page as render_dashboard_page_module
from pages.notice_detail import render_page as render_notice_detail_page_module
from pages.notice_queue import render_page as render_notice_queue_page_module
from pages.notice_queue import render_source as render_notice_queue_source_module
from pages.rfp_detail import render_page as render_rfp_detail_page_module
from pages.rfp_queue import render_page as render_rfp_queue_page_module
from app_config import (
    AppModeConfig,
    SourcePageConfig,
    SourceRouteConfig,
    build_app_mode_config,
    find_nav_group_for_route,
    get_default_page_for_source,
    get_source_config_map,
)
from core import routing as route_core
from dotenv import load_dotenv
from google.oauth2.service_account import Credentials

try:
    from jobs_mss.crawl_mss_list import crawl_mss_list
except Exception:
    crawl_mss_list = None

try:
    from jobs_nipa.crawl_nipa_list import crawl_nipa_list
except Exception:
    crawl_nipa_list = None


BASE_DIR = Path(__file__).resolve().parent
SCOPE = ["https://www.googleapis.com/auth/spreadsheets"]
DEFAULT_PAGE_SIZE = 300
REVIEW_OPTIONS = ["", "검토전", "관심공고", "보류", "완료", "검토완료"]
FAVORITE_REVIEW_STATUS = "관심공고"
ARCHIVE_REVIEW_STATUS_VALUES = {"완료", "검토완료"}


def _resolve_sheet_name(
    *,
    current_keys: tuple[str, ...],
    legacy_keys: tuple[str, ...] = (),
    default_name: str,
    legacy_default_names: tuple[str, ...] = (),
    getter=None,
) -> str:
    getter = getter or (lambda key: clean(os.getenv(key)))

    for key in current_keys:
        value = clean(getter(key))
        if value:
            return value

    for key in legacy_keys:
        value = clean(getter(key))
        if not value:
            continue
        if value in legacy_default_names:
            return default_name
        return value

    return default_name


def resolve_mss_opportunity_current_sheet(getter=None) -> str:
    return _resolve_sheet_name(
        current_keys=("MSS_OPPORTUNITY_CURRENT_SHEET",),
        legacy_keys=("MSS_OPPORTUNITY_MASTER_SHEET",),
        default_name="MSS_OPPORTUNITY_CURRENT",
        legacy_default_names=("MSS_OPPORTUNITY_MASTER",),
        getter=getter,
    )


def resolve_mss_opportunity_archive_sheet(getter=None) -> str:
    return _resolve_sheet_name(
        current_keys=("MSS_OPPORTUNITY_ARCHIVE_SHEET",),
        default_name="MSS_OPPORTUNITY_ARCHIVE",
        getter=getter,
    )


def resolve_nipa_opportunity_current_sheet(getter=None) -> str:
    return _resolve_sheet_name(
        current_keys=("NIPA_OPPORTUNITY_CURRENT_SHEET",),
        legacy_keys=("NIPA_OPPORTUNITY_MASTER_SHEET",),
        default_name="NIPA_OPPORTUNITY_CURRENT",
        legacy_default_names=("NIPA_OPPORTUNITY_MASTER",),
        getter=getter,
    )


def resolve_nipa_opportunity_archive_sheet(getter=None) -> str:
    return _resolve_sheet_name(
        current_keys=("NIPA_OPPORTUNITY_ARCHIVE_SHEET",),
        default_name="NIPA_OPPORTUNITY_ARCHIVE",
        getter=getter,
    )


def resolve_canonical_notice_master_sheet(getter=None) -> str:
    return _resolve_sheet_name(
        current_keys=("CANONICAL_NOTICE_MASTER_SHEET", "NOTICE_UNIFIED_MASTER_SHEET"),
        default_name="NOTICE_MASTER",
        getter=getter,
    )


def resolve_notice_current_view_sheet(getter=None) -> str:
    return _resolve_sheet_name(
        current_keys=("NOTICE_CURRENT_VIEW_SHEET",),
        default_name="NOTICE_CURRENT_VIEW",
        getter=getter,
    )


def resolve_notice_pending_view_sheet(getter=None) -> str:
    return _resolve_sheet_name(
        current_keys=("NOTICE_PENDING_VIEW_SHEET",),
        default_name="NOTICE_PENDING_VIEW",
        getter=getter,
    )


def resolve_notice_archive_view_sheet(getter=None) -> str:
    return _resolve_sheet_name(
        current_keys=("NOTICE_ARCHIVE_VIEW_SHEET",),
        default_name="NOTICE_ARCHIVE_VIEW",
        getter=getter,
    )


def resolve_iris_opportunity_current_sheet(getter=None) -> str:
    return _resolve_sheet_name(
        current_keys=("IRIS_OPPORTUNITY_CURRENT_SHEET", "OPPORTUNITY_CURRENT_SHEET"),
        legacy_keys=("IRIS_OPPORTUNITY_MASTER_SHEET", "OPPORTUNITY_MASTER_SHEET", "MASTER_SHEET"),
        default_name="IRIS_OPPORTUNITY_CURRENT",
        legacy_default_names=("IRIS_OPPORTUNITY_MASTER", "OPPORTUNITY_MASTER"),
        getter=getter,
    )


def resolve_iris_opportunity_archive_sheet(getter=None) -> str:
    return _resolve_sheet_name(
        current_keys=("IRIS_OPPORTUNITY_ARCHIVE_SHEET", "OPPORTUNITY_ARCHIVE_SHEET", "ARCHIVE_SHEET"),
        default_name="IRIS_OPPORTUNITY_ARCHIVE",
        legacy_default_names=("OPPORTUNITY_ARCHIVE",),
        getter=getter,
    )


NOTICE_PREFERRED_COLUMNS = [
    "공고일자",
    "접수기간",
    "전문기관",
    "공고명",
    "공고상태",
    "검토 여부",
    "상세링크",
]

PENDING_PREFERRED_COLUMNS = [
    "공고일자",
    "접수기간",
    "전문기관",
    "공고명",
    "공고상태",
    "검토 여부",
    "공고ID",
    "소관부처",
    "공고번호",
    "상태키",
    "is_current",
]

OPPORTUNITY_PREFERRED_COLUMNS = [
    "공고일자",
    "접수기간",
    "전문기관명",
    "공고명",
    "해당 과제명",
    "추천여부",
    "점수",
    "예산",
    "공고상태",
    "archive_reason_label",
    "검토여부",
    "상세링크",
]

SUMMARY_PREFERRED_COLUMNS = [
    "공고일자",
    "공고번호",
    "전문기관",
    "공고명",
    "공고상태",
    "접수기간",
    "추천도 및 점수",
    "해당 과제명",
    "예산",
    "검토 여부",
    "공고ID",
    "소관부처",
    "대표점수",
    "대표추천도",
    "과제수",
    "문서수",
    "is_current",
]

ERROR_PREFERRED_COLUMNS = [
    "source_site",
    "notice_id",
    "notice_title",
    "project_name",
    "rfp_title",
    "file_name",
    "validation_errors",
    "llm_error",
    "parse_error",
    "updated_at",
]

CLOSED_STATUS_VALUES = {
    "마감",
    "종료",
    "closed",
    "end",
    "ended",
    "접수마감",
    "접수 마감",
    "신청마감",
    "신청 마감",
    "공고마감",
    "공고 마감",
}
OPEN_STATUS_MARKERS = ("접수중", "진행중", "공고중", "마감임박", "예정")

MSS_VIEW_COLUMNS = [
    "등록일",
    "신청기간",
    "담당부서",
    "공고명",
    "상세링크",
    "공고번호",
    "상태",
    "검토 여부",
]

NIPA_VIEW_COLUMNS = [
    "등록일",
    "신청기간",
    "사업명",
    "공고명",
    "상세링크",
    "공고번호",
    "상태",
    "검토 여부",
]

FAVORITE_NOTICE_COLUMNS = [
    "매체",
    "공고일자",
    "접수기간",
    "전문기관",
    "담당부서",
    "공고명",
    "상세링크",
    "공고번호",
    "공고상태",
    "검토 여부",
]

COMMENT_COLUMNS = [
    "comment_id",
    "post_id",
    "user_id",
    "source",
    "notice_id",
    "notice_title",
    "parent_id",
    "nickname",
    "content",
    "mention",
    "ip_address",
    "ip_based_uid",
    "created_at",
    "updated_at",
    "deleted_at",
]

USER_REVIEW_COLUMNS = [
    "user_id",
    "source",
    "notice_id",
    "notice_title",
    "review_status",
    "updated_at",
]

AUTH_USER_COLUMNS = [
    "user_id",
    "password_hash",
    "display_name",
    "email",
    "role",
    "status",
    "requested_at",
    "approved_at",
    "approved_by",
    "rejected_at",
    "rejected_by",
]
SIGNUP_REQUEST_COLUMNS = [
    "request_id",
    "requested_at",
    "name",
    "email",
    "organization",
    "account_type",
    "request_note",
    "status",
    "admin_note",
    "reviewed_at",
    "reviewed_by",
]

SIGNUP_STATUS_OPTIONS = ["PENDING", "APPROVED", "REJECTED", "HOLD"]

def clean(value) -> str:
    if value is None:
        return ""
    try:
        if pd.isna(value):
            return ""
    except Exception:
        pass
    return str(value).strip()


def is_retryable_gspread_api_error(exc: Exception) -> bool:
    api_error_cls = getattr(getattr(gspread, "exceptions", None), "APIError", None)
    if api_error_cls is None or not isinstance(exc, api_error_cls):
        return False

    response = getattr(exc, "response", None)
    status_code = getattr(response, "status_code", None)
    response_text = ""
    if response is not None:
        response_text = clean(getattr(response, "text", "")) or clean(response)
    message = f"{response_text} {clean(exc)}".lower()
    if status_code in {429, 500, 502, 503, 504}:
        return True
    return any(
        marker in message
        for marker in [
            "quota",
            "rate limit",
            "resource exhausted",
            "backend error",
            "internal error",
            "try again later",
        ]
    )


def run_gspread_call(operation, *args, **kwargs):
    last_error: Exception | None = None
    for attempt in range(3):
        try:
            return operation(*args, **kwargs)
        except Exception as exc:
            last_error = exc
            if not is_retryable_gspread_api_error(exc) or attempt == 2:
                raise
            time.sleep(1.0 * (attempt + 1))
    if last_error is not None:
        raise last_error
    raise RuntimeError("Google Sheets call failed without an explicit error.")


def render_iris_page(page_key: str, datasets: dict[str, pd.DataFrame]) -> None:
    if page_key in {"opportunity", "rfp_queue"}:
        render_opportunity_page(
            datasets["opportunity"],
            page_key="rfp_queue",
            all_df=datasets["opportunity_all"],
        )
    elif page_key == "summary":
        render_summary_page(datasets["summary"], datasets["opportunity"])
    elif page_key in {"notice", "notice_queue"}:
        render_notice_page_with_scope(
            datasets["notice_view"],
            datasets["opportunity"],
            page_key="notice_queue",
            title="Notice Queue",
            default_status_scope="접수중",
            current_only_default=True,
        )
    elif page_key == "notice_scheduled":
        render_notice_page_with_scope(
            datasets["notice_view"],
            datasets["opportunity"],
            page_key="notice_scheduled",
            title="예정 공고",
            default_status_scope="?덉젙",
            current_only_default=True,
        )
    elif page_key == "notice_archive":
        render_notice_page_with_scope(
            datasets["notice_view"],
            datasets["opportunity"],
            page_key="notice_archive",
            title="Archive",
            default_status_scope="?꾩껜",
            current_only_default=False,
            archive=True,
        )


def build_dashboard_notice_index(
    datasets: dict[str, pd.DataFrame],
    source_datasets: dict[str, object] | None,
    *,
    archived: bool = False,
) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []

    def append_source(source_df: pd.DataFrame, *, source_key: str, source_label: str) -> None:
        if source_df is None or source_df.empty:
            return

        working = source_df.copy()
        normalized = pd.DataFrame(index=working.index.copy())
        normalized["source_key"] = source_key
        normalized["Source"] = source_label
        normalized["Notice ID"] = series_from_candidates(working, ["notice_id", "공고ID"])
        normalized["Title"] = series_from_candidates(working, ["notice_title", "title", "공고명"])
        normalized["Notice No"] = series_from_candidates(working, ["notice_no", "ancm_no", "공고번호"])
        normalized["Status"] = series_from_candidates(working, ["notice_status", "rcve_status", "status", "공고상태"])
        normalized["Period"] = series_from_candidates(working, ["notice_period", "period", "접수기간", "신청기간"])
        normalized["Review"] = series_from_candidates(working, ["review_status", "검토 여부", "검토여부"])
        normalized["Agency"] = series_from_candidates(working, ["agency", "전문기관", "담당부서"])
        normalized["Ministry"] = series_from_candidates(working, ["ministry", "소관부처", "주관부처"])
        normalized["Date"] = series_from_candidates(working, ["registered_at", "ancm_de", "공고일자"])
        normalized["Detail Link"] = working.apply(
            lambda row: resolve_external_detail_link(row, source_key=source_key),
            axis=1,
        )
        normalized["_sort_date"] = parse_date_column(normalized["Date"])
        frames.append(normalized)

    iris_df = datasets["notice_view"]
    iris_df = filter_archived_notice_rows(iris_df) if archived else filter_current_notice_rows(iris_df)
    append_source(iris_df, source_key="iris", source_label="IRIS")

    if source_datasets:
        tipa_base = combine_notice_frames(source_datasets["mss_current"], source_datasets["mss_past"])
        tipa_df = filter_archived_notice_rows(tipa_base) if archived else filter_current_notice_rows(source_datasets["mss_current"])
        append_source(tipa_df, source_key="tipa", source_label="以묒냼湲곗뾽踰ㅼ쿂遺")

        nipa_base = combine_notice_frames(source_datasets["nipa_current"], source_datasets["nipa_past"])
        nipa_df = filter_archived_notice_rows(nipa_base) if archived else filter_current_notice_rows(source_datasets["nipa_current"])
        append_source(nipa_df, source_key="nipa", source_label="NIPA")

    if not frames:
        return pd.DataFrame(
            columns=[
                "source_key",
                "Source",
                "Notice ID",
                "Title",
                "Notice No",
                "Status",
                "Period",
                "Review",
                "Agency",
                "Ministry",
                "Date",
                "Detail Link",
                "_sort_date",
            ]
        )

    combined = pd.concat(frames, ignore_index=True)
    return combined.sort_values(
        by=["_sort_date", "Source", "Title"],
        ascending=[False, True, True],
        na_position="last",
    )


def build_dashboard_source_snapshot_rows(
    datasets: dict[str, pd.DataFrame],
    source_datasets: dict[str, object] | None,
) -> pd.DataFrame:
    current_notice_index = build_dashboard_notice_index(datasets, source_datasets, archived=False)
    archive_notice_index = build_dashboard_notice_index(datasets, source_datasets, archived=True)

    opportunity_map: dict[str, pd.DataFrame] = {
        "iris": datasets["opportunity"],
        "tipa": source_datasets["mss_opportunity"] if source_datasets else pd.DataFrame(),
        "nipa": source_datasets["nipa_opportunity"] if source_datasets else pd.DataFrame(),
    }
    source_labels = {"iris": "IRIS", "tipa": "以묒냼湲곗뾽踰ㅼ쿂遺", "nipa": "NIPA"}

    rows: list[dict[str, object]] = []
    for source_key, source_label in source_labels.items():
        current_slice = current_notice_index[current_notice_index["source_key"].eq(source_key)].copy()
        archive_slice = archive_notice_index[archive_notice_index["source_key"].eq(source_key)].copy()
        review_needed = int(current_slice["Review"].fillna("").astype(str).str.strip().eq("").sum()) if not current_slice.empty else 0

        opportunity_df = opportunity_map.get(source_key, pd.DataFrame())
        current_opportunities = len(filter_current_opportunity_rows(opportunity_df)) if not opportunity_df.empty else 0
        archived_opportunities = len(filter_archived_opportunity_rows(opportunity_df)) if not opportunity_df.empty else 0

        rows.append(
            {
                "Source": source_label,
                "Current Notices": int(len(current_slice)),
                "Archived Notices": int(len(archive_slice)),
                "Review Needed": review_needed,
                "Current Opportunities": int(current_opportunities),
                "Archived Opportunities": int(archived_opportunities),
            }
        )

    return pd.DataFrame(rows)


def build_dashboard_notice_table(df: pd.DataFrame, *, limit: int = 8) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame(columns=["Source", "Title", "Status", "Period", "Review", "Date"])

    working = df.copy()
    if "_sort_date" in working.columns:
        working = working.sort_values(by=["_sort_date", "Source", "Title"], ascending=[False, True, True], na_position="last")

    view = working[["Source", "Title", "Status", "Period", "Review", "Date"]].head(limit).copy()
    for column in ["Title", "Period"]:
        view[column] = view[column].apply(lambda value: compact_table_value(value, max_chars=48))
    return view


def build_dashboard_trend_chart(
    df: pd.DataFrame,
    *,
    date_column: str = "Date",
    category_column: str = "Source",
    days: int = 14,
) -> pd.DataFrame:
    if df.empty or date_column not in df.columns or category_column not in df.columns:
        return pd.DataFrame()

    working = df.copy()
    working["_chart_date"] = parse_date_column(working[date_column]).dt.normalize()
    working = working.dropna(subset=["_chart_date"])
    if working.empty:
        return pd.DataFrame()

    end_date = pd.Timestamp.now().normalize()
    start_date = end_date - pd.Timedelta(days=max(days - 1, 0))
    working = working[working["_chart_date"].ge(start_date)]
    grouped = (
        working.groupby(["_chart_date", category_column])
        .size()
        .unstack(fill_value=0)
        .sort_index()
    )
    if grouped.empty:
        return pd.DataFrame()

    all_dates = pd.date_range(start=start_date, end=end_date, freq="D")
    grouped = grouped.reindex(all_dates, fill_value=0)
    grouped.index.name = "Date"
    return grouped


def build_dashboard_source_count_chart(snapshot_rows: pd.DataFrame, column: str) -> pd.DataFrame:
    if snapshot_rows.empty or column not in snapshot_rows.columns:
        return pd.DataFrame()
    chart_df = snapshot_rows[["Source", column]].copy()
    chart_df = chart_df.set_index("Source")
    chart_df.columns = ["Count"]
    return chart_df


def build_dashboard_status_chart(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty or "Status" not in df.columns:
        return pd.DataFrame()
    status_counts = (
        df["Status"]
        .fillna("")
        .astype(str)
        .apply(normalize_notice_status_label)
        .replace("", "미지정")
        .value_counts()
        .rename_axis("Status")
        .to_frame("Count")
    )
    return status_counts


def build_dashboard_review_chart(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame()
    review_values = df["Review"].fillna("").astype(str).str.strip() if "Review" in df.columns else pd.Series("", index=df.index)
    reviewed = int(review_values.ne("").sum())
    pending = int(review_values.eq("").sum())
    return pd.DataFrame(
        {"Count": [reviewed, pending]},
        index=["검토완료", "미검토"],
    )


def build_dashboard_opportunity_index(
    datasets: dict[str, pd.DataFrame],
    source_datasets: dict[str, object] | None,
) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []

    def append_source(df: pd.DataFrame, *, source_key: str, source_label: str) -> None:
        if df is None or df.empty:
            return
        working = filter_current_opportunity_rows(df)
        if working.empty:
            return

        normalized = pd.DataFrame(index=working.index.copy())
        normalized["source_key"] = source_key
        normalized["Source"] = source_label
        normalized["Notice ID"] = series_from_candidates(working, ["notice_id", "怨듦퀬ID"])
        normalized["Notice Title"] = series_from_candidates(working, ["notice_title", "공고명"])
        normalized["Project"] = series_from_candidates(working, ["project_name", "해당 과제명", "llm_project_name"])
        normalized["Recommendation"] = series_from_candidates(working, ["recommendation", "異붿쿇?щ?", "llm_recommendation"])
        normalized["Score"] = to_numeric_column(series_from_candidates(working, ["rfp_score", "?먯닔", "llm_fit_score"]))
        normalized["Budget"] = series_from_candidates(working, ["budget", "?덉궛", "llm_total_budget_text", "total_budget_text"])
        normalized["Reason"] = series_from_candidates(working, ["llm_reason", "reason", "권고사유"])
        normalized["Date"] = series_from_candidates(working, ["ancm_de", "공고일자", "registered_at"])
        normalized["_sort_date"] = parse_date_column(normalized["Date"])
        frames.append(normalized)

    append_source(datasets["opportunity"], source_key="iris", source_label="IRIS")
    if source_datasets:
        append_source(source_datasets["mss_opportunity"], source_key="tipa", source_label="중소기업벤처부")
        append_source(source_datasets["nipa_opportunity"], source_key="nipa", source_label="NIPA")

    if not frames:
        return pd.DataFrame(columns=["source_key", "Source", "Notice ID", "Notice Title", "Project", "Recommendation", "Score", "Budget", "Reason", "Date", "_sort_date"])

    combined = pd.concat(frames, ignore_index=True)
    return combined.sort_values(
        by=["Score", "_sort_date", "Project"],
        ascending=[False, False, True],
        na_position="last",
    )


def build_dashboard_opportunity_table(df: pd.DataFrame, *, limit: int = 8) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame(columns=["Source", "Project", "Recommendation", "Score", "Budget"])
    view = df[["Source", "Project", "Recommendation", "Score", "Budget"]].head(limit).copy()
    view["Project"] = view["Project"].apply(lambda value: compact_table_value(value, max_chars=42))
    view["Budget"] = view["Budget"].apply(lambda value: compact_table_value(value, max_chars=24))
    return view


def build_dashboard_deadline_table(df: pd.DataFrame, *, limit: int = 8) -> pd.DataFrame:
    if df.empty or "Period" not in df.columns:
        return pd.DataFrame(columns=["Source", "Title", "Period", "D-Day"])

    working = df.copy()
    working["_period_end"] = working["Period"].apply(extract_period_end)
    working = working.dropna(subset=["_period_end"])
    if working.empty:
        return pd.DataFrame(columns=["Source", "Title", "Period", "D-Day"])

    today = pd.Timestamp.now().normalize()
    working["D-Day"] = (working["_period_end"].dt.normalize() - today).dt.days
    working = working[working["D-Day"].ge(0)]
    if working.empty:
        return pd.DataFrame(columns=["Source", "Title", "Period", "D-Day"])

    working = working.sort_values(by=["D-Day", "_sort_date"], ascending=[True, False], na_position="last")
    view = working[["Source", "Title", "Period", "D-Day"]].head(limit).copy()
    view["Title"] = view["Title"].apply(lambda value: compact_table_value(value, max_chars=38))
    return view


def build_dashboard_recent_comments_table(limit: int = 5) -> pd.DataFrame:
    try:
        comments_df = load_notice_comments()
    except Exception:
        return pd.DataFrame(columns=["작성시각", "작성자", "댓글"])

    if comments_df.empty:
        return pd.DataFrame(columns=["작성시각", "작성자", "댓글"])

    recent = comments_df.copy()
    if is_user_scoped_operations_enabled() and "user_id" in recent.columns:
        recent = recent[recent["user_id"].fillna("").astype(str).str.strip().eq(get_current_operation_scope_key())].copy()
    recent = recent.head(limit).copy()
    if recent.empty:
        return pd.DataFrame(columns=["작성시각", "작성자", "댓글"])
    recent["작성자"] = series_from_candidates(recent, ["nickname", "user_id", "author"])
    recent["댓글"] = series_from_candidates(recent, ["content", "comment"]).apply(lambda value: compact_table_value(value, max_chars=42))
    return recent.rename(
        columns={
            "created_at": "작성시각",
        }
    )[["작성시각", "작성자", "댓글"]]


def render_dashboard_chart_block(title: str, chart_df: pd.DataFrame, *, chart_type: str = "bar") -> None:
    st.markdown(f"### {title}")
    if chart_df.empty:
        st.info("?쒖떆??곗씠?곌? ?놁뒿?덈떎.")
        return

    if chart_type == "line":
        st.line_chart(chart_df, use_container_width=True)
    elif chart_type == "area":
        st.area_chart(chart_df, use_container_width=True)
    else:
        st.bar_chart(chart_df, use_container_width=True)


def render_dashboard_table_block(title: str, df: pd.DataFrame) -> None:
    st.markdown(f"### {title}")
    if df.empty:
        st.info("?쒖떆??곗씠?곌? ?놁뒿?덈떎.")
        return
    st.dataframe(df, use_container_width=True, hide_index=True)


def build_dashboard_notice_route(source_key: object, notice_id: object) -> str:
    source = clean(source_key).lower()
    notice_id_text = clean(notice_id)
    if not notice_id_text:
        return ""
    page_map = {
        "iris": "notice",
        "tipa": "tipa_current",
        "nipa": "nipa_current",
    }
    page_key = page_map.get(source, "notice")
    params = {
        "source": source or "iris",
        "page": page_key,
        "view": "detail",
        "id": notice_id_text,
    }
    params = apply_return_route(params)
    return f"?{urlencode(params)}"


def render_dashboard_metrics_strip(items: list[tuple[str, str, str]]) -> None:
    render_metrics([(label, value) for label, value, _caption in items])


def render_dashboard_rank_list(
    title: str,
    rows: list[dict[str, str]],
    *,
    empty_message: str = "?쒖떆??곗씠?곌? ?놁뒿?덈떎.",
) -> None:
    st.markdown(f"### {title}")
    if not rows:
        st.info(empty_message)
        return

    item_html = []
    for index, row in enumerate(rows, start=1):
        title_text = clean(row.get("title"))
        title_html = f'<span class="dashboard-rank-title">{escape(title_text)}</span>'

        badges = []
        for badge_text in row.get("badges", []):
            badge_value = clean(badge_text)
            if badge_value:
                badges.append(f'<span class="dashboard-rank-badge">{escape(badge_value)}</span>')

        meta_parts = []
        left_meta = clean(row.get("meta_left"))
        right_meta = clean(row.get("meta_right"))
        if left_meta:
            meta_parts.append(f'<span>{escape(left_meta)}</span>')
        if right_meta:
            meta_parts.append(f'<span>{escape(right_meta)}</span>')

        item_html.append(
            """
            <div class="dashboard-rank-row">
              <div class="dashboard-rank-order">{order}</div>
              <div class="dashboard-rank-main">
                <div class="dashboard-rank-head">
                  {title_html}
                  <div class="dashboard-rank-badges">{badges}</div>
                </div>
                <div class="dashboard-rank-meta">{meta}</div>
              </div>
              <div class="dashboard-rank-value">{value}</div>
            </div>
            """.format(
                order=index,
                title_html=title_html,
                badges="".join(badges),
                meta="".join(meta_parts),
                value=escape(clean(row.get("value"))),
            )
        )

    st.markdown(
        '<div class="dashboard-rank-list">{}</div>'.format("".join(item_html)),
        unsafe_allow_html=True,
    )


def build_notice_rank_rows(df: pd.DataFrame, *, limit: int = 8, value_column: str = "Status") -> list[dict[str, str]]:
    if df.empty:
        return []

    rows: list[dict[str, str]] = []
    for _, row in df.head(limit).iterrows():
        rows.append(
            {
                "title": compact_table_value(row.get("Title"), max_chars=44),
                "href": build_dashboard_notice_route(row.get("source_key"), row.get("Notice ID")),
                "badges": [row.get("Source")],
                "meta_left": compact_table_value(row.get("Period"), max_chars=34),
                "meta_right": row.get("Date"),
                "value": row.get(value_column),
            }
        )
    return rows


def build_deadline_rank_rows(df: pd.DataFrame, *, limit: int = 8) -> list[dict[str, str]]:
    if df.empty:
        return []
    rows: list[dict[str, str]] = []
    for _, row in df.head(limit).iterrows():
        rows.append(
            {
                "title": compact_table_value(row.get("Title"), max_chars=40),
                "badges": [row.get("Source")],
                "meta_left": compact_table_value(row.get("Period"), max_chars=34),
                "meta_right": "",
                "value": f"D-{clean(row.get('D-Day'))}",
            }
        )
    return rows


def build_opportunity_rank_rows(df: pd.DataFrame, *, limit: int = 8) -> list[dict[str, str]]:
    if df.empty:
        return []
    rows: list[dict[str, str]] = []
    for _, row in df.head(limit).iterrows():
        rows.append(
            {
                "title": compact_table_value(row.get("Project"), max_chars=40),
                "badges": [row.get("Source"), row.get("Recommendation")],
                "meta_left": compact_table_value(row.get("Budget"), max_chars=30),
                "meta_right": row.get("Date"),
                "value": str(row.get("Score")),
            }
        )
    return rows


def build_comment_rank_rows(df: pd.DataFrame, *, limit: int = 5) -> list[dict[str, str]]:
    if df.empty:
        return []
    rows: list[dict[str, str]] = []
    for _, row in df.head(limit).iterrows():
        rows.append(
            {
                "title": compact_table_value(row.get("댓글"), max_chars=44),
                "badges": [row.get("작성자")],
                "meta_left": row.get("작성시각"),
                "meta_right": "",
                "value": "",
            }
        )
    return rows


def navigate_to_source_page(source_key: str, page_key: str) -> None:
    normalized_page = normalize_route_page_key(page_key)
    normalized_source = clean(source_key)
    if normalized_page == "notice_queue":
        normalized_source = "notices"
    elif normalized_page == "favorites":
        normalized_source = "favorites"
    elif normalized_page == "dashboard":
        normalized_source = "dashboard"
    route = route_core.normalize_route(
        {
            "source": normalized_source,
            "page": normalized_page,
            "view": "list",
            "source_key": normalized_source,
        }
    )
    route_core.navigate_to(route, push=True)
    replace_query_params(with_auth_params(route_core.serialize_route(route)))
    st.rerun()


def navigate_to_route(source_key: str, page_key: str) -> None:
    navigate_to_source_page(source_key, page_key)


def navigate_to_route_state(route: dict, *, push: bool = True) -> None:
    normalized = route_core.navigate_to(route, push=push)
    replace_query_params(with_auth_params(route_core.serialize_route(normalized)))
    st.rerun()


def navigate_to_notice_detail(source_key: str, notice_id: str) -> None:
    navigate_to_route_state(route_core.build_notice_detail_route(clean(notice_id), source_key=source_key), push=True)


def navigate_to_opportunity_detail(source_key: str, row_id: str) -> None:
    navigate_to_route_state(route_core.build_rfp_detail_route(clean(row_id), source_key=source_key), push=True)


def render_nav_tabs(current_key: str, options: list[tuple[str, str]], *, key: str, label: str = "Navigation") -> str:
    option_map = {option_key: option_label for option_key, option_label in options}
    if current_key not in option_map:
        current_key = next(iter(option_map))
    if clean(label):
        st.markdown(
            f'<div class="section-label" style="margin-top:6px">{escape(clean(label))}</div>',
            unsafe_allow_html=True,
        )
    st.markdown('<div class="workspace-nav-shell">', unsafe_allow_html=True)
    cols = st.columns(len(options))
    selected_key = current_key
    for col, (option_key, option_label) in zip(cols, options):
        with col:
            button_type = "primary" if option_key == current_key else "secondary"
            if st.button(
                option_label,
                key=f"{key}_{option_key}",
                type=button_type,
                use_container_width=True,
            ):
                selected_key = option_key
    st.markdown('</div>', unsafe_allow_html=True)
    return selected_key


def render_dashboard_quick_links(mode_config: AppModeConfig) -> None:
    st.markdown("### Quick Links")

    primary_links = [
        ("IRIS 吏꾪뻾", "iris", "notice"),
        ("IRIS Opportunity", "iris", "opportunity"),
        ("以묒냼湲곗뾽踰ㅼ쿂遺 吏꾪뻾", "tipa", "tipa_current"),
        ("NIPA 吏꾪뻾", "nipa", "nipa_current"),
    ]
    secondary_links = [("愿??怨듦퀬", "favorites", "favorites")]
    if "summary" in mode_config.valid_iris_pages:
        secondary_links.insert(0, ("IRIS Summary", "iris", "summary"))

    for row_index, link_specs in enumerate([primary_links, secondary_links]):
        if not link_specs:
            continue
        cols = st.columns(len(link_specs))
        for col, (label, source_key, page_key) in zip(cols, link_specs):
            with col:
                if st.button(label, key=f"dashboard_link_{row_index}_{source_key}_{page_key}", use_container_width=True):
                    navigate_to_source_page(source_key, page_key)


def render_grant_search_dashboard_intro(
    *,
    source_count: int,
    agency_count: int,
    notice_count: int,
) -> None:
    st.markdown(
        f"""
        <div class="grant-search-header">
          <div class="grant-search-brand-row">
            <div class="grant-search-brand">?뺣? 怨쇱젣 異붿쿇</div>
            <div class="grant-search-divider"></div>
            <div class="grant-search-nav">
              <span class="active">怨쇱젣 寃??/span>
              <span>留욎땄 異붿쿇</span>
            </div>
          </div>
          <div class="grant-search-auth">
            <span>濡쒓렇??/span>
            <span>?뚯썝媛??/span>
          </div>
        </div>
        <div class="grant-search-hero">
          <div class="grant-search-title">?먰븯??뺣? 怨쇱젣瑜?寃?됲븯怨??꾪꽣瑜??곸슜?대낫?몄슂</div>
          <div class="grant-search-subtitle">{source_count}媛?遺泥?쨌 {agency_count:,}媛??섑뻾湲곌? 쨌 {notice_count:,}媛?怨듦퀬 ?ъ씠??湲곕컲 ?ㅼ떆媛??낅뜲?댄듃</div>
        </div>
        """,
        unsafe_allow_html=True,
    )

    st.markdown('<div class="grant-search-shell">', unsafe_allow_html=True)
    with st.form("grant_dashboard_search_form"):
        search_col, button_col = st.columns([10, 1])
        with search_col:
            search_text = st.text_area(
                "과제 검색",
                key="grant_dashboard_search_text",
                placeholder="예: 과제명, 사업 분야, 기술/연구 키워드를 입력해 필요한 과제를 찾아보세요.",
                label_visibility="collapsed",
                height=210,
            )
        with button_col:
            st.markdown('<div class="grant-search-button-wrap">', unsafe_allow_html=True)
            submitted = st.form_submit_button("검색", use_container_width=True)
            st.markdown("</div>", unsafe_allow_html=True)
    st.markdown("</div>", unsafe_allow_html=True)

    if submitted:
        st.session_state["sidebar_search"] = clean(search_text)
        navigate_to_route("iris", "notice_queue")

    st.markdown(
        """
        <div class="grant-filter-head">
          <div class="grant-filter-title">?붽굔 / ?꾪꽣</div>
          <div class="grant-chip">??珥덇린??/div>
        </div>
        <div class="grant-filter-grid">
          <div class="grant-filter-cell">
            <div class="grant-filter-label">湲곌? ?좏삎</div>
            <div class="grant-chip-row">
              <span class="grant-chip">?湲곗뾽</span>
              <span class="grant-chip">以묎껄湲곗뾽</span>
              <span class="grant-chip">以묒냼湲곗뾽/?ㅽ??몄뾽</span>
              <span class="grant-chip">???곌뎄??/span>
              <span class="grant-chip">怨듦났/誘쇨컙 ?곌뎄湲곌?</span>
              <span class="grant-chip">?섎즺湲곌?</span>
            </div>
          </div>
          <div class="grant-filter-cell">
            <div class="grant-filter-label">??留ㅼ텧??/div>
            <div class="grant-filter-input"><div class="grant-filter-placeholder">留ㅼ텧??낅젰</div><div class="grant-filter-unit">?듭썝</div></div>
          </div>
          <div class="grant-filter-cell">
            <div class="grant-filter-label">??ъ뾽?곗닔</div>
            <div class="grant-filter-input"><div class="grant-filter-placeholder">?ъ뾽 ?곗닔 ?낅젰</div><div class="grant-filter-unit">??/div></div>
          </div>
          <div class="grant-filter-cell">
            <div class="grant-filter-label">湲곌? ?뚯옱吏</div>
            <div class="grant-chip-row">
              <span class="grant-chip active">?꾧뎅</span>
              <span class="grant-chip">?쒖슱</span>
              <span class="grant-chip">寃쎄린</span>
              <span class="grant-chip">?몄쿇</span>
              <span class="grant-chip">遺??/span>
              <span class="grant-chip">?援?/span>
            </div>
          </div>
          <div class="grant-filter-cell">
            <div class="grant-filter-label">遺?ㅼ뿰援ъ냼/?곌뎄?꾨떞遺??좊Т</div>
            <div class="grant-chip-row">
              <span class="grant-chip">??/span>
              <span class="grant-chip">?꾨땲??/span>
            </div>
          </div>
          <div class="grant-filter-cell">
            <div class="grant-filter-label">怨쇱젣 ?좏삎</div>
            <div class="grant-chip-row">
              <span class="grant-chip active">?꾩껜</span>
              <span class="grant-chip">?곌뎄媛쒕컻</span>
              <span class="grant-chip">?ъ뾽??/span>
            </div>
          </div>
        </div>
        """,
        unsafe_allow_html=True,
    )







def render_external_source_page(
    source_config: SourceRouteConfig,
    page_config: SourcePageConfig,
    source_datasets: dict[str, object],
) -> None:
    if page_config.kind == "opportunity":
        render_opportunity_page(
            source_datasets[page_config.data_key],
            page_key=page_config.key,
            title=page_config.title,
        )
        return

    source_label = source_config.label
    view_columns = list(page_config.view_columns) if page_config.view_columns else None
    primary_df = source_datasets[page_config.data_key]
    primary_origin = clean(source_datasets.get(page_config.origin_key, ""))

    if page_config.kind == "archive":
        secondary_df = source_datasets[page_config.secondary_data_key]
        secondary_origin = clean(source_datasets.get(page_config.secondary_origin_key, ""))
        render_source_notice_page(
            combine_notice_frames(primary_df, secondary_df),
            f"{primary_origin} + {secondary_origin}",
            prefix=page_config.key,
            title=page_config.title,
            source_label=source_label,
            view_columns=view_columns,
            archive=True,
        )
        return

    render_source_notice_page(
        primary_df,
        primary_origin,
        prefix=page_config.key,
        title=page_config.title,
        source_label=source_label,
        view_columns=view_columns,
    )


def render_external_source(
    source_config: SourceRouteConfig,
    source_datasets: dict[str, object] | None,
    *,
    show_internal_tabs: bool = True,
) -> None:
    if not source_datasets:
        st.error(f"{source_config.label} ?곗씠?곕? 遺덈윭?ㅼ? 紐삵뻽?듬땲??")
        return

    st.subheader(source_config.label)
    raw_page_key = normalize_route_page_key(get_query_param("page"))
    current_page_key = raw_page_key or source_config.default_page
    valid_page_keys = {page.key for page in source_config.page_configs}

    if current_page_key not in valid_page_keys:
        st.query_params.clear()
        st.query_params.update(with_auth_params({
            "source": source_config.key,
            "page": source_config.default_page,
            "view": "table",
        }))
        st.rerun()

    if show_internal_tabs:
        current_page_key = render_page_tabs(
            current_page_key,
            [(page.key, page.label) for page in source_config.page_configs],
            key=f"{source_config.key}_page_tabs",
        )

    page_config = next((page for page in source_config.page_configs if page.key == current_page_key), None)
    if page_config is None:
        st.error(f"{source_config.label} ?섏씠吏 援ъ꽦??李얠? 紐삵뻽?듬땲?? {current_page_key}")
        return

    render_external_source_page(source_config, page_config, source_datasets)


def render_tipa_source(
    source_config: SourceRouteConfig,
    mode_config: AppModeConfig,
    datasets: dict[str, pd.DataFrame],
    source_datasets: dict[str, object] | None,
    *,
    show_internal_tabs: bool = True,
) -> None:
    del mode_config, datasets
    render_external_source(source_config, source_datasets, show_internal_tabs=show_internal_tabs)


def render_nipa_source(
    source_config: SourceRouteConfig,
    mode_config: AppModeConfig,
    datasets: dict[str, pd.DataFrame],
    source_datasets: dict[str, object] | None,
    *,
    show_internal_tabs: bool = True,
) -> None:
    del mode_config, datasets
    render_external_source(source_config, source_datasets, show_internal_tabs=show_internal_tabs)


def render_favorites_source(
    source_config: SourceRouteConfig,
    mode_config: AppModeConfig,
    datasets: dict[str, pd.DataFrame],
    source_datasets: dict[str, object] | None,
    *,
    show_internal_tabs: bool = True,
) -> None:
    del show_internal_tabs
    del source_config, mode_config
    render_favorite_notice_page(
        datasets["notice_view"],
        datasets["opportunity"],
        source_datasets or {},
    )


def render_proposal_source(
    source_config: SourceRouteConfig,
    mode_config: AppModeConfig,
    datasets: dict[str, pd.DataFrame],
    source_datasets: dict[str, object] | None,
    *,
    show_internal_tabs: bool = True,
) -> None:
    del source_config, mode_config, show_internal_tabs
    opportunity_index = build_dashboard_opportunity_index(datasets, source_datasets)
    recommended_count = int(opportunity_index["Recommendation"].fillna("").astype(str).str.contains("異붿쿇").sum()) if not opportunity_index.empty else 0
    high_score_count = int(opportunity_index["Score"].fillna(0).ge(80).sum()) if not opportunity_index.empty else 0

    render_page_header(
        "?쒖븞愿由?,
        "?쒖븞 ?④퀎 ?곗씠?곕뒗 ?꾩쭅 遺꾨━ ?곕룞 ?꾩엯?덈떎. ?꾩옱??異붿쿇湲고쉶? 愿??怨듦퀬 以묒떖?쇰줈 ?꾨낫瑜?愿由ы빀?덈떎.",
        eyebrow="Proposal",
    )
    render_metrics(
        [
            ("異붿쿇 ?꾨낫", str(recommended_count)),
            ("怨좊뱷??꾨낫", str(high_score_count)),
            ("?묒꽦以?, "-"),
            ("?쒖텧 ?꾨즺", "-"),
        ]
    )
    st.info("?쒖븞 ?④퀎蹂??곹깭, ?쒖텧 ?대젰, 寃곌낵 ?湲??꾪솴? ?꾩냽 ?곕룞??꾩슂?⑸땲??")
    action_cols = st.columns(3)
    with action_cols[0]:
        if st.button("異붿쿇湲고쉶濡??대룞", key="proposal_go_opportunity", use_container_width=True):
            navigate_to_route("iris", "rfp_queue")
    with action_cols[1]:
        if st.button("愿??怨듦퀬 蹂닿린", key="proposal_go_favorites", use_container_width=True):
            navigate_to_route("favorites", "favorites")
    with action_cols[2]:
        if st.button("??쒕낫?쒕줈 ?대룞", key="proposal_go_dashboard", use_container_width=True):
            navigate_to_route("dashboard", "dashboard")


def render_operations_source(
    source_config: SourceRouteConfig,
    mode_config: AppModeConfig,
    datasets: dict[str, pd.DataFrame],
    source_datasets: dict[str, object] | None,
    *,
    show_internal_tabs: bool = True,
) -> None:
    del source_config, mode_config, show_internal_tabs
    current_notice_index = build_dashboard_notice_index(datasets, source_datasets, archived=False)
    total_current_notices = int(len(current_notice_index))
    total_review_needed = int(current_notice_index["Review"].fillna("").astype(str).str.strip().eq("").sum()) if not current_notice_index.empty else 0
    review_coverage = "-"
    if total_current_notices > 0:
        review_coverage = f"{((total_current_notices - total_review_needed) / total_current_notices) * 100:.0f}%"

    favorites_df = build_favorite_notice_df(datasets["notice_view"], source_datasets or {})
    recent_comments_df = build_dashboard_recent_comments_table(limit=8)

    render_page_header(
        "?댁쁺愿由?,
        (
            f"{get_current_operation_scope_label()} 湲곗??愿??怨듦퀬, ?볤?, ?ㅻ쪟, 寃??而ㅻ쾭由ъ?瑜??뺤씤?⑸땲??"
            if is_user_scoped_operations_enabled()
            else "愿??怨듦퀬, ?볤?, ?ㅻ쪟, 寃??而ㅻ쾭由ъ?瑜?湲곗??쇰줈 ?댁쁺 ?곹깭瑜??뺤씤?⑸땲??"
        ),
        eyebrow="Operations",
    )
    render_metrics(
        [
            ("?꾩옱 怨듦퀬", str(total_current_notices)),
            ("誘멸??, str(total_review_needed)),
            ("愿??怨듦퀬", str(len(favorites_df))),
            ("而ㅻ쾭由ъ?", review_coverage),
        ]
    )

    left_col, right_col = st.columns([1.2, 1.0])
    with left_col:
        render_dashboard_table_block(
            "愿??怨듦퀬",
            favorites_df[["留ㅼ껜", "怨듦퀬紐?, "怨듦퀬?쇱옄"]].head(10)
            if not favorites_df.empty and {"留ㅼ껜", "怨듦퀬紐?, "怨듦퀬?쇱옄"}.issubset(favorites_df.columns)
            else pd.DataFrame(),
        )
    with right_col:
        render_dashboard_table_block("理쒓렐 ?볤?", recent_comments_df)


def render_signup_request_public_page() -> None:
    render_page_header(
        "媛??붿껌",
        "Viewer ?ъ슜 ?붿껌??④린硫?private admin app?먯꽌 諛붾줈 寃?좏븷 ??덈룄濡??묒닔?⑸땲??",
        eyebrow="Support",
    )
    st.caption("?묒닔??붿껌 寃?좎? ?뱀씤/諛섎젮 泥섎━??蹂꾨룄 private admin app?먯꽌 吏꾪뻾?⑸땲??")

    default_email = clean(get_env("APP_USER_EMAIL"))
    default_name = clean(get_env("APP_USER_NAME") or get_env("DEFAULT_COMMENT_AUTHOR"))
    default_org = clean(get_env("APP_USER_ORGANIZATION"))

    with st.form("signup_request_public_form"):
        name = st.text_input("?대쫫", value=default_name)
        email = st.text_input("?대찓??, value=default_email)
        organization = st.text_input("?뚯냽 / ?뚯궗", value=default_org)
        account_type = st.selectbox("怨꾩젙 ?좏삎", ["company", "lab", "institution", "student", "team"], index=0)
        request_note = st.text_area("?붿껌 硫붾え", height=140, placeholder="?ъ슜 紐⑹쟻?대굹 ?꾩슂??곗씠??踰붿쐞瑜??곸뼱二쇱꽭??")
        submitted = st.form_submit_button("媛??붿껌 蹂대궡湲?, type="primary", use_container_width=True)

    normalized_email = clean(email).lower()
    existing_requests = get_signup_requests_for_email(normalized_email) if normalized_email else pd.DataFrame()
    latest_request = existing_requests.iloc[0].to_dict() if not existing_requests.empty else {}
    latest_status = clean(latest_request.get("status")).upper()

    if submitted:
        if not normalized_email:
            st.error("?대찓?쇱? 鍮꾩썙??놁뒿?덈떎.")
            return
        if latest_status in {"PENDING", "HOLD"}:
            st.warning("媛숈? ?대찓?쇰줈 吏꾪뻾 以묒씤 媛??붿껌??대? ?덉뒿?덈떎. private admin app 寃??ㅼ떆 ?뺤씤?댁＜?몄슂.")
            return
        if latest_status == "APPROVED":
            st.success("?대? ?뱀씤??붿껌??덉뒿?덈떎. ?댁쁺? ?덈궡 硫붿씪??癒쇱? ?뺤씤?댁＜?몄슂.")
            return
        save_signup_request(
            {
                "name": name,
                "email": normalized_email,
                "organization": organization,
                "account_type": account_type,
                "request_note": request_note,
                "status": "PENDING",
            }
        )
        st.success("媛??붿껌??묒닔?덉뒿?덈떎. private admin app?먯꽌 諛붾줈 寃?좏븷 ??덉뒿?덈떎.")
        st.rerun()


def render_access_request_source(
    source_config: SourceRouteConfig,
    mode_config: AppModeConfig,
    datasets: dict[str, pd.DataFrame],
    source_datasets: dict[str, object] | None,
    *,
    show_internal_tabs: bool = True,
) -> None:
    del source_config, mode_config, datasets, source_datasets, show_internal_tabs
    render_signup_request_public_page()



def _inject_opportunity_workspace_styles() -> None:
    if st.session_state.get("_opportunity_workspace_styles_injected"):
        return
    st.session_state["_opportunity_workspace_styles_injected"] = True
    st.markdown(
        """
        <style>
        .dashboard-shell {
          display: flex;
          flex-direction: column;
          gap: 1rem;
        }
        .dashboard-greeting {
          display: flex;
          align-items: center;
          justify-content: space-between;
          gap: 1rem;
          min-height: 82px;
          padding: 1rem 1.25rem;
          border: 1px solid #e2e8f0;
          border-radius: 16px;
          background: #ffffff;
          box-shadow: 0 10px 26px rgba(15, 23, 42, 0.04);
        }
        .dashboard-greeting-title {
          color: var(--text-strong);
          font-size: 1.45rem;
          font-weight: 900;
          line-height: 1.2;
          letter-spacing: -0.03em;
        }
        .dashboard-greeting-copy {
          margin-top: 0.3rem;
          color: var(--text-muted);
          font-size: 0.92rem;
          line-height: 1.55;
        }
        .dashboard-greeting-meta {
          display: flex;
          flex-wrap: wrap;
          justify-content: flex-end;
          gap: 0.55rem;
        }
        .dashboard-greeting-pill {
          display: inline-flex;
          align-items: center;
          min-height: 34px;
          padding: 0 0.82rem;
          border-radius: 999px;
          border: 1px solid #dbe4f0;
          background: #f8fbff;
          color: var(--text-body);
          font-size: 0.78rem;
          font-weight: 700;
        }
        .dashboard-kpi-grid {
          display: grid;
          grid-template-columns: repeat(4, minmax(0, 1fr));
          gap: 0.9rem;
        }
        .dashboard-kpi-card {
          min-height: 96px;
          padding: 1rem 1.05rem;
          border: 1px solid #e2e8f0;
          border-radius: 14px;
          background: #ffffff;
          box-shadow: 0 8px 20px rgba(15, 23, 42, 0.04);
        }
        .dashboard-kpi-topline {
          display: flex;
          align-items: flex-start;
          justify-content: space-between;
          gap: 0.75rem;
        }
        .dashboard-kpi-label {
          color: var(--text-muted);
          font-size: 0.8rem;
          font-weight: 800;
          line-height: 1.2;
        }
        .dashboard-kpi-icon {
          color: #2563eb;
          font-size: 1rem;
          line-height: 1;
        }
        .dashboard-kpi-value {
          margin-top: 0.42rem;
          color: var(--text-strong);
          font-size: 1.9rem;
          font-weight: 900;
          line-height: 1;
          letter-spacing: -0.03em;
        }
        .dashboard-kpi-copy {
          margin-top: 0.3rem;
          color: var(--text-muted);
          font-size: 0.78rem;
          line-height: 1.4;
        }
        .oppty-section-header {
          display: flex;
          align-items: center;
          justify-content: space-between;
          gap: 1rem;
          margin: 0.4rem 0 0.25rem;
        }
        .oppty-section-title {
          color: var(--text-strong);
          font-size: 1.2rem;
          font-weight: 900;
          letter-spacing: -0.03em;
        }
        .oppty-section-subtitle {
          margin-top: 0.18rem;
          color: var(--text-muted);
          font-size: 0.84rem;
          line-height: 1.5;
        }
        .dashboard-section-link {
          color: #2563eb;
          font-size: 0.86rem;
          font-weight: 800;
          white-space: nowrap;
        }
        .rfp-card {
          border: 1px solid #e2e8f0;
          border-radius: 18px;
          background: #ffffff;
          box-shadow: 0 12px 28px rgba(15, 23, 42, 0.05);
          padding: 1rem;
          min-height: 100%;
          display: flex;
          flex-direction: column;
        }
        .rfp-card.is-active {
          border-color: #2563eb;
          box-shadow: 0 16px 34px rgba(37, 99, 235, 0.12);
          background: #f8fbff;
        }
        .rfp-card-topline {
          display: flex;
          align-items: center;
          justify-content: space-between;
          gap: 0.55rem;
          margin-bottom: 0.68rem;
        }
        .rfp-card-rank {
          width: 30px;
          height: 30px;
          border-radius: 999px;
          background: linear-gradient(180deg, #3b82f6, #2563eb);
          color: #ffffff;
          display: inline-flex;
          align-items: center;
          justify-content: center;
          font-size: 0.8rem;
          font-weight: 900;
        }
        .rfp-card-badges {
          display: flex;
          flex-wrap: wrap;
          justify-content: flex-end;
          gap: 0.38rem;
        }
        .rfp-card-title {
          color: var(--text-strong);
          font-size: 0.98rem;
          font-weight: 900;
          line-height: 1.42;
          min-height: 3.05rem;
        }
        .rfp-card-notice {
          margin-top: 0.3rem;
          color: var(--text-muted);
          font-size: 0.78rem;
          line-height: 1.45;
          min-height: 2.15rem;
        }
        .rfp-card-analysis {
          margin-top: 0.55rem;
          color: var(--text-body);
          font-size: 0.82rem;
          line-height: 1.55;
          min-height: 2.45rem;
          display: -webkit-box;
          -webkit-line-clamp: 2;
          -webkit-box-orient: vertical;
          overflow: hidden;
        }
        .rfp-card-keywords {
          display: flex;
          flex-wrap: wrap;
          gap: 0.38rem;
          margin-top: 0.6rem;
        }
        .rfp-card-keyword {
          display: inline-flex;
          align-items: center;
          padding: 0.28rem 0.62rem;
          border-radius: 999px;
          background: #f8fafc;
          border: 1px solid #e2e8f0;
          color: var(--text-muted);
          font-size: 0.74rem;
          font-weight: 700;
        }
        .rfp-card-meta {
          display: grid;
          grid-template-columns: minmax(0, 1.1fr) minmax(0, 1fr);
          gap: 0.55rem 0.75rem;
          margin-top: 0.8rem;
          padding-top: 0.72rem;
          border-top: 1px solid rgba(226, 232, 240, 0.9);
        }
        .rfp-card-meta-label {
          color: var(--text-subtle);
          font-size: 0.72rem;
          font-weight: 800;
          letter-spacing: 0.05em;
          text-transform: uppercase;
          margin-bottom: 0.22rem;
        }
        .rfp-card-meta-value {
          color: var(--text-body);
          font-size: 0.8rem;
          font-weight: 700;
          line-height: 1.45;
        }
        .rfp-card-action-slot {
          margin-top: auto;
          padding-top: 0.8rem;
        }
        .oppty-carousel-summary {
          color: var(--text-muted);
          font-size: 0.84rem;
          text-align: center;
          padding-top: 0.4rem;
        }
        .notice-row-shell {
          border: 1px solid #e2e8f0;
          border-radius: 16px;
          background: #ffffff;
          box-shadow: 0 12px 28px rgba(15, 23, 42, 0.05);
          overflow: hidden;
        }
        .notice-row-head {
          display: grid;
          grid-template-columns: 80px 92px minmax(240px, 2.6fr) minmax(120px, 1.2fr) 96px 76px 108px minmax(210px, 1.8fr) 58px;
          gap: 0.75rem;
          align-items: center;
          padding: 0.82rem 1rem;
          border-bottom: 1px solid rgba(226, 232, 240, 0.92);
          color: var(--text-subtle);
          font-size: 0.74rem;
          font-weight: 800;
          letter-spacing: 0.04em;
          text-transform: uppercase;
        }
        .notice-row-body {
          display: grid;
          grid-template-columns: 80px 92px minmax(240px, 2.6fr) minmax(120px, 1.2fr) 96px 76px 108px minmax(210px, 1.8fr) 58px;
          gap: 0.75rem;
          align-items: center;
          padding: 0.75rem 1rem;
          min-height: 46px;
          border-top: 1px solid rgba(248, 250, 252, 0.4);
        }
        .notice-row-body + .notice-row-body {
          border-top: 1px solid rgba(226, 232, 240, 0.82);
        }
        .notice-row-body.is-active {
          background: #f8fbff;
        }
        .notice-row-title {
          color: var(--text-strong);
          font-size: 0.88rem;
          font-weight: 800;
          line-height: 1.38;
        }
        .notice-row-meta,
        .notice-row-summary {
          color: var(--text-muted);
          font-size: 0.78rem;
          line-height: 1.4;
        }
        .notice-row-summary {
          display: -webkit-box;
          -webkit-line-clamp: 2;
          -webkit-box-orient: vertical;
          overflow: hidden;
        }
        [class*="st-key-dashboard_notice_open_"] button,
        [class*="st-key-dashboard_recommended_rfp_select_"] button {
          width: 100%;
          min-height: 38px !important;
          border-radius: 12px !important;
          font-size: 0.84rem !important;
          font-weight: 800 !important;
        }
        [class*="st-key-dashboard_notice_open_"] button {
          min-height: 34px !important;
        }
        .summary-panel {
          position: sticky;
          top: 96px;
          border: 1px solid #e2e8f0;
          border-radius: 16px;
          background: #ffffff;
          box-shadow: 0 18px 40px rgba(15, 23, 42, 0.08);
          padding: 1rem 1rem 1.05rem;
        }
        .summary-panel-header {
          color: var(--text-strong);
          font-size: 1rem;
          font-weight: 900;
          letter-spacing: -0.02em;
        }
        .summary-panel-empty {
          padding: 0.9rem 0.15rem 0.3rem;
        }
        .summary-panel-empty-title {
          color: var(--text-strong);
          font-size: 1rem;
          font-weight: 800;
          line-height: 1.5;
        }
        .summary-panel-empty-copy {
          margin-top: 0.45rem;
          color: var(--text-muted);
          font-size: 0.86rem;
          line-height: 1.6;
        }
        .summary-panel-badges {
          display: flex;
          flex-wrap: wrap;
          gap: 0.38rem;
          margin-top: 0.5rem;
        }
        .summary-panel-type {
          display: inline-flex;
          align-items: center;
          justify-content: center;
          min-height: 28px;
          padding: 0 0.8rem;
          border-radius: 999px;
          background: rgba(79, 70, 229, 0.12);
          color: #4f46e5;
          font-size: 0.76rem;
          font-weight: 800;
        }
        .summary-panel-title {
          margin-top: 0.82rem;
          color: var(--text-strong);
          font-size: 1.12rem;
          font-weight: 900;
          line-height: 1.48;
          letter-spacing: -0.02em;
        }
        .summary-panel-source {
          margin-top: 0.45rem;
          color: var(--text-muted);
          font-size: 0.86rem;
          line-height: 1.55;
        }
        .summary-panel-meta-grid {
          display: grid;
          grid-template-columns: minmax(0, 1fr);
          gap: 0.7rem;
          margin-top: 0.95rem;
          padding: 0.9rem 0;
          border-top: 1px solid rgba(226, 232, 240, 0.92);
          border-bottom: 1px solid rgba(226, 232, 240, 0.92);
        }
        .summary-panel-meta-label {
          color: var(--text-subtle);
          font-size: 0.72rem;
          font-weight: 800;
          text-transform: uppercase;
          letter-spacing: 0.05em;
          margin-bottom: 0.22rem;
        }
        .summary-panel-meta-value {
          color: var(--text-body);
          font-size: 0.88rem;
          font-weight: 700;
          line-height: 1.55;
        }
        .summary-panel-copy {
          margin-top: 0.9rem;
          color: var(--text-body);
          font-size: 0.9rem;
          line-height: 1.72;
        }
        .summary-panel-keywords {
          display: flex;
          flex-wrap: wrap;
          gap: 0.38rem;
          margin-top: 0.8rem;
        }
        .summary-panel-keyword {
          display: inline-flex;
          align-items: center;
          padding: 0.32rem 0.66rem;
          border-radius: 999px;
          background: var(--surface-soft);
          border: 1px solid rgba(216, 227, 242, 0.9);
          color: var(--text-muted);
          font-size: 0.74rem;
          font-weight: 700;
        }
        .summary-panel-link-secondary,
        .summary-panel-link-primary {
          display: inline-flex;
          align-items: center;
          justify-content: center;
          width: 100%;
          min-height: 42px;
          border-radius: 14px;
          text-decoration: none !important;
          font-size: 0.88rem;
          font-weight: 800;
          transition: border-color 140ms ease, background-color 140ms ease, transform 140ms ease;
        }
        .summary-panel-link-secondary {
          border: 1px solid #e2e8f0;
          background: #ffffff;
          color: var(--text-body);
        }
        .summary-panel-link-primary {
          border: 1px solid #4f46e5;
          background: #4f46e5;
          color: #ffffff;
        }
        .summary-panel-link-secondary:hover,
        .summary-panel-link-primary:hover {
          transform: translateY(-1px);
        }
        @media (max-width: 1200px) {
          .dashboard-kpi-grid {
            grid-template-columns: repeat(2, minmax(0, 1fr));
          }
          .notice-row-head,
          .notice-row-body {
            grid-template-columns: 80px 90px minmax(220px, 2.2fr) minmax(110px, 1.15fr) 88px 72px 96px minmax(160px, 1.5fr) 58px;
          }
        }
        @media (max-width: 820px) {
          .dashboard-greeting {
            flex-direction: column;
            align-items: flex-start;
          }
          .dashboard-greeting-meta {
            justify-content: flex-start;
          }
          .dashboard-kpi-grid,
          .rfp-card-meta {
            grid-template-columns: minmax(0, 1fr);
          }
          .notice-row-head {
            display: none;
          }
          .notice-row-body {
            grid-template-columns: minmax(0, 1fr);
            gap: 0.35rem;
          }
          .summary-panel {
            position: static;
          }
        }
        @media (max-width: 640px) {
          .dashboard-kpi-grid {
            grid-template-columns: 1fr;
          }
        }
        </style>
        """,
        unsafe_allow_html=True,
    )


def _extract_dashboard_keywords(row: dict[str, object] | pd.Series, *, limit: int = 4) -> list[str]:
    text = clean(first_non_empty(row, "llm_keywords", "keywords", "Keywords"))
    if not text:
        return []
    items: list[str] = []
    for token in text.replace("|", ",").replace("/", ",").replace("\n", ",").split(","):
        normalized = clean(token)
        if normalized and normalized not in items:
            items.append(normalized)
        if len(items) >= limit:
            break
    return items


def _dashboard_review_value(row: dict[str, object] | pd.Series | None) -> str:
    return clean(first_non_empty(row or {}, "Review", "review_status", "寃??щ?", "寃?좎뿬遺"))


def _count_dashboard_urgent_notices(rows: pd.DataFrame, *, max_days: int = 30) -> int:
    if rows is None or rows.empty:
        return 0
    count = 0
    period_values = series_from_candidates(rows, ["notice_period", "?묒닔湲곌컙", "period"]).fillna("").astype(str)
    today = pd.Timestamp.now().normalize()
    for period_text in period_values:
        period_end = extract_period_end(clean(period_text))
        if pd.isna(period_end):
            continue
        d_day = int((period_end.normalize() - today).days)
        if 0 <= d_day <= max_days:
            count += 1
    return count


def _navigate_from_dashboard_kpi(card_key: str) -> None:
    if card_key == "recommended_rfp":
        route = route_core.build_rfp_queue_route(
            filters={
                "recommendation": ["異붿쿇"],
                "status": [],
                "deadline": [],
                "field": [],
                "review": [],
                "sort": "異붿쿇??,
                "archive_reason": [],
            },
            page_no=1,
            page_size=20,
        )
    elif card_key == "review_needed":
        route = route_core.build_rfp_queue_route(
            filters={
                "recommendation": ["異붿쿇"],
                "status": [],
                "deadline": [],
                "field": [],
                "review": ["", "寃?좎쟾", "誘멸??],
                "sort": "異붿쿇??,
                "archive_reason": [],
            },
            page_no=1,
            page_size=20,
        )
    elif card_key == "urgent_notice":
        route = route_core.build_notice_queue_route(
            filters={
                "status": ["吏꾪뻾以?, "?덉젙"],
                "recommendation": [],
                "search": "",
                "source": [],
                "page_size": 20,
                "dday_max": 30,
                "include_closed": False,
            },
            page_no=1,
            page_size=20,
        )
    else:
        route = route_core.build_favorites_route(
            filters={
                "type": [],
                "review": [FAVORITE_REVIEW_STATUS],
                "deadline": [],
                "page_size": 20,
            },
            page_no=1,
            page_size=20,
        )
    navigate_to_route_state(route, push=True)
    st.rerun()


def _render_compact_public_dashboard_kpi_cards(recommended_rows: pd.DataFrame, notice_rows: pd.DataFrame) -> None:
    recommended_only_rows = pd.DataFrame()
    if recommended_rows is not None and not recommended_rows.empty:
        recommendation_series = series_from_candidates(
            recommended_rows,
            ["_queue_recommendation", "Recommendation", "recommendation", "llm_recommendation"],
        ).fillna("").astype(str).apply(_normalize_recommendation_value)
        recommended_only_rows = recommended_rows[recommendation_series.eq("異붿쿇")].copy()
    recommended_count = len(recommended_only_rows) if not recommended_only_rows.empty else 0
    review_needed = 0
    favorite_count = 0
    if not recommended_only_rows.empty:
        review_series = recommended_only_rows.apply(_dashboard_review_value, axis=1)
        review_needed = int(review_series.isin(["", "寃?좎쟾", "誘멸??]).sum())
    if notice_rows is not None and not notice_rows.empty:
        favorite_series = notice_rows.apply(_dashboard_review_value, axis=1)
        favorite_count = int(favorite_series.eq(FAVORITE_REVIEW_STATUS).sum())
    urgent_count = _count_dashboard_urgent_notices(notice_rows)

    cards = [
        ("recommended_rfp", "異붿쿇 RFP", str(recommended_count), "異붿쿇 RFP Queue濡?諛붾줈 ?대룞", "??),
        ("review_needed", "寃??꾩슂", str(review_needed), "寃?좎쟾 異붿쿇 怨쇱젣留?紐⑥븘??蹂닿린", "??),
        ("urgent_notice", "留덇컧 ?꾨컯", str(urgent_count), "30??대궡 吏꾪뻾以??덉젙 怨듦퀬 蹂닿린", "!"),
        ("favorite_notice", "愿?ш났怨?, str(favorite_count), "Favorites 紐⑸줉?쇰줈 諛붾줈 ?대룞", "??),
    ]
    cols = st.columns(4, gap="medium")
    for column, (card_key, label, value, copy, icon) in zip(cols, cards):
        safe_key = _css_safe_key(f"dashboard_kpi_{card_key}")
        st.markdown(
            f"""
            <style>
            .st-key-{safe_key} button {{
              min-height: 96px !important;
              width: 100% !important;
              padding: 0.95rem 1rem !important;
              border-radius: 14px !important;
              border: 1px solid #e2e8f0 !important;
              background: #ffffff !important;
              color: #15233b !important;
              box-shadow: 0 8px 20px rgba(15, 23, 42, 0.04) !important;
              text-align: left !important;
              white-space: pre-line !important;
              line-height: 1.35 !important;
              font-size: 0.84rem !important;
              font-weight: 700 !important;
            }}
            .st-key-{safe_key} button:hover {{
              border-color: #93c5fd !important;
              background: #f8fbff !important;
              color: #1d4ed8 !important;
              box-shadow: 0 14px 28px rgba(37, 99, 235, 0.10) !important;
            }}
            </style>
            """,
            unsafe_allow_html=True,
        )
        with column:
            if st.button(
                f"{label}  {icon}\n{value}\n{copy}\n諛붾줈媛湲?->",
                key=f"dashboard_kpi_{card_key}",
                use_container_width=True,
                type="secondary",
            ):
                _navigate_from_dashboard_kpi(card_key)


def _dashboard_selection_state() -> dict[str, str]:
    value = st.session_state.get("dashboard_summary_selection")
    return dict(value) if isinstance(value, dict) else {}


def _set_dashboard_rfp_selection(row: dict[str, object] | pd.Series) -> None:
    row_id = clean(first_non_empty(row, "_row_id", "Row ID"))
    if not row_id:
        return
    st.session_state["selected_item_type"] = "rfp"
    st.session_state["selected_id"] = row_id
    st.session_state["selected_rfp_id"] = row_id
    st.session_state["selected_notice_id"] = ""
    st.session_state["dashboard_selected_item_type"] = "rfp"
    st.session_state["dashboard_selected_rfp_id"] = row_id
    st.session_state["dashboard_selected_notice_id"] = ""
    st.session_state["dashboard_summary_selection"] = {
        "type": "rfp",
        "source_key": resolve_route_source_key_for_row(row) or "iris",
        "row_id": row_id,
        "notice_id": clean(first_non_empty(row, "notice_id", "Notice ID")),
    }
    route = route_core.build_dashboard_route(view="summary")
    route["item_type"] = "rfp"
    route["item_id"] = row_id
    route["source_key"] = resolve_route_source_key_for_row(row) or "iris"
    route_core.set_current_route(route)
    replace_query_params(with_auth_params(route_core.serialize_route(route)))


def _set_dashboard_notice_selection(row: dict[str, object] | pd.Series) -> None:
    notice_id = clean(first_non_empty(row, "怨듦퀬ID", "notice_id"))
    if not notice_id:
        return
    st.session_state["selected_item_type"] = "notice"
    st.session_state["selected_id"] = notice_id
    st.session_state["selected_notice_id"] = notice_id
    st.session_state["selected_rfp_id"] = ""
    st.session_state["dashboard_selected_item_type"] = "notice"
    st.session_state["dashboard_selected_notice_id"] = notice_id
    st.session_state["dashboard_selected_rfp_id"] = ""
    st.session_state["dashboard_summary_selection"] = {
        "type": "notice",
        "source_key": resolve_route_source_key_for_row(row, source_key=first_non_empty(row, "source_key")) or "iris",
        "notice_id": notice_id,
    }
    route = route_core.build_dashboard_route(view="summary")
    route["item_type"] = "notice"
    route["item_id"] = notice_id
    route["source_key"] = resolve_route_source_key_for_row(row, source_key=first_non_empty(row, "source_key")) or "iris"
    route_core.set_current_route(route)
    replace_query_params(with_auth_params(route_core.serialize_route(route)))


def _clear_dashboard_selection() -> None:
    st.session_state.pop("selected_item_type", None)
    st.session_state.pop("selected_id", None)
    st.session_state.pop("selected_rfp_id", None)
    st.session_state.pop("selected_notice_id", None)
    st.session_state.pop("dashboard_selected_item_type", None)
    st.session_state.pop("dashboard_selected_rfp_id", None)
    st.session_state.pop("dashboard_selected_notice_id", None)
    st.session_state.pop("dashboard_summary_selection", None)
    route = route_core.build_dashboard_route(view="list")
    route_core.set_current_route(route)
    replace_query_params(with_auth_params(route_core.serialize_route(route)))


def _is_selected_dashboard_rfp(row: dict[str, object] | pd.Series) -> bool:
    selection = _dashboard_selection_state()
    if selection.get("type") != "rfp":
        return False
    return (
        clean(selection.get("row_id")) == clean(first_non_empty(row, "_row_id", "Row ID"))
        and clean(selection.get("source_key")) == (resolve_route_source_key_for_row(row) or "iris")
    )


def _is_selected_dashboard_notice(row: dict[str, object] | pd.Series) -> bool:
    selection = _dashboard_selection_state()
    if selection.get("type") != "notice":
        return False
    return (
        clean(selection.get("notice_id")) == clean(first_non_empty(row, "怨듦퀬ID", "notice_id"))
        and clean(selection.get("source_key")) == (resolve_route_source_key_for_row(row, source_key=first_non_empty(row, "source_key")) or "iris")
    )


def _resolve_dashboard_selected_row(
    opportunity_rows: pd.DataFrame,
    notice_rows: pd.DataFrame,
) -> tuple[str, pd.Series | None]:
    selection = _dashboard_selection_state()
    selected_type = clean(selection.get("type"))
    source_key = clean(selection.get("source_key"))
    if selected_type == "rfp":
        row_id = clean(selection.get("row_id"))
        if not row_id or opportunity_rows.empty:
            return "rfp", None
        matched = opportunity_rows[
            opportunity_rows["Row ID"].fillna("").astype(str).str.strip().eq(row_id)
            & opportunity_rows["source_key"].fillna("").astype(str).str.strip().eq(source_key)
        ]
        return "rfp", matched.iloc[0] if not matched.empty else None
    if selected_type == "notice":
        notice_id = clean(selection.get("notice_id"))
        if not notice_id or notice_rows.empty:
            return "notice", None
        matched = notice_rows[
            notice_rows["_dashboard_notice_id"].fillna("").astype(str).str.strip().eq(notice_id)
            & notice_rows["source_key"].fillna("").astype(str).str.strip().eq(source_key)
        ]
        return "notice", matched.iloc[0] if not matched.empty else None
    return "", None


def _render_same_tab_link_button(label: str, href: str, *, kind: str = "secondary", key: str = "") -> None:
    if not clean(href):
        st.button(label, key=key or f"disabled_{label}_{kind}", disabled=True, use_container_width=True)
        return
    st.link_button(label, href, use_container_width=True)


def _render_summary_panel_empty(empty_title: str, empty_copy: str) -> None:
    st.markdown(
        f'<div class="summary-panel-empty"><div class="summary-panel-empty-title">{escape(empty_title)}</div><div class="summary-panel-empty-copy">{escape(empty_copy)}</div></div>',
        unsafe_allow_html=True,
    )


def _render_rfp_preview_panel(
    selected_row: pd.Series | dict | None,
    *,
    panel_key: str,
    empty_title: str,
    empty_copy: str,
    close_callback=None,
) -> None:
    st.markdown('<div class="summary-panel">', unsafe_allow_html=True)
    header_cols = st.columns([5, 1], gap="small")
    with header_cols[0]:
        st.markdown('<div class="summary-panel-header">Preview Panel</div>', unsafe_allow_html=True)
    with header_cols[1]:
        if st.button("??, key=f"{panel_key}_close", use_container_width=True, disabled=selected_row is None):
            if callable(close_callback):
                close_callback()
            else:
                st.rerun()

    if selected_row is None:
        _render_summary_panel_empty(empty_title, empty_copy)
        st.markdown("</div>", unsafe_allow_html=True)
        return

    source_key = resolve_route_source_key_for_row(selected_row, source_key=first_non_empty(selected_row, "source_key")) or "iris"
    ctx = _queue_row_context(selected_row)
    summary_text = clean(first_non_empty(selected_row, "Reason", "reason", "llm_reason")) or ctx["reason"]
    keywords = _extract_dashboard_keywords(selected_row)
    top_badges = "".join(
        [
            '<span class="summary-panel-type">RFP</span>',
            _pill_html(ctx["recommendation"]),
            _pill_html(ctx["score"], kind="score"),
            _pill_html(ctx["deadline"], kind="deadline"),
        ]
    )
    detail_target_id = clean(first_non_empty(selected_row, "Row ID", "_row_id"))
    notice_id = clean(first_non_empty(selected_row, "Notice ID", "notice_id"))
    current_value = clean(first_non_empty(selected_row, "Review", "review_status"))
    notice_title = clean(first_non_empty(selected_row, "Notice Title", "notice_title"))
    source_line = " / ".join(part for part in [ctx["ministry"], ctx["agency"]] if clean(part) and part != "-") or ctx["agency"]
    detail_link = resolve_external_detail_link(selected_row, source_key=source_key)
    keyword_html = "".join(f'<span class="summary-panel-keyword">{escape(keyword)}</span>' for keyword in keywords[:6])

    st.markdown(
        (
            f'<div class="summary-panel-badges">{top_badges}</div>'
            f'<div class="summary-panel-title">{escape(ctx["project"])}</div>'
            f'<div class="summary-panel-source">{escape(source_line or "-")}</div>'
            '<div class="summary-panel-meta-grid">'
            f'<div><div class="summary-panel-meta-label">湲곌컙 / D-day</div><div class="summary-panel-meta-value">{escape(ctx["period"])} 쨌 {escape(ctx["deadline"])}</div></div>'
            f'<div><div class="summary-panel-meta-label">?덉궛</div><div class="summary-panel-meta-value">{escape(ctx["budget"])}</div></div>'
            '</div>'
            f'<div class="summary-panel-copy">{escape(summary_text)}</div>'
            f'<div class="summary-panel-keywords">{keyword_html}</div>'
        ),
        unsafe_allow_html=True,
    )

    favorite_col, detail_col = st.columns(2, gap="small")
    with favorite_col:
        render_favorite_scrap_button(
            notice_id=notice_id,
            current_value=current_value,
            source_key=source_key,
            notice_title=notice_title,
            button_key=f"{panel_key}_favorite_{notice_id or detail_target_id}",
            compact=True,
            use_container_width=True,
        )
    with detail_col:
        if st.button("RFP ?곸꽭 蹂닿린", key=f"{panel_key}_detail", type="primary", use_container_width=True):
            navigate_to_opportunity_detail(source_key, detail_target_id)

    st.markdown('<div style="height:0.45rem"></div>', unsafe_allow_html=True)
    _render_same_tab_link_button(
        "?먮Ц怨듦퀬 ?닿린",
        detail_link,
        kind="secondary",
        key=f"{panel_key}_origin_{notice_id or detail_target_id}",
    )
    st.markdown("</div>", unsafe_allow_html=True)


def _render_notice_preview_panel(
    selected_row: pd.Series | dict | None,
    *,
    panel_key: str,
    empty_title: str,
    empty_copy: str,
    close_callback=None,
    detail_source_key: str | None = None,
) -> None:
    st.markdown('<div class="summary-panel">', unsafe_allow_html=True)
    header_cols = st.columns([5, 1], gap="small")
    with header_cols[0]:
        st.markdown('<div class="summary-panel-header">Summary Panel</div>', unsafe_allow_html=True)
    with header_cols[1]:
        if st.button("??, key=f"{panel_key}_close", use_container_width=True, disabled=selected_row is None):
            if callable(close_callback):
                close_callback()
            else:
                st.rerun()

    if selected_row is None:
        _render_summary_panel_empty(empty_title, empty_copy)
        st.markdown("</div>", unsafe_allow_html=True)
        return

    source_key = clean(detail_source_key) or resolve_route_source_key_for_row(
        selected_row,
        source_key=first_non_empty(selected_row, "source_key"),
    ) or "iris"
    title_text = clean(first_non_empty(selected_row, "怨듦퀬紐?, "notice_title")) or "-"
    period_text = clean(first_non_empty(selected_row, "notice_period", "?묒닔湲곌컙", "period")) or "-"
    summary_text = clean(first_non_empty(selected_row, "_queue_analysis", "summary", "_queue_project_name")) or "?곌껐??RFP 遺꾩꽍??꾩쭅 ?놁뒿?덈떎."
    keywords = _extract_dashboard_keywords(selected_row)
    status_text = normalize_notice_status_label(first_non_empty(selected_row, "status", "rcve_status", "怨듦퀬?곹깭")) or "-"
    recommendation_text = clean(first_non_empty(selected_row, "_queue_recommendation", "recommendation")) or "蹂댄넻"
    top_badges = "".join(
        [
            '<span class="summary-panel-type">Notice</span>',
            _pill_html(recommendation_text),
            _pill_html(status_text, kind="deadline"),
        ]
    )
    detail_target_id = clean(first_non_empty(selected_row, "怨듦퀬ID", "notice_id"))
    current_value = clean(first_non_empty(selected_row, "review_status", "寃?좎뿬遺", "寃??щ?"))
    source_line = " / ".join(
        part
        for part in [
            clean(first_non_empty(selected_row, "留ㅼ껜", "source_label", "source_site")) or (source_key or "IRIS").upper(),
            clean(first_non_empty(selected_row, "?꾨Ц湲곌?", "agency", "?대떦遺??)),
        ]
        if clean(part) and part != "-"
    )
    budget_text = clean(first_non_empty(selected_row, "_queue_budget", "budget", "?덉궛")) or "-"
    deadline_text = format_dashboard_deadline_badge(period_text, status_text) or "-"
    detail_link = resolve_external_detail_link(selected_row, source_key=source_key)
    keyword_html = "".join(f'<span class="summary-panel-keyword">{escape(keyword)}</span>' for keyword in keywords[:6])

    st.markdown(
        (
            f'<div class="summary-panel-badges">{top_badges}</div>'
            f'<div class="summary-panel-title">{escape(title_text)}</div>'
            f'<div class="summary-panel-source">{escape(source_line or "-")}</div>'
            '<div class="summary-panel-meta-grid">'
            f'<div><div class="summary-panel-meta-label">湲곌컙 / D-day</div><div class="summary-panel-meta-value">{escape(period_text)} 쨌 {escape(deadline_text)}</div></div>'
            f'<div><div class="summary-panel-meta-label">?덉궛</div><div class="summary-panel-meta-value">{escape(budget_text)}</div></div>'
            '</div>'
            f'<div class="summary-panel-copy">{escape(summary_text)}</div>'
            f'<div class="summary-panel-keywords">{keyword_html}</div>'
        ),
        unsafe_allow_html=True,
    )

    favorite_col, detail_col = st.columns(2, gap="small")
    with favorite_col:
        render_favorite_scrap_button(
            notice_id=detail_target_id,
            current_value=current_value,
            source_key=source_key,
            notice_title=title_text,
            button_key=f"{panel_key}_favorite_{detail_target_id}",
            compact=True,
            use_container_width=True,
        )
    with detail_col:
        if st.button("Notice ?곸꽭 蹂닿린", key=f"{panel_key}_detail", type="primary", use_container_width=True):
            navigate_to_notice_detail(source_key, detail_target_id)

    st.markdown('<div style="height:0.45rem"></div>', unsafe_allow_html=True)
    _render_same_tab_link_button(
        "?먮Ц怨듦퀬 ?닿린",
        detail_link,
        kind="secondary",
        key=f"{panel_key}_origin_{detail_target_id}",
    )
    st.markdown("</div>", unsafe_allow_html=True)


def _render_dashboard_summary_panel(
    opportunity_rows: pd.DataFrame,
    notice_rows: pd.DataFrame,
) -> None:
    selection = _dashboard_selection_state()
    if not clean(selection.get("type")):
        return
    selected_type, selected_row = _resolve_dashboard_selected_row(opportunity_rows, notice_rows)
    if selected_row is None:
        return
    if selected_type == "rfp":
        _render_rfp_preview_panel(
            selected_row,
            panel_key="dashboard_rfp_preview",
            empty_title="移대뱶??怨듦퀬瑜??좏깮?섎㈃ ?붿빟??대┰?덈떎.",
            empty_copy="Dashboard 蹂몃Ц? 洹몃?濡??좎??梨??곗륫 ?⑤꼸?먯꽌 ?듭떖 ?뺣낫留?癒쇱? 蹂닿퀬, ?꾩슂??寃쎌슦?먮쭔 ?곸꽭 ?섏씠吏濡??대룞??덉뒿?덈떎.",
            close_callback=lambda: (_clear_dashboard_selection(), st.rerun()),
        )
        return
    _render_notice_preview_panel(
        selected_row,
        panel_key="dashboard_notice_preview",
        empty_title="移대뱶??怨듦퀬瑜??좏깮?섎㈃ ?붿빟??대┰?덈떎.",
        empty_copy="Dashboard 蹂몃Ц? 洹몃?濡??좎??梨??곗륫 ?⑤꼸?먯꽌 ?듭떖 ?뺣낫留?癒쇱? 蹂닿퀬, ?꾩슂??寃쎌슦?먮쭔 ?곸꽭 ?섏씠吏濡??대룞??덉뒿?덈떎.",
        close_callback=lambda: (_clear_dashboard_selection(), st.rerun()),
    )


def _build_dashboard_notice_inbox_rows(
    datasets: dict[str, pd.DataFrame],
    source_datasets: dict[str, object] | None,
) -> pd.DataFrame:
    rows = build_crawled_notice_collection(datasets, source_datasets)
    if rows.empty:
        return rows
    working = rows.copy()
    working["_dashboard_notice_id"] = series_from_candidates(working, ["怨듦퀬ID", "notice_id"]).fillna("").astype(str).str.strip()
    working = working[working["_notice_scope"].fillna("").astype(str).str.strip().ne("archive")].copy()
    opportunity_index = build_dashboard_opportunity_index(datasets, source_datasets)
    if not opportunity_index.empty:
        indexed = opportunity_index.copy()
        indexed["_notice_join_id"] = indexed["Notice ID"].fillna("").astype(str).str.strip()
        indexed["_is_positive"] = build_positive_recommendation_mask(indexed).astype(int)
        indexed = indexed.sort_values(
            by=["_is_positive", "Score", "_sort_date", "Project"],
            ascending=[False, False, False, True],
            na_position="last",
        )
        top_rows = indexed.drop_duplicates(subset=["_notice_join_id"], keep="first")[
            ["_notice_join_id", "Recommendation", "Budget", "Reason"]
        ].rename(
            columns={
                "_notice_join_id": "_dashboard_notice_id",
                "Recommendation": "_queue_recommendation",
                "Budget": "_queue_budget",
                "Reason": "_queue_analysis",
            }
        )
        working = working.merge(top_rows, on="_dashboard_notice_id", how="left")
    for column in ["_queue_recommendation", "_queue_budget", "_queue_analysis"]:
        if column not in working.columns:
            working[column] = ""
        working[column] = working[column].fillna("").astype(str).str.strip()
    return working.sort_values(by=["_sort_date", "留ㅼ껜", "怨듦퀬紐?], ascending=[False, True, True], na_position="last")


def _render_dashboard_top_rfp_cards(
    rows: pd.DataFrame,
    *,
    selected_item_id: str = "",
    on_select=None,
    visible_count: int = 5,
) -> None:
    if rows.empty:
        st.info("?쒖떆??異붿쿇 Opportunity媛 ?놁뒿?덈떎.")
        return

    window = rows.head(visible_count).copy()
    cols = st.columns(len(window), gap="medium")
    for column, (_, row), rank in zip(cols, window.iterrows(), range(1, len(window) + 1)):
        ctx = _queue_row_context(row)
        row_id = clean(first_non_empty(row, "_row_id", "Row ID"))
        is_active = _is_selected_dashboard_rfp(row) if not clean(selected_item_id) else clean(selected_item_id) == row_id
        keywords = "".join(
            f'<span class="rfp-card-keyword">{escape(keyword)}</span>'
            for keyword in _extract_dashboard_keywords(row, limit=3)
        )
        badges = "".join([_pill_html(ctx["recommendation"]), _pill_html(ctx["score"], kind="score")])
        with column:
            st.markdown(
                (
                    f'<div class="rfp-card{" is-active" if is_active else ""}">'
                    '<div class="rfp-card-topline">'
                    f'<div class="rfp-card-rank">{rank}</div>'
                    f'<div class="rfp-card-badges">{badges}</div>'
                    '</div>'
                    f'<div class="rfp-card-title">{escape(truncate_text(ctx["project"], max_chars=64))}</div>'
                    f'<div class="rfp-card-notice">{escape(truncate_text(ctx["notice"], max_chars=82))}</div>'
                    f'<div class="rfp-card-analysis">{escape(ctx["reason"])}</div>'
                    f'<div class="rfp-card-keywords">{keywords}</div>'
                    '<div class="rfp-card-meta">'
                    f'<div><div class="rfp-card-meta-label">湲곌?</div><div class="rfp-card-meta-value">{escape(ctx["agency"])}</div></div>'
                    f'<div><div class="rfp-card-meta-label">湲곌컙 / D-day</div><div class="rfp-card-meta-value">{escape(ctx["period"])} / {escape(ctx["deadline"])}</div></div>'
                    f'<div><div class="rfp-card-meta-label">?덉궛</div><div class="rfp-card-meta-value">{escape(ctx["budget"])}</div></div>'
                    f'<div><div class="rfp-card-meta-label">異쒖쿂</div><div class="rfp-card-meta-value">{escape(ctx["source"])}</div></div>'
                    '</div>'
                    '</div>'
                ),
                unsafe_allow_html=True,
            )
            st.markdown('<div class="rfp-card-action-slot">', unsafe_allow_html=True)
            if st.button(
                "?좏깮?? if is_active else "?붿빟 蹂닿린",
                key=f"dashboard_recommended_rfp_select_{rank}",
                type="primary" if is_active else "secondary",
                use_container_width=True,
            ):
                if callable(on_select):
                    on_select(row)
                else:
                    _set_dashboard_rfp_selection(row)
                st.rerun()
            st.markdown("</div>", unsafe_allow_html=True)


def _render_dashboard_recent_notice_inbox(rows: pd.DataFrame, *, limit: int = 12) -> None:
    if rows.empty:
        st.info("理쒓렐 ?쒖떆??怨듦퀬媛 ?놁뒿?덈떎.")
        return

    st.markdown(
        '<div class="notice-row-shell"><div class="notice-row-head"><div>?곹깭</div><div>異붿쿇?щ?</div><div>怨듦퀬紐?/div><div>湲곌?</div><div>?깅줉??/div><div>D-day</div><div>?덉궛</div><div>?붿빟</div><div>愿?ш났怨?/div></div>',
        unsafe_allow_html=True,
    )
    for idx, (_, row) in enumerate(rows.head(limit).iterrows(), start=1):
        source_key = resolve_route_source_key_for_row(row, source_key=row.get("source_key"))
        is_active = _is_selected_dashboard_notice(row)
        status_text = normalize_notice_status_label(first_non_empty(row, "status", "rcve_status", "怨듦퀬?곹깭")) or "-"
        recommendation_text = clean(row.get("_queue_recommendation")) or "-"
        title_text = clean(first_non_empty(row, "怨듦퀬紐?, "notice_title")) or "-"
        agency_text = clean(first_non_empty(row, "?꾨Ц湲곌?", "agency", "?대떦遺??)) or "-"
        notice_date = clean(first_non_empty(row, "registered_at", "怨듦퀬?쇱옄", "ancm_de")) or "-"
        period_text = clean(first_non_empty(row, "notice_period", "?묒닔湲곌컙", "period")) or ""
        budget_text = clean(first_non_empty(row, "_queue_budget", "budget", "?덉궛")) or "-"
        summary_text = clean(first_non_empty(row, "_queue_analysis", "_queue_project_name")) or "?곌껐??RFP 遺꾩꽍??꾩쭅 ?놁뒿?덈떎."
        dday_text = format_dashboard_deadline_badge(period_text, status_text) or "-"
        notice_id = clean(first_non_empty(row, "怨듦퀬ID", "notice_id"))
        st.markdown(f'<div class="notice-row-body{" is-active" if is_active else ""}">', unsafe_allow_html=True)
        row_cols = st.columns([1.0, 1.1, 3.0, 1.6, 1.0, 0.9, 1.2, 2.3, 0.9], gap="small")
        with row_cols[0]:
            st.markdown(_pill_html(status_text, kind="deadline"), unsafe_allow_html=True)
        with row_cols[1]:
            st.markdown(_pill_html(recommendation_text), unsafe_allow_html=True)
        with row_cols[2]:
            st.markdown(f'<div class="notice-row-title">{escape(truncate_text(title_text, max_chars=78))}</div>', unsafe_allow_html=True)
            if notice_id and st.button(
                "?좏깮?? if is_active else "?붿빟 蹂닿린",
                key=f"dashboard_notice_open_{idx}",
                type="primary" if is_active else "secondary",
                use_container_width=True,
            ):
                _set_dashboard_notice_selection(row)
                st.rerun()
        with row_cols[3]:
            st.markdown(f'<div class="notice-row-meta">{escape(agency_text)}</div>', unsafe_allow_html=True)
        with row_cols[4]:
            st.markdown(f'<div class="notice-row-meta">{escape(notice_date)}</div>', unsafe_allow_html=True)
        with row_cols[5]:
            st.markdown(f'<div class="notice-row-meta">{escape(dday_text)}</div>', unsafe_allow_html=True)
        with row_cols[6]:
            st.markdown(f'<div class="notice-row-meta">{escape(budget_text)}</div>', unsafe_allow_html=True)
        with row_cols[7]:
            st.markdown(f'<div class="notice-row-summary">{escape(summary_text)}</div>', unsafe_allow_html=True)
        with row_cols[8]:
            render_favorite_scrap_button(
                notice_id=notice_id,
                current_value=clean(row.get("review_status") or row.get("寃?좎뿬遺")),
                source_key=source_key or "iris",
                notice_title=title_text,
                button_key=f"dashboard_notice_favorite_{idx}",
                compact=True,
                icon_only=True,
                use_container_width=False,
            )
        st.markdown("</div>", unsafe_allow_html=True)
    st.markdown("</div>", unsafe_allow_html=True)


def _render_compact_public_dashboard_workspace(
    datasets: dict[str, pd.DataFrame],
    source_datasets: dict[str, object] | None,
) -> None:
    opportunity_index = build_dashboard_opportunity_index(datasets, source_datasets)
    if not opportunity_index.empty:
        opportunity_index = opportunity_index.sort_values(
            by=["Score", "_sort_date", "Project"],
            ascending=[False, False, True],
            na_position="last",
        )
    recommended_rows = (
        opportunity_index[build_positive_recommendation_mask(opportunity_index)].copy()
        if not opportunity_index.empty
        else pd.DataFrame()
    )
    notice_rows = _build_dashboard_notice_inbox_rows(datasets, source_datasets)
    page_size_key = "dashboard_notice_inbox_page_size"
    st.session_state.setdefault(page_size_key, 10)
    notice_page_size = int(st.session_state.get(page_size_key, 10) or 10)
    preview_rows = notice_rows.head(notice_page_size).copy() if not notice_rows.empty else pd.DataFrame()

    selected_type, selected_row = _resolve_dashboard_selected_row(opportunity_index, notice_rows)
    has_summary_panel = bool(clean(selected_type) and selected_row is not None)
    current_user_label = get_current_user_label()

    st.markdown('<div class="dashboard-shell">', unsafe_allow_html=True)
    st.markdown(
        (
            '<div class="dashboard-greeting">'
            '<div>'
            f'<div class="dashboard-greeting-title">{escape(current_user_label)}?? ?ㅻ뒛??醫뗭? 湲고쉶瑜?李얠븘蹂댁꽭??</div>'
            '<div class="dashboard-greeting-copy">AI 遺꾩꽍 湲곕컲?쇰줈 ?좊퀎??R&amp;D Opportunity瑜?異붿쿇?쒕┰?덈떎.</div>'
            '</div>'
            '<div class="dashboard-greeting-meta">'
            f'<span class="dashboard-greeting-pill">異붿쿇 RFP {len(recommended_rows.head(5))}嫄?/span>'
            f'<span class="dashboard-greeting-pill">理쒓렐 怨듦퀬 {len(preview_rows)}嫄?/span>'
            f'<span class="dashboard-greeting-pill">遺꾩꽍 ?꾨즺 {len(opportunity_index)}嫄?/span>'
            '</div>'
            '</div>'
        ),
        unsafe_allow_html=True,
    )
    _render_dashboard_kpi_cards(recommended_rows, notice_rows)

    if has_summary_panel:
        workspace_col, summary_col = st.columns([4.9, 1.8], gap="large")
    else:
        workspace_col, summary_col = st.container(), None

    with workspace_col:
        top_left, top_right = st.columns([6, 1.8], gap="medium")
        with top_left:
            st.markdown(
                '<div class="oppty-section-header"><div><div class="oppty-section-title">?뵦 異붿쿇 RFP Top 5</div><div class="oppty-section-subtitle">異붿쿇 移대뱶?먯꽌 ?듭떖 ?뺣낫留?癒쇱? 蹂닿퀬, ?꾩슂??寃쎌슦?먮쭔 ?곗륫 Summary Panel?먯꽌 ?곸꽭 寃?좊줈 ?댁뼱吏묐땲??</div></div></div>',
                unsafe_allow_html=True,
            )
        with top_right:
            st.markdown('<div style="height:1rem"></div>', unsafe_allow_html=True)
            if st.button("?꾩껜 RFP Queue 蹂닿린 >", key="dashboard_to_rfp_queue", use_container_width=True):
                navigate_to_route_state(route_core.build_rfp_queue_route(), push=True)
        _render_dashboard_top_rfp_cards(recommended_rows, selected_item_id="", on_select=None, visible_count=5)

        notice_left, notice_right = st.columns([6, 2.1], gap="medium")
        with notice_left:
            st.markdown(
                '<div class="oppty-section-header"><div><div class="oppty-section-title">Recent Notice Inbox</div><div class="oppty-section-subtitle">理쒓렐 怨듦퀬瑜?compact table濡?鍮좊Ⅴ寃??묎퀬, ?꾩슂??怨듦퀬留?Summary Panel濡??뺤씤?⑸땲??</div></div></div>',
                unsafe_allow_html=True,
            )
        with notice_right:
            notice_action_col, notice_size_col = st.columns([2.3, 1.2], gap="small")
            with notice_action_col:
                st.markdown('<div style="height:1rem"></div>', unsafe_allow_html=True)
                if st.button("?꾩껜 Notice Queue 蹂닿린 >", key="dashboard_to_notice_queue", use_container_width=True):
                    navigate_to_route_state(route_core.build_notice_queue_route(), push=True)
            with notice_size_col:
                st.selectbox(
                    "Notice page size",
                    options=[10, 15, 20],
                    key=page_size_key,
                    label_visibility="collapsed",
                )
        _render_dashboard_recent_notice_inbox(preview_rows, limit=notice_page_size)

    if summary_col is not None:
        with summary_col:
            _render_dashboard_summary_panel(opportunity_index, notice_rows)

    st.markdown("</div>", unsafe_allow_html=True)


def _render_recommended_opportunity_cards(
    rows: pd.DataFrame,
    *,
    page_key: str,
    carousel_key: str,
    visible_count: int = 4,
    show_rank: bool = False,
    selected_item_id: str = "",
    on_select=None,
) -> None:
    if page_key == "dashboard":
        _render_dashboard_top_rfp_cards(
            rows,
            selected_item_id=selected_item_id,
            on_select=on_select,
            visible_count=max(visible_count, 5),
        )
        return
    if rows.empty:
        st.info("?쒖떆??異붿쿇 Opportunity媛 ?놁뒿?덈떎.")
        return
    max_start = max(len(rows) - visible_count, 0)
    start = int(st.session_state.get(carousel_key, 0))
    start = max(0, min(start, max_start))
    nav_left, nav_mid, nav_right = st.columns([1, 4, 1])
    with nav_left:
        if st.button("?", key=f"{carousel_key}_prev", use_container_width=True, disabled=start <= 0):
            start = max(0, start - visible_count)
    with nav_mid:
        st.markdown(f'<div class="oppty-carousel-summary">{start + 1}-{min(start + visible_count, len(rows))} / {len(rows)}</div>', unsafe_allow_html=True)
    with nav_right:
        if st.button("??, key=f"{carousel_key}_next", use_container_width=True, disabled=start >= max_start):
            start = min(max_start, start + visible_count)
    st.session_state[carousel_key] = start
    window = rows.iloc[start:start + visible_count].copy()
    cols = st.columns(len(window), gap="medium")
    for column, (_, row), rank in zip(cols, window.iterrows(), range(start + 1, start + len(window) + 1)):
        ctx = _queue_row_context(row)
        source_key = resolve_route_source_key_for_row(row)
        row_id = clean(first_non_empty(row, "_row_id", "Row ID"))
        is_active = _is_selected_dashboard_rfp(row) if page_key == "dashboard" else clean(selected_item_id) == row_id
        badges = "".join([_pill_html(ctx["recommendation"]), _pill_html(ctx["score"], kind="score"), _pill_html(ctx["deadline"], kind="deadline"), _pill_html(ctx["source"], kind="recommendation")])
        keywords = "".join(f'<span class="rfp-card-keyword">{escape(keyword)}</span>' for keyword in _extract_dashboard_keywords(row))
        with column:
            st.markdown(
                (
                    f'<div class="rfp-card{" is-active" if is_active else ""}">'
                    '<div class="rfp-card-topline">'
                    f'{"<div class=\"rfp-card-rank\">%s</div>" % rank if show_rank else "<div></div>"}'
                    f'<div class="rfp-card-badges">{badges}</div>'
                    '</div>'
                    f'<div class="rfp-card-title">{escape(truncate_text(ctx["project"], max_chars=70))}</div>'
                    f'<div class="rfp-card-notice">{escape(truncate_text(ctx["notice"], max_chars=96))}</div>'
                    f'<div class="rfp-card-analysis">{escape(ctx["reason"])}</div>'
                    f'<div class="rfp-card-keywords">{keywords}</div>'
                    '<div class="rfp-card-meta">'
                    f'<div><div class="rfp-card-meta-label">湲곌?</div><div class="rfp-card-meta-value">{escape(ctx["agency"])}</div></div>'
                    f'<div><div class="rfp-card-meta-label">湲곌컙</div><div class="rfp-card-meta-value">{escape(ctx["period"])}</div></div>'
                    f'<div><div class="rfp-card-meta-label">?덉궛</div><div class="rfp-card-meta-value">{escape(ctx["budget"])}</div></div>'
                    f'<div><div class="rfp-card-meta-label">?뚯뒪</div><div class="rfp-card-meta-value">{escape(ctx["source"])}</div></div>'
                    '</div>'
                    '</div>'
                ),
                unsafe_allow_html=True,
            )
            action_cols = st.columns(3, gap="small")
            detail_link = resolve_external_detail_link(row, source_key=source_key)
            with action_cols[0]:
                _render_same_tab_link_button(
                    "?먮Ц怨듦퀬",
                    detail_link,
                    kind="secondary",
                    key=f"{carousel_key}_origin_disabled_{rank}",
                )
            with action_cols[1]:
                if st.button(
                    "?붿빟 蹂닿린" if not is_active else "?좏깮??,
                    key=f"{carousel_key}_select_{rank}",
                    type="primary" if is_active else "secondary",
                    use_container_width=True,
                ):
                    if callable(on_select):
                        on_select(row)
                    else:
                        _set_dashboard_rfp_selection(row)
                    st.rerun()
            with action_cols[2]:
                render_favorite_scrap_button(
                    notice_id=clean(row.get("notice_id") or row.get("Notice ID")),
                    current_value=clean(row.get("review_status") or row.get("Review")),
                    source_key=source_key or "iris",
                    notice_title=clean(row.get("notice_title") or row.get("Notice Title")),
                    button_key=f"{carousel_key}_favorite_{rank}",
                )


def _render_recent_notice_inbox(rows: pd.DataFrame, *, limit: int = 12) -> None:
    _render_dashboard_recent_notice_inbox(rows, limit=limit)
    return
    if rows.empty:
        st.info("理쒓렐 ?쒖떆??怨듦퀬媛 ?놁뒿?덈떎.")
        return
    st.markdown(
        '<div class="notice-row-shell"><div class="notice-row-head"><div>?곹깭</div><div>異붿쿇</div><div>異쒖쿂</div><div>怨듦퀬紐?/div><div>?깅줉??/div><div>D-day</div><div>?덉궛</div><div>?붿빟</div><div>愿??/div></div>',
        unsafe_allow_html=True,
    )
    for idx, (_, row) in enumerate(rows.head(limit).iterrows(), start=1):
        source_key = resolve_route_source_key_for_row(row, source_key=row.get("source_key"))
        is_active = _is_selected_dashboard_notice(row)
        status_text = normalize_notice_status_label(first_non_empty(row, "status", "rcve_status", "怨듦퀬?곹깭")) or "-"
        recommendation_text = clean(row.get("_queue_recommendation")) or "-"
        title_text = clean(first_non_empty(row, "怨듦퀬紐?, "notice_title")) or "-"
        source_text = clean(first_non_empty(row, "留ㅼ껜", "source_label", "source_site")) or (source_key or "IRIS").upper()
        agency_text = clean(first_non_empty(row, "?꾨Ц湲곌?", "agency", "?대떦遺??)) or "-"
        notice_date = clean(first_non_empty(row, "registered_at", "怨듦퀬?쇱옄", "ancm_de")) or "-"
        period_text = clean(first_non_empty(row, "notice_period", "?묒닔湲곌컙", "period")) or ""
        budget_text = clean(first_non_empty(row, "_queue_budget", "budget", "?덉궛")) or "-"
        summary_text = clean(first_non_empty(row, "_queue_analysis", "_queue_project_name")) or "?곌껐??RFP 遺꾩꽍??꾩쭅 ?놁뒿?덈떎."
        dday_text = format_dashboard_deadline_badge(period_text, status_text) or "-"
        notice_id = clean(first_non_empty(row, "怨듦퀬ID", "notice_id"))
        row_cols = st.columns([1.1, 1.1, 1.2, 3.4, 1.2, 1.0, 1.1, 2.7, 0.9], gap="small")
        with row_cols[0]:
            st.markdown(_pill_html(status_text, kind="deadline"), unsafe_allow_html=True)
        with row_cols[1]:
            st.markdown(_pill_html(recommendation_text), unsafe_allow_html=True)
        with row_cols[2]:
            st.markdown(f'<div class="notice-row-meta">{escape(source_text)} 쨌 {escape(agency_text)}</div>', unsafe_allow_html=True)
        with row_cols[3]:
            st.markdown(f'<div class="notice-row-title">{escape(truncate_text(title_text, max_chars=84))}</div>', unsafe_allow_html=True)
            if notice_id and st.button(
                "?붿빟 蹂닿린" if not is_active else "?좏깮??,
                key=f"dashboard_notice_open_{idx}",
                type="primary" if is_active else "secondary",
                use_container_width=False,
            ):
                _set_dashboard_notice_selection(row)
                st.rerun()
        with row_cols[4]:
            st.markdown(f'<div class="notice-row-meta">{escape(notice_date)}</div>', unsafe_allow_html=True)
        with row_cols[5]:
            st.markdown(f'<div class="notice-row-meta">{escape(dday_text)}</div>', unsafe_allow_html=True)
        with row_cols[6]:
            st.markdown(f'<div class="notice-row-meta">{escape(budget_text)}</div>', unsafe_allow_html=True)
        with row_cols[7]:
            st.markdown(f'<div class="notice-row-summary">{escape(summary_text)}</div>', unsafe_allow_html=True)
        with row_cols[8]:
            render_favorite_scrap_button(
                notice_id=notice_id,
                current_value=clean(row.get("review_status") or row.get("寃?좎뿬遺")),
                source_key=source_key or "iris",
                notice_title=title_text,
                button_key=f"dashboard_notice_favorite_{idx}",
                compact=True,
                icon_only=True,
                use_container_width=False,
            )
    st.markdown('</div>', unsafe_allow_html=True)


def render_dashboard_source(
    source_config: SourceRouteConfig,
    mode_config: AppModeConfig,
    datasets: dict[str, pd.DataFrame],
    source_datasets: dict[str, object] | None,
    *,
    show_internal_tabs: bool = True,
) -> None:
    render_dashboard_page_module(
        st,
        source_config,
        mode_config,
        datasets,
        source_datasets,
        show_internal_tabs=show_internal_tabs,
        api=sys.modules[__name__],
    )

def render_iris_source(
    source_config: SourceRouteConfig,
    mode_config: AppModeConfig,
    datasets: dict[str, pd.DataFrame],
    source_datasets: dict[str, object] | None = None,
    *,
    show_internal_tabs: bool = True,
) -> None:
    del source_config
    current_route = route_core.get_current_route(route_core.build_rfp_queue_route())
    current_page_key = normalize_route_page_key(current_route.get("page")) or mode_config.default_iris_page

    if current_page_key not in mode_config.valid_iris_pages:
        navigate_to_route_state(route_core.build_rfp_queue_route(), push=False)
        st.rerun()

    if show_internal_tabs:
        current_page_key = render_page_tabs(
            current_page_key,
            list(mode_config.iris_tabs),
            key=mode_config.iris_tab_key,
        )

    if current_page_key in {"opportunity", "rfp_queue"}:
        render_rfp_queue_page_module(
            st,
            datasets,
            source_datasets,
            api=sys.modules[__name__],
        )
        return

    if current_page_key == "notice_queue":
        render_notice_queue_page(datasets, source_datasets)
        return

    render_iris_page(current_page_key, datasets)


def _normalize_workspace_shell_route(route: dict[str, object]) -> dict[str, object]:
    normalized = route_core.normalize_route(route)
    current_page = normalize_route_page_key(normalized.get("page"))
    current_view = clean(normalized.get("view")) or "list"
    current_item_id = clean(normalized.get("item_id"))
    current_filters = dict(normalized.get("filters") or {})
    current_page_no = int(normalized.get("page_no") or 1)
    current_page_size = int(normalized.get("page_size") or 20)
    current_source_key = clean(normalized.get("source_key"))

    if current_page == "dashboard":
        return route_core.build_dashboard_route(
            view=current_view,
            filters=current_filters,
        )
    if current_page == "rfp_queue":
        return route_core.build_rfp_queue_route(
            filters=current_filters,
            page_no=current_page_no,
            page_size=current_page_size,
            view=current_view,
            item_id=current_item_id,
            source_key=current_source_key if current_source_key in {"iris", "tipa", "nipa"} else "iris",
        )
    if current_page == "notice_queue":
        return route_core.build_notice_queue_route(
            filters=current_filters,
            page_no=current_page_no,
            page_size=current_page_size,
            view=current_view,
            item_id=current_item_id,
            source_key=current_source_key if current_source_key in {"iris", "tipa", "nipa"} else "iris",
        )
    if current_page == "favorites":
        return route_core.build_favorites_route(
            filters=current_filters,
            page_no=current_page_no,
            page_size=current_page_size,
            view=current_view,
            item_id=current_item_id,
            source_key=current_source_key or "favorites",
        )
    return normalized


SOURCE_RENDERERS = {
    "dashboard": render_dashboard_source,
    "notices": render_iris_source,
    "iris": render_iris_source,
    "tipa": render_tipa_source,
    "nipa": render_nipa_source,
    "proposal": render_proposal_source,
    "operations": render_operations_source,
    "favorites": render_favorites_source,
}


def render_selected_source(
    source_key: str,
    *,
    source_config: SourceRouteConfig | None,
    mode_config: AppModeConfig,
    datasets: dict[str, pd.DataFrame],
    source_datasets: dict[str, object] | None,
    show_internal_tabs: bool = True,
) -> None:
    renderer_lookup_key = source_config.renderer_key if source_config else source_key
    if source_key == "notices":
        renderer = render_notices_source
    else:
        renderer = SOURCE_RENDERERS.get(renderer_lookup_key) or SOURCE_RENDERERS.get(source_key)
    if renderer is None:
        fallback_config = source_config or SourceRouteConfig("iris", "IRIS", mode_config.default_iris_page, False, "iris")
        render_iris_source(fallback_config, mode_config, datasets, show_internal_tabs=show_internal_tabs)
        return
    active_config = source_config or SourceRouteConfig(source_key, source_key, mode_config.default_iris_page, False, source_key)
    renderer(active_config, mode_config, datasets, source_datasets, show_internal_tabs=show_internal_tabs)


IRIS_DETAIL_BASE_URL = "https://www.iris.go.kr/contents/retrieveBsnsAncmView.do"


def build_iris_detail_link(notice_id: object, status_key: object = "") -> str:
    notice_id_text = clean(notice_id)
    if not notice_id_text:
        return ""
    params = {
        "ancmId": notice_id_text,
        "ancmStsCd": clean(status_key) or "ancmIng",
    }
    return f"{IRIS_DETAIL_BASE_URL}?{urlencode(params)}"


def row_first_non_empty(row: dict | pd.Series, *keys: str) -> str:
    for key in keys:
        value = clean(row.get(key))
        if value:
            return value
    return ""


def resolve_external_detail_link(row: dict | pd.Series, source_key: str = "") -> str:
    link = row_first_non_empty(row, "?곸꽭留곹겕", "detail_link")
    normalized_source = clean(
        source_key
        or row.get("_source_key")
        or row.get("source_site")
        or row.get("異쒖쿂?ъ씠??)
    ).lower()
    if normalized_source in {"tipa", "mss", "nipa"}:
        return link

    notice_id = row_first_non_empty(row, "怨듦퀬ID", "notice_id")
    if not notice_id:
        return link

    status_key = row_first_non_empty(row, "?곹깭??, "status_key")
    if not link or (IRIS_DETAIL_BASE_URL in link and "ancmStsCd=" not in link):
        return build_iris_detail_link(notice_id, status_key)
    return link


def normalize_notice_id_for_match(value: object) -> str:
    text = clean(value)
    if not text:
        return ""
    if text.isdigit():
        return text.lstrip("0") or "0"
    return text


def normalize_display_value(value: object) -> str:
    if value is None:
        return ""
    if isinstance(value, dict):
        try:
            return json.dumps(value, ensure_ascii=False)
        except Exception:
            return clean(value)
    if isinstance(value, (list, tuple, set)):
        items = [clean(item) for item in value if clean(item)]
        return " | ".join(items)
    return clean(value)


def text_printable_ratio(text: str) -> float:
    if not text:
        return 0.0

    printable = 0
    for ch in text:
        code = ord(ch)
        if ch in "\n\r\t":
            printable += 1
        elif 32 <= code <= 126:
            printable += 1
        elif 0xAC00 <= code <= 0xD7A3:
            printable += 1
        elif 0x3131 <= code <= 0x318E:
            printable += 1
    return printable / max(len(text), 1)


def looks_garbled_text(value: object) -> bool:
    text = normalize_display_value(value)
    if not text:
        return False

    markers = [
        "root entry",
        "fileheader",
        "hwpsummaryinformation",
        "docinfo",
        "bodytext",
        "\x00",
    ]
    lowered = text.lower()
    if any(marker in lowered for marker in markers):
        return True

    control_count = sum(1 for ch in text if ord(ch) < 32 and ch not in "\n\r\t")
    if control_count >= 5:
        return True

    replacement_count = text.count("\ufffd")
    if replacement_count >= 10:
        return True
    if len(text) >= 50 and replacement_count / max(len(text), 1) > 0.02:
        return True

    if len(text) >= 120 and text_printable_ratio(text) < 0.7:
        return True

    return False


def sanitize_display_text(label: str, value: object) -> str:
    text = normalize_display_value(value)
    if not text:
        return ""

    if looks_garbled_text(text):
        normalized_label = normalize_display_value(label)
        lowered = normalized_label.lower()
        if any(
            keyword in normalized_label
            for keyword in [
                "\ud14d\uc2a4\ud2b8",
                "\ubbf8\ub9ac\ubcf4\uae30",
                "\uadfc\uac70",
                "\ucda9\ub3cc",
            ]
        ):
            return "\uc6d0\ubb38 \ucd94\ucd9c \ud488\uc9c8\uc774 \ub0ae\uc544 \ud45c\uc2dc\ud558\uc9c0 \uc54a\uc2b5\ub2c8\ub2e4."
        if any(keyword in lowered for keyword in ["text", "preview", "evidence", "conflict", "raw"]):
            return "Hidden due to low-quality extracted text."
        return ""

    return text


def sanitize_display_title(value: object, fallback: str = "\uc0c1\uc138 \uc815\ubcf4") -> str:
    text = sanitize_display_text("title", value)
    return text or fallback


def series_from_candidates(df: pd.DataFrame, candidates: list[str]) -> pd.Series:
    if df.empty:
        return pd.Series(dtype="object")

    result = pd.Series([""] * len(df), index=df.index, dtype="object")
    for column in candidates:
        if column not in df.columns:
            continue
        values = df[column].fillna("").astype(str).str.strip()
        result = result.where(result.ne(""), values)
    return result


def truncate_text(value: str, max_chars: int = 140) -> str:
    text = clean(value)
    if len(text) <= max_chars:
        return text
    return f"{text[:max_chars].rstrip()}..."


LONG_ANALYSIS_LABELS = {
    "媛쒕뀗 諛?媛쒕컻 ?댁슜",
    "吏?먰븘?붿꽦(怨쇱젣 諛곌꼍)",
    "?쒖슜遺꾩빞",
    "吏?먭린媛?諛??덉궛쨌異붿쭊泥닿퀎",
    "異붿쿇 ?댁쑀",
}


def preview_max_chars_for_label(label: str) -> int:
    normalized = clean(label)
    if normalized in LONG_ANALYSIS_LABELS:
        return 900
    if "?덉궛" in normalized or normalized.lower() == "budget":
        return 80
    return 220


def split_preview_and_remainder(value: str, max_chars: int = 220) -> tuple[str, str]:
    text = clean(value)
    if len(text) <= max_chars:
        return text, ""
    preview = text[:max_chars].rstrip()
    remainder = text[len(preview):].lstrip()
    return preview, remainder


def extract_budget_summary(value: str, max_items: int = 3) -> str:
    text = clean(value)
    if not text:
        return ""

    matches = re.findall(r"\d[\d,]*(?:\.\d+)?\s*(?:議곗썝|?듭썝|泥쒕쭔??諛깅쭔??留뚯썝|??", text)
    unique_matches = []
    for match in matches:
        normalized = re.sub(r"\s+", "", clean(match))
        if normalized and normalized not in unique_matches:
            unique_matches.append(normalized)

    if unique_matches:
        return " / ".join(unique_matches[:max_items])

    if len(text) <= 30:
        return text
    return ""


def display_value_for_label(label: str, value: str) -> str:
    normalized_label = clean(label)
    text = sanitize_display_text(label, value)
    if not text:
        return ""

    if "?덉궛" in normalized_label or normalized_label.lower() == "budget":
        budget_summary = extract_budget_summary(text)
        if budget_summary:
            return budget_summary
        return truncate_text(text, max_chars=40)

    return truncate_text(text, max_chars=preview_max_chars_for_label(normalized_label))


def should_use_expandable_value(label: str, value: str) -> bool:
    text = sanitize_display_text(label, value)
    if not text:
        return False
    if len(text) <= preview_max_chars_for_label(label):
        return False
    normalized_label = clean(label)
    if "?덉궛" in normalized_label or normalized_label.lower() == "budget":
        return True
    return True


def compact_table_value(value: object, max_chars: int = 70) -> object:
    text = sanitize_display_text("", value)
    if not text:
        return ""
    if len(text) <= max_chars:
        return text
    return f"{text[:max_chars].rstrip()}..."


def parse_date_column(series: pd.Series) -> pd.Series:
    return pd.to_datetime(series.fillna("").astype(str).str.strip(), errors="coerce")


def to_numeric_column(series: pd.Series) -> pd.Series:
    return pd.to_numeric(series, errors="coerce").fillna(0)


PERIOD_END_RE = re.compile(r"(\d{4}[-.]\d{2}[-.]\d{2})\s*$")


def extract_period_end(value: object) -> pd.Timestamp:
    text = clean(value)
    if not text:
        return pd.NaT

    match = PERIOD_END_RE.search(text)
    if not match:
        return pd.NaT

    return pd.to_datetime(match.group(1).replace(".", "-"), errors="coerce")


def classify_notice_status_for_view(row: pd.Series) -> str:
    status_key = clean(row.get("status_key") or row.get("?곹깭??))
    period_text = clean(row.get("period") or row.get("?묒닔湲곌컙"))
    period_end = extract_period_end(period_text)
    period_start = pd.to_datetime(
        clean(period_text).split("~", 1)[0].strip().replace(".", "-") if "~" in clean(period_text) else "",
        errors="coerce",
    )
    today = pd.Timestamp.now().normalize()

    if status_key == "ancmIng":
        if pd.notna(period_start) and period_start.normalize() > today:
            return "?덉젙"
        if pd.notna(period_end) and period_end.normalize() < today:
            return "留덇컧"
        return "?묒닔以?
    if status_key == "ancmPre":
        return "?덉젙"
    if status_key in {"ancmCls", "ancmEnd"}:
        return "留덇컧"

    status_text = clean(row.get("rcve_status") or row.get("怨듦퀬?곹깭"))
    if "?묒닔以? in status_text or "怨듦퀬以? in status_text:
        if pd.notna(period_start) and period_start.normalize() > today:
            return "?덉젙"
        if pd.notna(period_end) and period_end.normalize() < today:
            return "留덇컧"
        return "?묒닔以?
    if "?덉젙" in status_text:
        return "?덉젙"
    if "留덇컧" in status_text:
        return "留덇컧"

    if pd.notna(period_start) and period_start.normalize() > today:
        return "?덉젙"
    if pd.notna(period_end) and period_end.normalize() < today:
        return "留덇컧"

    return status_text


def safe_mean(series: pd.Series) -> str:
    numeric = pd.to_numeric(series, errors="coerce")
    numeric = numeric.dropna()
    if numeric.empty:
        return "-"
    return f"{numeric.mean():.1f}"


def build_opportunity_row_id(row: pd.Series) -> str:
    document_id = clean(row.get("document_id"))
    if document_id:
        return document_id

    notice_id = clean(row.get("notice_id"))
    project_name = clean(row.get("project_name"))
    rfp_title = clean(row.get("rfp_title"))
    file_name = clean(row.get("file_name"))

    composite = " | ".join([value for value in [notice_id, project_name or rfp_title, file_name] if value])
    if composite:
        return composite
    return clean(row.get("notice_title")) or clean(row.get("怨듦퀬紐?))


def ensure_opportunity_row_ids(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df

    working = df.copy()
    working["_row_id"] = working.apply(build_opportunity_row_id, axis=1)
    missing_mask = working["_row_id"].fillna("").astype(str).str.strip().eq("")
    if missing_mask.any():
        working.loc[missing_mask, "_row_id"] = [
            f"row-{index}" for index in working.index[missing_mask]
        ]
    return working


def get_env(name: str, default: str = "") -> str:
    try:
        if name in st.secrets:
            return clean(st.secrets[name])
    except Exception:
        pass
    return clean(os.getenv(name, default))


def get_bool_env(name: str, default: bool = False) -> bool:
    value = get_env(name, "1" if default else "0").lower()
    return value in {"1", "true", "y", "yes", "on"}


def get_secret_mapping(name: str) -> dict:
    try:
        value = st.secrets.get(name)
        if isinstance(value, str):
            try:
                parsed = json.loads(value)
                return dict(parsed) if isinstance(parsed, dict) else {}
            except Exception:
                return {}
        if hasattr(value, "items"):
            return dict(value)
    except Exception:
        pass
    return {}


def get_secret_value(name: str):
    try:
        if name in st.secrets:
            return st.secrets[name]
    except Exception:
        pass
    return None


def normalize_auth_password_value(value) -> str:
    if isinstance(value, dict) or hasattr(value, "items"):
        try:
            mapping = dict(value)
        except Exception:
            mapping = {}
        for key in ("password_hash", "password", "secret"):
            password = clean(mapping.get(key))
            if password:
                return password
        return ""
    return clean(value)


def parse_key_value_auth_users(raw_value: str) -> dict[str, str]:
    users: dict[str, str] = {}
    for item in clean(raw_value).split(","):
        if not clean(item) or ":" not in item:
            continue
        user_id, password = item.split(":", 1)
        user_id = clean(user_id)
        if user_id:
            users[user_id] = normalize_auth_password_value(password)
    return users


def load_static_auth_users() -> dict[str, str]:
    for secret_name in ("app_users", "APP_USERS"):
        users = get_secret_mapping(secret_name)
        if users:
            return {
                clean(user_id): normalize_auth_password_value(password)
                for user_id, password in users.items()
                if clean(user_id) and normalize_auth_password_value(password)
            }

    raw_users = get_env("APP_USERS")
    if not raw_users:
        return {}
    try:
        parsed = json.loads(raw_users)
        if isinstance(parsed, dict):
            return {
                clean(user_id): normalize_auth_password_value(password)
                for user_id, password in parsed.items()
                if clean(user_id) and normalize_auth_password_value(password)
            }
    except Exception:
        pass
    return parse_key_value_auth_users(raw_users)


def parse_csv_values(raw_value: str) -> set[str]:
    return {clean(item) for item in clean(raw_value).split(",") if clean(item)}


def normalize_email_domain(email: str) -> str:
    email = clean(email).lower()
    if "@" not in email:
        return ""
    return clean(email.rsplit("@", 1)[-1]).lower()


def load_allowed_email_domains() -> set[str]:
    domains: set[str] = set()
    for secret_name in ("app_allowed_email_domains", "APP_ALLOWED_EMAIL_DOMAINS"):
        value = get_secret_value(secret_name)
        if isinstance(value, (list, tuple, set)):
            domains.update(normalize_email_domain(f"user@{item}") for item in value if clean(item))
        elif isinstance(value, str):
            domains.update(normalize_email_domain(f"user@{item}") for item in parse_csv_values(value))
    domains.update(normalize_email_domain(f"user@{item}") for item in parse_csv_values(get_env("APP_ALLOWED_EMAIL_DOMAINS")))
    return {domain for domain in domains if domain}


def hash_password(password: str) -> str:
    digest = hashlib.sha256(clean(password).encode("utf-8")).hexdigest()
    return f"sha256:{digest}"


def verify_password(password: str, stored_password: str) -> bool:
    password = clean(password)
    stored_password = normalize_auth_password_value(stored_password)
    if not stored_password:
        return False
    if stored_password.startswith("sha256:"):
        expected = stored_password.removeprefix("sha256:")
        actual = hashlib.sha256(password.encode("utf-8")).hexdigest()
        return hmac.compare_digest(actual.encode("utf-8"), expected.encode("utf-8"))
    return hmac.compare_digest(password.encode("utf-8"), stored_password.encode("utf-8"))


def get_auth_signing_secret() -> str:
    return (
        get_env("APP_AUTH_TOKEN_SECRET")
        or get_env("COOKIE_SECRET")
        or get_env("GOOGLE_SHEET_ID")
        or "crawler-hub-auth"
    )


def sign_auth_user_id(user_id: str) -> str:
    return hmac.new(
        get_auth_signing_secret().encode("utf-8"),
        clean(user_id).encode("utf-8"),
        hashlib.sha256,
    ).hexdigest()


def encode_auth_token(user_id: str) -> str:
    user_id = clean(user_id)
    payload = f"{user_id}:{sign_auth_user_id(user_id)}"
    return base64.urlsafe_b64encode(payload.encode("utf-8")).decode("ascii").rstrip("=")


def decode_auth_token(token: str) -> str:
    token = clean(token)
    if not token:
        return ""
    try:
        padded = token + "=" * (-len(token) % 4)
        payload = base64.urlsafe_b64decode(padded.encode("ascii")).decode("utf-8")
        user_id, signature = payload.rsplit(":", 1)
    except Exception:
        return ""
    expected = sign_auth_user_id(user_id)
    return clean(user_id) if hmac.compare_digest(signature, expected) else ""


def get_query_auth_token() -> str:
    value = st.query_params.get("auth", "")
    if isinstance(value, list):
        return clean(value[0]) if value else ""
    return clean(value)


def get_query_params_dict() -> dict[str, str]:
    params: dict[str, str] = {}
    try:
        keys = st.query_params.keys()
    except Exception:
        return params
    for key in keys:
        value = st.query_params.get(key, "")
        if isinstance(value, list):
            value = value[0] if value else ""
        params[clean(key)] = clean(value)
    return params


def get_current_auth_token() -> str:
    return clean(st.session_state.get("auth_token")) or get_query_auth_token()


def with_auth_params(params: dict[str, str]) -> dict[str, str]:
    params = {clean(key): clean(value) for key, value in dict(params).items()}
    token = get_current_auth_token()
    if token:
        params["auth"] = token
    return params


def initialize_route_state(default_route: dict[str, object]) -> dict[str, object]:
    query_params = get_query_params_dict()
    if not clean(query_params.get("source")) and not clean(query_params.get("page")):
        route_core.set_current_route(default_route)
        route_core.clear_route_stack()
        route = route_core.get_current_route(default_route)
        replace_query_params(with_auth_params(route_core.serialize_route(route)))
        return route

    route = route_core.init_route(
        default_route=default_route,
        query_params=query_params,
    )
    replace_query_params(with_auth_params(route_core.serialize_route(route)))
    return route


def get_current_route_dict(default_route: dict[str, object] | None = None) -> dict[str, object]:
    if default_route is None:
        return route_core.get_current_route()
    return route_core.get_current_route(default_route)


def update_current_route_state(**changes: object) -> dict[str, object]:
    route = route_core.update_current_route(**changes)
    replace_query_params(with_auth_params(route_core.serialize_route(route)))
    return route


def go_back_route(fallback_route: dict[str, object] | None = None) -> dict[str, object]:
    route = route_core.go_back(fallback_route)
    replace_query_params(with_auth_params(route_core.serialize_route(route)))
    return route


def replace_query_params(params: dict[str, str]) -> None:
    st.query_params.clear()
    clean_params = {clean(key): clean(value) for key, value in params.items() if clean(key)}
    if clean_params:
        st.query_params.update(clean_params)


def encode_return_route(params: dict[str, str]) -> str:
    allowed_keys = {"source", "page", "view", "id", "return_to"}
    payload = {
        clean(key): clean(value)
        for key, value in params.items()
        if clean(key) in allowed_keys and clean(value)
    }
    if not payload:
        return ""
    encoded = base64.urlsafe_b64encode(
        json.dumps(payload, ensure_ascii=True, separators=(",", ":")).encode("utf-8")
    ).decode("ascii")
    return encoded.rstrip("=")


def decode_return_route(token: str) -> dict[str, str]:
    token = clean(token)
    if not token:
        return {}
    padded = token + ("=" * (-len(token) % 4))
    try:
        decoded = base64.urlsafe_b64decode(padded.encode("ascii")).decode("utf-8")
        payload = json.loads(decoded)
    except Exception:
        return {}
    if not isinstance(payload, dict):
        return {}
    return {
        clean(key): clean(value)
        for key, value in payload.items()
        if clean(key) in {"source", "page", "view", "id", "return_to"} and clean(value)
    }


def capture_current_route_params() -> dict[str, str]:
    params: dict[str, str] = {}
    current_source = get_query_param("source")
    current_page = normalize_route_page_key(get_query_param("page"))
    current_view = get_query_param("view") or "table"
    current_id = get_query_param("id")
    current_return_to = get_query_param("return_to")
    if current_source:
        params["source"] = current_source
    if current_page:
        params["page"] = current_page
    if current_view:
        params["view"] = current_view
    if current_id:
        params["id"] = current_id
    if current_return_to:
        params["return_to"] = current_return_to
    return params


def current_return_route_token() -> str:
    return encode_return_route(capture_current_route_params())


def apply_return_route(params: dict[str, str]) -> dict[str, str]:
    route_token = current_return_route_token()
    merged = {clean(key): clean(value) for key, value in params.items() if clean(key)}
    if route_token:
        merged["return_to"] = route_token
    return merged


def restore_auth_from_query(mode_config: AppModeConfig) -> None:
    if get_current_user_id():
        return
    token = get_query_auth_token()
    user_id = decode_auth_token(token)
    if not user_id:
        return
    account = get_auth_account(user_id)
    if not account or clean(account.get("status")).lower() != "approved":
        return
    st.session_state["auth_user"] = {
        "user_id": clean(account.get("user_id")),
        "display_name": clean(account.get("display_name")),
        "email": clean(account.get("email")),
        "role": clean(account.get("role")) or "viewer",
    }
    st.session_state["auth_token"] = token


def get_service_account_info() -> dict | None:
    try:
        if "gcp_service_account" in st.secrets:
            info = dict(st.secrets["gcp_service_account"])
            if info:
                return info
    except Exception:
        pass

    raw_json = get_env("GOOGLE_CREDENTIALS_JSON_CONTENT")
    if raw_json:
        try:
            return json.loads(raw_json)
        except Exception as exc:
            raise RuntimeError("GOOGLE_CREDENTIALS_JSON_CONTENT is not valid JSON.") from exc

    return None


@st.cache_resource(show_spinner=False)
def get_gspread_client():
    service_account_info = get_service_account_info()
    if service_account_info:
        creds = Credentials.from_service_account_info(service_account_info, scopes=SCOPE)
        return gspread.authorize(creds)

    credentials_path = get_env("GOOGLE_CREDENTIALS_JSON")
    if not credentials_path:
        raise RuntimeError(
            "Google credentials are not set. "
            "Provide gcp_service_account in st.secrets or GOOGLE_CREDENTIALS_JSON."
        )
    if not Path(credentials_path).exists():
        raise RuntimeError(f"credentials file not found: {credentials_path}")

    creds = Credentials.from_service_account_file(credentials_path, scopes=SCOPE)
    return gspread.authorize(creds)


def get_worksheet(sheet_name: str):
    gc = get_gspread_client()
    sheet_id = get_env("GOOGLE_SHEET_ID")
    if not sheet_id:
        raise RuntimeError("GOOGLE_SHEET_ID is not set.")
    sh = run_gspread_call(gc.open_by_key, sheet_id)
    return run_gspread_call(sh.worksheet, sheet_name)


@st.cache_resource(show_spinner=False)
def get_spreadsheet():
    gc = get_gspread_client()
    sheet_id = get_env("GOOGLE_SHEET_ID")
    if not sheet_id:
        raise RuntimeError("GOOGLE_SHEET_ID is not set.")
    return run_gspread_call(gc.open_by_key, sheet_id)


def get_or_create_worksheet(sheet_name: str, headers: list[str], rows: int = 1000, cols: int | None = None):
    sh = get_spreadsheet()
    try:
        ws = run_gspread_call(sh.worksheet, sheet_name)
    except gspread.WorksheetNotFound:
        ws = run_gspread_call(sh.add_worksheet, title=sheet_name, rows=rows, cols=cols or len(headers))
        run_gspread_call(ws.update, [headers])
        return ws

    values = run_gspread_call(ws.get_all_values)
    if not values:
        run_gspread_call(ws.update, [headers])
        return ws

    header = [clean(x) for x in values[0]]
    missing_headers = [column for column in headers if column not in header]
    if missing_headers:
        run_gspread_call(
            ws.update,
            range_name=f"A1:{chr(64 + len(header) + len(missing_headers))}1",
            values=[header + missing_headers],
        )
    return ws


def get_worksheet_header(ws) -> list[str]:
    values = run_gspread_call(ws.get_all_values)
    return [clean(x) for x in values[0]] if values else []


def append_dict_row(ws, row: dict[str, object], fallback_headers: list[str]) -> None:
    header = get_worksheet_header(ws) or fallback_headers
    run_gspread_call(
        ws.append_row,
        [clean(row.get(column)) for column in header],
        value_input_option="USER_ENTERED",
    )


@st.cache_data(ttl=300, show_spinner=False)
def load_sheet_as_dataframe(sheet_name: str) -> pd.DataFrame:
    return load_sheet_as_dataframe_uncached(sheet_name)


def load_sheet_as_dataframe_uncached(sheet_name: str) -> pd.DataFrame:
    ws = get_worksheet(sheet_name)
    values = run_gspread_call(ws.get_all_values)

    if not values:
        return pd.DataFrame()

    header = [clean(x) for x in values[0]]
    rows = []
    for row in values[1:]:
        item = {}
        for index, column in enumerate(header):
            item[column] = clean(row[index] if index < len(row) else "")
        rows.append(item)

    df = pd.DataFrame(rows)
    if df.empty:
        return df

    for col in df.columns:
        df[col] = df[col].fillna("").astype(str).str.strip()

    return df


def load_optional_sheet_as_dataframe(sheet_name: str) -> pd.DataFrame:
    try:
        return load_sheet_as_dataframe(sheet_name)
    except Exception as exc:
        if isinstance(exc, gspread.WorksheetNotFound) or "WorksheetNotFound" in str(exc) or "not found" in str(exc).lower():
            return pd.DataFrame()
        raise


def load_optional_sheet_as_dataframe_uncached(sheet_name: str) -> pd.DataFrame:
    try:
        return load_sheet_as_dataframe_uncached(sheet_name)
    except Exception as exc:
        if isinstance(exc, gspread.WorksheetNotFound) or "WorksheetNotFound" in str(exc) or "not found" in str(exc).lower():
            return pd.DataFrame()
        raise


def get_comment_sheet_name() -> str:
    return get_env("NOTICE_COMMENT_SHEET", "NOTICE_COMMENTS")


def get_user_review_sheet_name() -> str:
    return get_env("NOTICE_USER_REVIEW_SHEET", "NOTICE_USER_REVIEWS")


def get_auth_user_sheet_name() -> str:
    return get_env("APP_USER_ACCOUNT_SHEET", "APP_USER_ACCOUNTS")


def get_signup_request_sheet_name() -> str:
    return get_env("SIGNUP_REQUEST_SHEET", "SIGNUP_REQUESTS")


def get_current_app_user_key() -> str:
    explicit_user_key = (
        get_env("APP_USER_KEY")
        or get_env("APP_USER_EMAIL")
        or get_env("DEFAULT_COMMENT_AUTHOR")
    )
    if explicit_user_key:
        return explicit_user_key

    username = get_env("USERNAME") or get_env("USER")
    hostname = get_env("COMPUTERNAME") or get_env("HOSTNAME")
    if username and hostname:
        return f"{username}@{hostname}".lower()
    if username:
        return username.lower()
    return "local-user"


def column_number_to_name(column_number: int) -> str:
    result = ""
    current = max(int(column_number), 1)
    while current:
        current, remainder = divmod(current - 1, 26)
        result = chr(65 + remainder) + result
    return result


def update_worksheet_row(ws, row_number: int, headers: list[str], row: dict[str, str]) -> None:
    end_column = column_number_to_name(len(headers))
    run_gspread_call(
        ws.update,
        range_name=f"A{row_number}:{end_column}{row_number}",
        values=[[row.get(column, "") for column in headers]],
        value_input_option="USER_ENTERED",
    )


def _empty_comment_dataframe() -> pd.DataFrame:
    working = pd.DataFrame(columns=COMMENT_COLUMNS)
    for legacy_column in ("author", "comment"):
        working[legacy_column] = pd.Series(dtype="object")
    working["created_at_sort"] = pd.Series(dtype="datetime64[ns]")
    return working


def _comment_text_series(df: pd.DataFrame, column: str) -> pd.Series:
    if column not in df.columns:
        return pd.Series("", index=df.index, dtype="object")
    return df[column].fillna("").astype(str).str.strip()


def build_comment_post_id(source_key: str, notice_id: str) -> str:
    normalized_source = normalize_opportunity_source_key(source_key) or clean(source_key).lower()
    normalized_notice_id = normalize_notice_id_for_match(notice_id)
    if not normalized_source or not normalized_notice_id:
        return ""
    return f"{normalized_source}:{normalized_notice_id}"


def _normalize_comment_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    if df is None:
        return _empty_comment_dataframe()

    working = df.copy()
    if working.empty and not list(working.columns):
        working = pd.DataFrame(columns=COMMENT_COLUMNS)

    for column in COMMENT_COLUMNS:
        if column not in working.columns:
            working[column] = ""

    legacy_author = _comment_text_series(working, "author")
    legacy_comment = _comment_text_series(working, "comment")
    nickname_series = _comment_text_series(working, "nickname")
    user_id_series = _comment_text_series(working, "user_id")
    content_series = _comment_text_series(working, "content")

    working["nickname"] = nickname_series.where(nickname_series.ne(""), legacy_author)
    working["user_id"] = user_id_series.where(
        user_id_series.ne(""),
        working["nickname"].where(_comment_text_series(working, "nickname").ne(""), legacy_author),
    )
    working["content"] = content_series.where(content_series.ne(""), legacy_comment)
    working["updated_at"] = _comment_text_series(working, "updated_at").where(
        _comment_text_series(working, "updated_at").ne(""),
        _comment_text_series(working, "created_at"),
    )
    working["post_id"] = _comment_text_series(working, "post_id").where(
        _comment_text_series(working, "post_id").ne(""),
        [
            build_comment_post_id(source, notice_id)
            for source, notice_id in zip(_comment_text_series(working, "source"), _comment_text_series(working, "notice_id"))
        ],
    )

    for column in COMMENT_COLUMNS:
        working[column] = working[column].fillna("").astype(str).str.strip()

    working["author"] = _comment_text_series(working, "nickname").where(
        _comment_text_series(working, "nickname").ne(""),
        _comment_text_series(working, "user_id"),
    )
    working["comment"] = _comment_text_series(working, "content")
    working["created_at_sort"] = pd.to_datetime(working["created_at"], errors="coerce")
    return working


@st.cache_data(ttl=300, show_spinner=False)
def load_notice_comments(include_deleted: bool = False) -> pd.DataFrame:
    sheet_name = get_comment_sheet_name()
    df = load_optional_sheet_as_dataframe(sheet_name)
    if df.empty:
        ws = get_or_create_worksheet(sheet_name, COMMENT_COLUMNS, rows=1000, cols=len(COMMENT_COLUMNS))
        header = get_worksheet_header(ws)
        missing_headers = [column for column in COMMENT_COLUMNS if column not in header]
        if missing_headers:
            run_gspread_call(ws.update, range_name="A1", values=[header + missing_headers])
        return _empty_comment_dataframe()

    ws = get_or_create_worksheet(sheet_name, COMMENT_COLUMNS, rows=1000, cols=len(COMMENT_COLUMNS))
    header = get_worksheet_header(ws)
    missing_headers = [column for column in COMMENT_COLUMNS if column not in header]
    if missing_headers:
        run_gspread_call(ws.update, range_name="A1", values=[header + missing_headers])
        df = load_sheet_as_dataframe_uncached(sheet_name)

    working = _normalize_comment_dataframe(df)
    if not include_deleted:
        working = working[_comment_text_series(working, "deleted_at").eq("")].copy()
    if working.empty:
        return _empty_comment_dataframe()
    return working.sort_values(by=["created_at_sort"], ascending=False, na_position="last")


@st.cache_data(ttl=30, show_spinner=False)
def load_auth_user_accounts() -> pd.DataFrame:
    try:
        df = load_optional_sheet_as_dataframe_uncached(get_auth_user_sheet_name())
    except Exception:
        return pd.DataFrame(columns=AUTH_USER_COLUMNS)
    if df.empty:
        return pd.DataFrame(columns=AUTH_USER_COLUMNS)
    working = df.copy()
    for column in AUTH_USER_COLUMNS:
        if column not in working.columns:
            working[column] = ""
    return working


def load_auth_accounts() -> dict[str, dict[str, str]]:
    accounts: dict[str, dict[str, str]] = {}
    static_users = load_static_auth_users()
    sheet_accounts = load_auth_user_accounts()
    for _, row in sheet_accounts.iterrows():
        user_id = clean(row.get("user_id"))
        if not user_id:
            continue
        accounts[user_id] = {
            "user_id": user_id,
            "password_hash": clean(row.get("password_hash")),
            "display_name": clean(row.get("display_name")) or user_id,
            "email": clean(row.get("email")),
            "role": clean(row.get("role")) or "viewer",
            "status": clean(row.get("status")) or "pending",
        }
    for user_id, password in static_users.items():
        accounts[user_id] = {
            "user_id": user_id,
            "password_hash": password,
            "display_name": user_id,
            "email": clean(accounts.get(user_id, {}).get("email")),
            "role": "viewer",
            "status": "approved",
        }
    return accounts


def get_auth_account(user_id: str) -> dict[str, str] | None:
    return load_auth_accounts().get(clean(user_id))


def find_auth_account_row(*, user_id: str = "", email: str = "") -> tuple[int, list[str], dict[str, str]] | tuple[int, None, None]:
    ws = get_or_create_worksheet(get_auth_user_sheet_name(), AUTH_USER_COLUMNS, rows=1000, cols=len(AUTH_USER_COLUMNS))
    values = run_gspread_call(ws.get_all_values)
    headers = [clean(value) for value in values[0]] if values else AUTH_USER_COLUMNS
    normalized_user_id = clean(user_id)
    normalized_email = clean(email).lower()

    for row_index, existing_values in enumerate(values[1:], start=2):
        existing = {
            headers[column_index]: clean(existing_values[column_index] if column_index < len(existing_values) else "")
            for column_index in range(len(headers))
        }
        existing_user_id = clean(existing.get("user_id"))
        existing_email = clean(existing.get("email")).lower()
        if normalized_user_id and existing_user_id == normalized_user_id:
            return row_index, headers, existing
        if normalized_email and existing_email == normalized_email:
            return row_index, headers, existing

    return 0, None, None


def sync_auth_account_status(
    *,
    user_id: str = "",
    email: str = "",
    status: str,
    actor: str,
    display_name: str = "",
) -> None:
    row_index, headers, existing = find_auth_account_row(user_id=user_id, email=email)
    if not row_index or not headers or existing is None:
        return

    now = pd.Timestamp.now(tz="Asia/Seoul").strftime("%Y-%m-%d %H:%M:%S")
    updated = dict(existing)
    normalized_status = clean(status).lower()
    updated["status"] = normalized_status or clean(existing.get("status")) or "pending"
    if clean(display_name):
        updated["display_name"] = clean(display_name)
    if clean(email):
        updated["email"] = clean(email).lower()

    if normalized_status == "approved":
        updated["approved_at"] = now
        updated["approved_by"] = clean(actor)
        updated["rejected_at"] = ""
        updated["rejected_by"] = ""
    elif normalized_status == "rejected":
        updated["rejected_at"] = now
        updated["rejected_by"] = clean(actor)
    else:
        updated["status"] = "pending"

    ws = get_or_create_worksheet(get_auth_user_sheet_name(), AUTH_USER_COLUMNS, rows=1000, cols=len(AUTH_USER_COLUMNS))
    update_worksheet_row(ws, row_index, headers, updated)
    load_sheet_as_dataframe.clear()
    load_auth_user_accounts.clear()


def get_current_user_id() -> str:
    user = st.session_state.get("auth_user") or {}
    return clean(user.get("user_id")) if isinstance(user, dict) else ""


def get_current_user_label() -> str:
    user = st.session_state.get("auth_user") or {}
    display_name = clean(user.get("display_name")) if isinstance(user, dict) else ""
    user_id = get_current_user_id()
    return display_name or user_id or get_env("DEFAULT_COMMENT_AUTHOR") or get_env("USER") or "app"


def get_current_user_email() -> str:
    user = st.session_state.get("auth_user") or {}
    return clean(user.get("email")) if isinstance(user, dict) else ""


def get_current_user_domain() -> str:
    return normalize_email_domain(get_current_user_email())


def get_admin_user_ids() -> set[str]:
    raw_value = get_env("ADMIN_USER_IDS", "admin")
    return {clean(item).lower() for item in clean(raw_value).split(",") if clean(item)}


def is_admin_user(user_id: str) -> bool:
    return clean(user_id).lower() in get_admin_user_ids()


def get_comment_owner_id(comment_row) -> str:
    if isinstance(comment_row, pd.Series):
        comment_row = comment_row.to_dict()
    if not isinstance(comment_row, dict):
        return ""
    return (
        clean(comment_row.get("user_id"))
        or clean(comment_row.get("nickname"))
        or clean(comment_row.get("author"))
    )


def can_delete_comment(comment_row, current_user_id: str) -> bool:
    normalized_user_id = clean(current_user_id).lower()
    if not normalized_user_id:
        return False
    if is_admin_user(normalized_user_id):
        return True
    owner_id = clean(get_comment_owner_id(comment_row)).lower()
    return bool(owner_id and owner_id == normalized_user_id)


def build_operation_scope_key(account: dict[str, str] | None) -> str:
    account = account or {}
    domain = normalize_email_domain(account.get("email", ""))
    if domain:
        return f"domain:{domain}"
    user_id = clean(account.get("user_id"))
    return f"user:{user_id}" if user_id else ""


def get_current_operation_scope_key() -> str:
    user_id = get_current_user_id()
    account = get_auth_account(user_id) if user_id else None
    if account:
        return build_operation_scope_key(account)
    if user_id:
        return f"user:{user_id}"
    return ""


def get_current_operation_scope_label() -> str:
    scope_key = get_current_operation_scope_key()
    if scope_key.startswith("domain:"):
        return scope_key.removeprefix("domain:")
    return get_current_user_id()


def is_user_scoped_operations_enabled() -> bool:
    return bool(get_current_operation_scope_key()) and get_bool_env("USER_SCOPED_OPERATIONS", default=True)


def logout_current_user() -> None:
    st.session_state.pop("auth_user", None)
    st.session_state.pop("auth_token", None)
    params = get_query_params_dict()
    params.pop("auth", None)
    replace_query_params(params)
    st.rerun()


@st.cache_data(ttl=300, show_spinner=False)
def load_user_review_statuses(user_id: str) -> pd.DataFrame:
    user_id = clean(user_id)
    if not user_id:
        return pd.DataFrame(columns=USER_REVIEW_COLUMNS)

    try:
        df = load_optional_sheet_as_dataframe(get_user_review_sheet_name())
    except Exception:
        return pd.DataFrame(columns=USER_REVIEW_COLUMNS)
    if df.empty:
        return pd.DataFrame(columns=USER_REVIEW_COLUMNS)

    working = df.copy()
    for column in USER_REVIEW_COLUMNS:
        if column not in working.columns:
            working[column] = ""
    return working[working["user_id"].fillna("").astype(str).str.strip().eq(user_id)].copy()


def build_user_review_lookup(user_reviews_df: pd.DataFrame) -> dict[tuple[str, str], str]:
    if user_reviews_df.empty:
        return {}
    lookup: dict[tuple[str, str], str] = {}
    for _, row in user_reviews_df.iterrows():
        source_key = clean(row.get("source")) or "iris"
        notice_key = normalize_notice_id_for_match(row.get("notice_id"))
        if not notice_key:
            continue
        lookup[(source_key, notice_key)] = clean(row.get("review_status"))
    return lookup


def apply_user_review_statuses_to_df(df: pd.DataFrame, source_key: str, user_reviews_df: pd.DataFrame) -> pd.DataFrame:
    if df.empty or user_reviews_df.empty:
        return df

    working = df.copy()
    lookup = build_user_review_lookup(user_reviews_df)
    if not lookup:
        return working

    notice_ids = series_from_candidates(working, ["怨듦퀬ID", "notice_id"])
    override_values = [
        lookup.get((source_key, normalize_notice_id_for_match(notice_id)), None)
        for notice_id in notice_ids
    ]
    override_series = pd.Series(override_values, index=working.index, dtype=object)
    override_mask = override_series.notna()
    if not override_mask.any():
        return working

    for column in ["寃??щ?", "寃?좎뿬遺", "review_status"]:
        if column in working.columns:
            working.loc[override_mask, column] = override_series[override_mask].fillna("")
    return working


def apply_user_review_statuses(
    datasets: dict[str, pd.DataFrame],
    source_datasets: dict[str, object] | None,
    user_id: str,
) -> tuple[dict[str, pd.DataFrame], dict[str, object] | None]:
    if not user_id:
        return datasets, source_datasets

    try:
        user_reviews_df = load_user_review_statuses(user_id)
    except Exception:
        return datasets, source_datasets
    if user_reviews_df.empty:
        return datasets, source_datasets

    scoped_datasets = dict(datasets)
    for key in [
        "notice",
        "notice_master",
        "notice_current",
        "notice_view",
        "pending",
        "notice_archive",
        "opportunity",
        "opportunity_all",
        "opportunity_archive",
        "summary",
    ]:
        if key in scoped_datasets:
            scoped_datasets[key] = apply_user_review_statuses_to_df(scoped_datasets[key], "iris", user_reviews_df)

    scoped_source_datasets = dict(source_datasets) if source_datasets else source_datasets
    if scoped_source_datasets:
        source_key_map = {
            "mss_current": "tipa",
            "mss_past": "tipa",
            "mss_opportunity": "tipa",
            "mss_opportunity_archive": "tipa",
            "nipa_current": "nipa",
            "nipa_past": "nipa",
            "nipa_opportunity": "nipa",
            "nipa_opportunity_archive": "nipa",
        }
        for dataset_key, source_key in source_key_map.items():
            value = scoped_source_datasets.get(dataset_key)
            if isinstance(value, pd.DataFrame):
                scoped_source_datasets[dataset_key] = apply_user_review_statuses_to_df(value, source_key, user_reviews_df)

    return scoped_datasets, scoped_source_datasets


def filter_notice_comments(comments_df: pd.DataFrame, *, source_key: str, notice_id: str) -> pd.DataFrame:
    if comments_df.empty:
        return _empty_comment_dataframe()

    working = _normalize_comment_dataframe(comments_df)
    comment_notice_keys = _comment_text_series(working, "notice_id").apply(normalize_notice_id_for_match)
    current_notice_key = normalize_notice_id_for_match(notice_id)
    current_source_key = normalize_opportunity_source_key(source_key) or clean(source_key).lower()
    current_post_id = build_comment_post_id(current_source_key, current_notice_key)
    filtered = working[
        _comment_text_series(working, "source").str.lower().eq(current_source_key)
        & (
            comment_notice_keys.eq(current_notice_key)
            | _comment_text_series(working, "post_id").eq(current_post_id)
        )
    ].copy()
    filtered = filtered[_comment_text_series(filtered, "deleted_at").eq("")].copy()
    return filtered


def append_notice_comment(
    *,
    source_key: str,
    notice_id: str,
    notice_title: str,
    author: str,
    comment: str,
) -> None:
    notice_id = clean(notice_id)
    content = clean(comment)
    if not notice_id:
        raise RuntimeError("공고ID가 없어 댓글을 저장할 수 없습니다.")
    if not content:
        raise RuntimeError("댓글 내용을 입력해 주세요.")

    ws = get_or_create_worksheet(get_comment_sheet_name(), COMMENT_COLUMNS, rows=1000, cols=len(COMMENT_COLUMNS))
    current_user_id = clean(get_current_user_id()) or clean(author) or clean(get_env("DEFAULT_COMMENT_AUTHOR"))
    nickname = clean(author) or clean(get_current_user_label()) or current_user_id or "익명"
    timestamp = pd.Timestamp.now(tz="Asia/Seoul").strftime("%Y-%m-%d %H:%M:%S")
    row = {
        "comment_id": str(uuid.uuid4()),
        "post_id": build_comment_post_id(source_key, notice_id),
        "user_id": current_user_id or nickname,
        "source": normalize_opportunity_source_key(source_key) or clean(source_key).lower() or "iris",
        "notice_id": notice_id,
        "notice_title": clean(notice_title),
        "parent_id": "",
        "nickname": nickname,
        "content": content[:5000],
        "mention": "",
        "ip_address": "",
        "ip_based_uid": "",
        "created_at": timestamp,
        "updated_at": timestamp,
        "deleted_at": "",
    }
    append_dict_row(ws, row, COMMENT_COLUMNS)
    load_sheet_as_dataframe.clear()
    load_notice_comments.clear()
    load_app_datasets.clear()


def delete_notice_comment(comment_id: str, current_user_id: str) -> None:
    comment_id = clean(comment_id)
    if not comment_id:
        raise RuntimeError("삭제할 댓글 ID가 없습니다.")

    ws = get_worksheet(get_comment_sheet_name())
    values = run_gspread_call(ws.get_all_values)
    if not values:
        raise RuntimeError("댓글 이력 시트가 비어 있습니다.")

    header = [clean(x) for x in values[0]]
    if "comment_id" not in header:
        raise RuntimeError("댓글 이력 시트에 comment_id 컬럼이 없습니다.")

    comment_id_col = header.index("comment_id")
    for row_index, sheet_row in enumerate(values[1:], start=2):
        current_comment_id = clean(sheet_row[comment_id_col] if comment_id_col < len(sheet_row) else "")
        if current_comment_id == comment_id:
            existing = {
                header[column_index]: clean(sheet_row[column_index] if column_index < len(sheet_row) else "")
                for column_index in range(len(header))
            }
            comment_row = _normalize_comment_dataframe(pd.DataFrame([existing])).iloc[0].to_dict()
            if not can_delete_comment(comment_row, current_user_id):
                raise RuntimeError("본인이 작성한 댓글만 삭제할 수 있습니다.")

            timestamp = pd.Timestamp.now(tz="Asia/Seoul").strftime("%Y-%m-%d %H:%M:%S")
            updated = dict(existing)
            updated["deleted_at"] = timestamp
            updated["updated_at"] = timestamp
            update_worksheet_row(ws, row_index, header, updated)
            load_sheet_as_dataframe.clear()
            load_notice_comments.clear()
            load_app_datasets.clear()
            return

    raise RuntimeError("삭제할 댓글을 찾지 못했습니다.")


def upsert_user_review_status(
    *,
    user_id: str,
    source_key: str,
    notice_id: str,
    notice_title: str,
    review_status: str,
) -> None:
    user_id = clean(user_id)
    source_key = clean(source_key) or "iris"
    notice_id = clean(notice_id)
    if not user_id:
        raise RuntimeError("濡쒓렇??ъ슜??뺣낫媛 ?놁뼱 寃??щ?瑜???ν븷 ??놁뒿?덈떎.")
    if not notice_id:
        raise RuntimeError("怨듦퀬ID媛 ?놁뼱 寃??щ?瑜???ν븷 ??놁뒿?덈떎.")

    ws = get_or_create_worksheet(get_user_review_sheet_name(), USER_REVIEW_COLUMNS, rows=1000, cols=len(USER_REVIEW_COLUMNS))
    values = run_gspread_call(ws.get_all_values)
    header = [clean(x) for x in values[0]] if values else USER_REVIEW_COLUMNS.copy()

    def col_index(column: str) -> int | None:
        return header.index(column) if column in header else None

    user_col = col_index("user_id")
    source_col = col_index("source")
    notice_col = col_index("notice_id")
    review_col = col_index("review_status")
    title_col = col_index("notice_title")
    updated_col = col_index("updated_at")
    if user_col is None or source_col is None or notice_col is None or review_col is None:
        raise RuntimeError("?ъ슜??寃??쒗듃??꾩닔 而щ읆??놁뒿?덈떎.")

    timestamp = pd.Timestamp.now(tz="Asia/Seoul").strftime("%Y-%m-%d %H:%M:%S")
    notice_key = normalize_notice_id_for_match(notice_id)
    target_row_index = None
    for row_index, row in enumerate(values[1:], start=2):
        current_user = clean(row[user_col] if user_col < len(row) else "")
        current_source = clean(row[source_col] if source_col < len(row) else "")
        current_notice_key = normalize_notice_id_for_match(row[notice_col] if notice_col < len(row) else "")
        if current_user == user_id and current_source == source_key and current_notice_key == notice_key:
            target_row_index = row_index
            break

    if target_row_index:
        ws.update_cell(target_row_index, review_col + 1, clean(review_status))
        if title_col is not None:
            ws.update_cell(target_row_index, title_col + 1, clean(notice_title))
        if updated_col is not None:
            ws.update_cell(target_row_index, updated_col + 1, timestamp)
    else:
        row = {
            "user_id": user_id,
            "source": source_key,
            "notice_id": notice_id,
            "notice_title": clean(notice_title),
            "review_status": clean(review_status),
            "updated_at": timestamp,
        }
        append_dict_row(ws, row, USER_REVIEW_COLUMNS)

    load_sheet_as_dataframe.clear()
    load_user_review_statuses.clear()
    build_source_datasets.clear()
    load_app_datasets.clear()


def submit_signup_request(*, user_id: str, password: str, display_name: str, email: str) -> None:
    user_id = clean(user_id)
    password = clean(password)
    email = clean(email).lower()
    if not user_id:
        raise RuntimeError("?꾩씠?붾? ?낅젰??二쇱꽭??")
    if len(user_id) < 3:
        raise RuntimeError("?꾩씠?붾뒗 3??댁긽?댁뼱??⑸땲??")
    if not re.match(r"^[A-Za-z0-9_.-]+$", user_id):
        raise RuntimeError("?꾩씠?붾뒗 ?곷Ц, ?レ옄, ?? 諛묒쨪, ?섏씠?덈쭔 ?ъ슜??덉뒿?덈떎.")
    if len(password) < 6:
        raise RuntimeError("鍮꾨?踰덊샇??6??댁긽?댁뼱??⑸땲??")
    allowed_domains = load_allowed_email_domains()
    email_domain = normalize_email_domain(email)
    if allowed_domains and email_domain not in allowed_domains:
        raise RuntimeError("?덉슜??뚯궗 ?대찓??꾨찓?몃쭔 媛??붿껌??덉뒿?덈떎.")
    if get_auth_account(user_id):
        raise RuntimeError("?대? ?깅줉?섏뿀嫄곕굹 ?뱀씤 ?湲?以묒씤 ?꾩씠?붿엯?덈떎.")

    ws = get_or_create_worksheet(get_auth_user_sheet_name(), AUTH_USER_COLUMNS, rows=1000, cols=len(AUTH_USER_COLUMNS))
    timestamp = pd.Timestamp.now(tz="Asia/Seoul").strftime("%Y-%m-%d %H:%M:%S")
    append_dict_row(
        ws,
        {
            "user_id": user_id,
            "password_hash": hash_password(password),
            "display_name": clean(display_name) or user_id,
            "email": email,
            "role": "viewer",
            "status": "pending",
            "requested_at": timestamp,
        },
        AUTH_USER_COLUMNS,
    )
    load_sheet_as_dataframe.clear()


def render_signup_form() -> None:
    st.markdown("#### 媛??붿껌")
    allowed_domains = sorted(load_allowed_email_domains())
    if allowed_domains:
        st.caption("媛??媛??꾨찓?? " + ", ".join(allowed_domains))
    with st.form("signup_form"):
        user_id = st.text_input("?꾩씠??, key="signup_user_id")
        display_name = st.text_input("?대쫫", key="signup_display_name")
        email = st.text_input("?대찓??, key="signup_email")
        password = st.text_input("鍮꾨?踰덊샇", type="password", key="signup_password")
        password_confirm = st.text_input("鍮꾨?踰덊샇 ?뺤씤", type="password", key="signup_password_confirm")
        submitted = st.form_submit_button("媛??붿껌", use_container_width=True)
    if submitted:
        if clean(password) != clean(password_confirm):
            st.error("鍮꾨?踰덊샇 ?뺤씤??쇱튂?섏? ?딆뒿?덈떎.")
            return
        try:
            submit_signup_request(
                user_id=user_id,
                password=password,
                display_name=display_name,
                email=email,
            )
            st.success("媛??붿껌??蹂대깉?듬땲?? 愿由ъ옄媛 ?뱀씤?섎㈃ 濡쒓렇?명븷 ??덉뒿?덈떎.")
        except Exception as exc:
            st.error(f"媛??붿껌 ?ㅽ뙣: {exc}")


def render_login_page(mode_config: AppModeConfig, accounts: dict[str, dict[str, str]]) -> None:
    st.markdown("<div style='height: 12vh;'></div>", unsafe_allow_html=True)
    _, center_col, _ = st.columns([1.2, 1, 1.2])
    with center_col:
        st.title(mode_config.header_title)
        st.caption("媛숈? ?대찓??꾨찓?몄쓣 媛吏??ъ슜?먮겮由щ뒗 ?볤?, 愿?ш났怨? 寃??곹깭瑜??④퍡 怨듭쑀?⑸땲??")
        if mode_config.mode == "viewer":
            login_tab, signup_tab = st.tabs(["濡쒓렇??, "媛??붿껌"])
            with login_tab:
                with st.form("login_form"):
                    user_id = st.text_input("?꾩씠??)
                    password = st.text_input("鍮꾨?踰덊샇", type="password")
                    submitted = st.form_submit_button("濡쒓렇??, use_container_width=True)
                if submitted:
                    account = accounts.get(clean(user_id))
                    if account and clean(account.get("status")).lower() == "approved" and verify_password(password, account.get("password_hash", "")):
                        st.session_state["auth_user"] = {
                            "user_id": clean(account.get("user_id")),
                            "display_name": clean(account.get("display_name")),
                            "email": clean(account.get("email")),
                            "role": clean(account.get("role")) or "viewer",
                        }
                        token = encode_auth_token(account.get("user_id", ""))
                        st.session_state["auth_token"] = token
                        replace_query_params(with_auth_params(get_query_params_dict()))
                        st.rerun()
                    elif account and clean(account.get("status")).lower() == "pending":
                        st.warning("?꾩쭅 ?쒖꽦?붾릺吏 ?딆? 怨꾩젙?낅땲?? 愿由ъ옄?먭쾶 ?쒖꽦??곹깭瑜??뺤씤??二쇱꽭??")
                    elif account and clean(account.get("status")).lower() == "rejected":
                        st.error("?ъ슜??以묒??怨꾩젙?낅땲?? 愿由ъ옄?먭쾶 臾몄쓽??二쇱꽭??")
                    else:
                        st.error("?꾩씠??먮뒗 鍮꾨?踰덊샇瑜??뺤씤??二쇱꽭??")
            with signup_tab:
                render_signup_form()
        else:
            with st.form("login_form"):
                user_id = st.text_input("?꾩씠??)
                password = st.text_input("鍮꾨?踰덊샇", type="password")
                submitted = st.form_submit_button("濡쒓렇??, use_container_width=True)
            if submitted:
                account = accounts.get(clean(user_id))
                if account and clean(account.get("status")).lower() == "approved" and verify_password(password, account.get("password_hash", "")):
                    st.session_state["auth_user"] = {
                        "user_id": clean(account.get("user_id")),
                        "display_name": clean(account.get("display_name")),
                        "email": clean(account.get("email")),
                        "role": clean(account.get("role")) or "viewer",
                    }
                    token = encode_auth_token(account.get("user_id", ""))
                    st.session_state["auth_token"] = token
                    replace_query_params(with_auth_params(get_query_params_dict()))
                    st.rerun()
                elif account and clean(account.get("status")).lower() == "pending":
                    st.warning("?꾩쭅 ?쒖꽦?붾릺吏 ?딆? 怨꾩젙?낅땲?? 愿由ъ옄?먭쾶 ?쒖꽦??곹깭瑜??뺤씤??二쇱꽭??")
                elif account and clean(account.get("status")).lower() == "rejected":
                    st.error("?ъ슜??以묒??怨꾩젙?낅땲?? 愿由ъ옄?먭쾶 臾몄쓽??二쇱꽭??")
                else:
                    st.error("?꾩씠??먮뒗 鍮꾨?踰덊샇瑜??뺤씤??二쇱꽭??")


def require_login(mode_config: AppModeConfig) -> None:
    auth_required = get_bool_env("APP_AUTH_REQUIRED", default=True)
    if not auth_required:
        return
    restore_auth_from_query(mode_config)
    if get_current_user_id():
        return

    accounts = load_auth_accounts()
    render_login_page(mode_config, accounts)
    st.stop()

def normalize_signup_request_row(row: dict[str, object]) -> dict[str, str]:
    now = pd.Timestamp.now(tz="Asia/Seoul").strftime("%Y-%m-%d %H:%M:%S")
    normalized = {column: "" for column in SIGNUP_REQUEST_COLUMNS}
    normalized["request_id"] = clean(row.get("request_id")) or str(uuid.uuid4())
    normalized["requested_at"] = clean(row.get("requested_at")) or now
    normalized["name"] = clean(row.get("name"))
    normalized["email"] = clean(row.get("email")).lower()
    normalized["organization"] = clean(row.get("organization") or row.get("company"))
    normalized["account_type"] = clean(row.get("account_type")) or "company"
    normalized["request_note"] = clean(row.get("request_note") or row.get("note"))[:5000]
    status = clean(row.get("status")).upper() or "PENDING"
    normalized["status"] = status if status in SIGNUP_STATUS_OPTIONS else "PENDING"
    normalized["admin_note"] = clean(row.get("admin_note"))[:5000]
    normalized["reviewed_at"] = clean(row.get("reviewed_at"))
    normalized["reviewed_by"] = clean(row.get("reviewed_by"))
    return normalized


@st.cache_data(ttl=30, show_spinner=False)
def load_signup_requests() -> pd.DataFrame:
    df = load_optional_sheet_as_dataframe_uncached(get_signup_request_sheet_name())
    if df.empty:
        return pd.DataFrame(columns=SIGNUP_REQUEST_COLUMNS)

    working = df.copy()
    for column in SIGNUP_REQUEST_COLUMNS:
        if column not in working.columns:
            working[column] = ""
    working["requested_at_sort"] = pd.to_datetime(working["requested_at"], errors="coerce")
    return working.sort_values(
        by=["requested_at_sort", "email", "name"],
        ascending=[False, True, True],
        na_position="last",
    )


def clear_signup_request_caches() -> None:
    load_sheet_as_dataframe.clear()
    load_signup_requests.clear()


def extract_requested_user_id(request_note: object) -> str:
    note_text = clean(request_note)
    if not note_text:
        return ""
    match = re.search(r"(?:^|\\s)requested_user_id=([A-Za-z0-9_.-]+)", note_text)
    return clean(match.group(1)) if match else ""


def load_signup_requests_live() -> pd.DataFrame:
    df = load_optional_sheet_as_dataframe_uncached(get_signup_request_sheet_name())
    if df.empty:
        return pd.DataFrame(columns=SIGNUP_REQUEST_COLUMNS)

    working = df.copy()
    for column in SIGNUP_REQUEST_COLUMNS:
        if column not in working.columns:
            working[column] = ""
    working["requested_at_sort"] = pd.to_datetime(working["requested_at"], errors="coerce")
    return working.sort_values(
        by=["requested_at_sort", "email", "name"],
        ascending=[False, True, True],
        na_position="last",
    )


def get_signup_requests_for_email(email: str) -> pd.DataFrame:
    normalized_email = clean(email).lower()
    if not normalized_email:
        return pd.DataFrame(columns=SIGNUP_REQUEST_COLUMNS)
    request_df = load_signup_requests()
    if request_df.empty:
        return pd.DataFrame(columns=SIGNUP_REQUEST_COLUMNS)
    return request_df[
        request_df["email"].fillna("").astype(str).str.strip().str.lower().eq(normalized_email)
    ].copy()


def get_latest_signup_request_for_account(account: dict[str, str] | None) -> dict[str, str]:
    account = account or {}
    email = clean(account.get("email")).lower()
    user_id = clean(account.get("user_id"))
    if not email and not user_id:
        return {}

    try:
        request_df = load_signup_requests_live()
    except Exception:
        request_df = load_signup_requests()
    if request_df.empty:
        return {}

    working = request_df.copy()
    email_mask = pd.Series(False, index=working.index)
    user_id_mask = pd.Series(False, index=working.index)
    if email:
        email_mask = working["email"].fillna("").astype(str).str.strip().str.lower().eq(email)
    if user_id:
        user_ids = working["request_note"].apply(extract_requested_user_id)
        user_id_mask = user_ids.eq(user_id)

    matched = working[email_mask | user_id_mask].copy()
    if matched.empty:
        return {}
    return matched.iloc[0].to_dict()


def refresh_account_status_from_signup_request(account: dict[str, str] | None) -> dict[str, str] | None:
    account = account or {}
    current_user_id = clean(account.get("user_id"))
    current_email = clean(account.get("email")).lower()
    if not current_user_id and not current_email:
        return account

    latest_request = get_latest_signup_request_for_account(account)
    request_status = clean(latest_request.get("status")).upper()
    status_map = {
        "APPROVED": "approved",
        "REJECTED": "rejected",
        "PENDING": "pending",
        "HOLD": "hold",
    }
    target_status = status_map.get(request_status, "")
    if not target_status:
        return account

    current_status = clean(account.get("status")).lower()
    if current_status == ("pending" if target_status == "hold" else target_status):
        return account

    sync_auth_account_status(
        user_id=current_user_id or extract_requested_user_id(latest_request.get("request_note")),
        email=current_email or clean(latest_request.get("email")).lower(),
        status=target_status,
        actor="signup-request-sync",
        display_name=clean(account.get("display_name")) or clean(latest_request.get("name")),
    )
    refreshed_account = get_auth_account(current_user_id) if current_user_id else None
    return refreshed_account or account


def save_signup_request(row: dict[str, object]) -> dict[str, str]:
    ws = get_or_create_worksheet(
        get_signup_request_sheet_name(),
        SIGNUP_REQUEST_COLUMNS,
        rows=1000,
        cols=len(SIGNUP_REQUEST_COLUMNS),
    )
    values = run_gspread_call(ws.get_all_values)
    headers = [clean(value) for value in values[0]] if values else SIGNUP_REQUEST_COLUMNS
    normalized = normalize_signup_request_row(row)

    target_row_number = 0
    for row_index, existing_values in enumerate(values[1:], start=2):
        existing_request_id = clean(
            existing_values[headers.index("request_id")]
            if "request_id" in headers and headers.index("request_id") < len(existing_values)
            else ""
        )
        if existing_request_id == normalized["request_id"]:
            target_row_number = row_index
            break

    if target_row_number:
        update_worksheet_row(ws, target_row_number, headers, normalized)
    else:
        run_gspread_call(ws.append_row, [normalized[column] for column in headers], value_input_option="USER_ENTERED")

    clear_signup_request_caches()
    return normalized


def find_auth_account_row(*, user_id: str = "", email: str = "") -> tuple[int, list[str], dict[str, str]] | tuple[int, None, None]:
    ws = get_or_create_worksheet(get_auth_user_sheet_name(), AUTH_USER_COLUMNS, rows=1000, cols=len(AUTH_USER_COLUMNS))
    values = run_gspread_call(ws.get_all_values)
    headers = [clean(value) for value in values[0]] if values else AUTH_USER_COLUMNS
    normalized_user_id = clean(user_id)
    normalized_email = clean(email).lower()

    for row_index, existing_values in enumerate(values[1:], start=2):
        existing = {
            headers[column_index]: clean(existing_values[column_index] if column_index < len(existing_values) else "")
            for column_index in range(len(headers))
        }
        if normalized_user_id and clean(existing.get("user_id")) == normalized_user_id:
            return row_index, headers, existing
        if normalized_email and clean(existing.get("email")).lower() == normalized_email:
            return row_index, headers, existing
    return 0, None, None


def sync_auth_account_status(
    *,
    user_id: str = "",
    email: str = "",
    status: str,
    actor: str,
    display_name: str = "",
) -> None:
    row_index, headers, existing = find_auth_account_row(user_id=user_id, email=email)
    if not row_index or not headers or existing is None:
        return

    ws = get_or_create_worksheet(get_auth_user_sheet_name(), AUTH_USER_COLUMNS, rows=1000, cols=len(AUTH_USER_COLUMNS))
    now = pd.Timestamp.now(tz="Asia/Seoul").strftime("%Y-%m-%d %H:%M:%S")
    updated = dict(existing)
    normalized_status = clean(status).lower() or "pending"
    updated["status"] = "pending" if normalized_status == "hold" else normalized_status
    if clean(display_name):
        updated["display_name"] = clean(display_name)
    if clean(email):
        updated["email"] = clean(email).lower()

    if normalized_status == "approved":
        updated["approved_at"] = now
        updated["approved_by"] = clean(actor)
        updated["rejected_at"] = ""
        updated["rejected_by"] = ""
    elif normalized_status == "rejected":
        updated["rejected_at"] = now
        updated["rejected_by"] = clean(actor)

    update_worksheet_row(ws, row_index, headers, updated)
    load_sheet_as_dataframe.clear()
    load_auth_user_accounts.clear()


def submit_signup_request(*, user_id: str, password: str, display_name: str, email: str) -> None:
    user_id = clean(user_id)
    password = clean(password)
    email = clean(email).lower()
    display_name = clean(display_name) or user_id
    if not user_id:
        raise RuntimeError("?꾩씠?붾? ?낅젰??二쇱꽭??")
    if len(user_id) < 3:
        raise RuntimeError("?꾩씠?붾뒗 3??댁긽?댁뼱??⑸땲??")
    if not re.match(r"^[A-Za-z0-9_.-]+$", user_id):
        raise RuntimeError("?꾩씠?붾뒗 ?곷Ц, ?レ옄, 諛묒쨪, ?? ?섏씠?덈쭔 ?ъ슜??덉뒿?덈떎.")
    if len(password) < 6:
        raise RuntimeError("鍮꾨?踰덊샇??6??댁긽?댁뼱??⑸땲??")

    allowed_domains = load_allowed_email_domains()
    email_domain = normalize_email_domain(email)
    if allowed_domains and email_domain not in allowed_domains:
        raise RuntimeError("?덉슜??뚯궗 ?대찓??꾨찓?몃쭔 媛??붿껌??덉뒿?덈떎.")
    if get_auth_account(user_id):
        raise RuntimeError("?대? ?깅줉?먭굅??뱀씤 ?湲?以묒씤 ?꾩씠?붿엯?덈떎.")

    existing_requests = get_signup_requests_for_email(email)
    if not existing_requests.empty:
        latest_status = clean(existing_requests.iloc[0].get("status")).upper()
        if latest_status in {"PENDING", "HOLD"}:
            raise RuntimeError("媛숈? ?대찓?쇰줈 吏꾪뻾 以묒씤 媛??붿껌??대? ?덉뒿?덈떎.")
        if latest_status == "APPROVED":
            raise RuntimeError("?대? ?뱀씤??媛??붿껌??덉뒿?덈떎.")

    ws = get_or_create_worksheet(get_auth_user_sheet_name(), AUTH_USER_COLUMNS, rows=1000, cols=len(AUTH_USER_COLUMNS))
    timestamp = pd.Timestamp.now(tz="Asia/Seoul").strftime("%Y-%m-%d %H:%M:%S")
    append_dict_row(
        ws,
        {
            "user_id": user_id,
            "password_hash": hash_password(password),
            "display_name": display_name,
            "email": email,
            "role": "viewer",
            "status": "pending",
            "requested_at": timestamp,
        },
        AUTH_USER_COLUMNS,
    )
    save_signup_request(
        {
            "name": display_name,
            "email": email,
            "organization": "",
            "account_type": "viewer",
            "request_note": f"requested_user_id={user_id}",
            "status": "PENDING",
        }
    )
    load_sheet_as_dataframe.clear()
    load_auth_user_accounts.clear()


def resolve_notice_source_key(row: dict | pd.Series | None) -> str:
    if row is not None:
        row_dict = row.to_dict() if isinstance(row, pd.Series) else dict(row)
        source_key = clean(row_dict.get("_source_key")).lower()

        source_alias_map = {
            "tipa": "tipa",
            "mss": "tipa",
            "餓λ쵐?쇗묾怨쀫씜甕겹끉荑귡겫?": "tipa",
            "nipa": "nipa",
            "iris": "iris",
        }

        source_key = source_alias_map.get(source_key, source_key)

        if source_key and source_key != "favorites":
            return source_key

    current_source = clean(get_query_param("source")).lower()

    source_alias_map = {
        "tipa": "tipa",
        "mss": "tipa",
        "餓λ쵐?쇗묾怨쀫씜甕겹끉荑귡겫?": "tipa",
        "nipa": "nipa",
        "iris": "iris",
    }

    current_source = source_alias_map.get(current_source, current_source)

    if current_source in {"tipa", "nipa", "iris"}:
        return current_source

    return "iris"


def normalize_mss_notice_df(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df

    working = df.copy()
    working["registered_at"] = series_from_candidates(working, ["registered_at", "ancm_de", "?깅줉??])
    working["period"] = series_from_candidates(working, ["period", "?좎껌湲곌컙"])
    working["agency"] = series_from_candidates(working, ["agency", "department", "?대떦遺??])
    working["notice_title"] = series_from_candidates(working, ["notice_title", "title", "怨듦퀬紐?])
    working["notice_no"] = series_from_candidates(working, ["notice_no", "ancm_no", "怨듦퀬踰덊샇"])
    working["status"] = series_from_candidates(working, ["status", "怨듦퀬?곹깭"])
    working["views"] = series_from_candidates(working, ["views", "議고쉶"])
    working["detail_link"] = series_from_candidates(working, ["detail_link", "?곸꽭留곹겕"])
    working["review_status"] = series_from_candidates(working, ["review_status", "寃??щ?", "寃?좎뿬遺"])
    working["notice_id"] = series_from_candidates(working, ["notice_id", "怨듦퀬ID"])
    working["_sort_date"] = parse_date_column(working["registered_at"])

    working["?깅줉??] = working["registered_at"]
    working["?좎껌湲곌컙"] = working["period"]
    working["?대떦遺??] = working["agency"]
    working["?꾨Ц湲곌?"] = working["agency"]
    working["怨듦퀬紐?] = working["notice_title"]
    working["怨듦퀬踰덊샇"] = working["notice_no"]
    working["?곹깭"] = working["status"]
    working["怨듦퀬?곹깭"] = working["status"]
    working["議고쉶"] = working["views"]
    working["?곸꽭留곹겕"] = working["detail_link"]
    working["寃??щ?"] = working["review_status"]
    working["怨듦퀬ID"] = working["notice_id"]
    return working.sort_values(by=["_sort_date", "怨듦퀬踰덊샇", "怨듦퀬紐?], ascending=[False, False, True], na_position="last")


def normalize_nipa_notice_df(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df

    working = df.copy()
    working["registered_at"] = series_from_candidates(working, ["registered_at", "ancm_de", "?깅줉??])
    working["period"] = series_from_candidates(working, ["period", "?좎껌湲곌컙"])
    working["business_name"] = series_from_candidates(working, ["business_name", "project_name", "?ъ뾽紐?])
    working["agency"] = series_from_candidates(working, ["agency", "department", "?대떦遺??, "?꾨Ц湲곌?"])
    working["notice_title"] = series_from_candidates(working, ["notice_title", "title", "怨듦퀬紐?])
    working["notice_no"] = series_from_candidates(working, ["notice_no", "ancm_no", "怨듦퀬踰덊샇", "row_number"])
    working["status"] = series_from_candidates(working, ["status", "?곹깭", "怨듦퀬?곹깭"])
    working["detail_link"] = series_from_candidates(working, ["detail_link", "?곸꽭留곹겕"])
    working["review_status"] = series_from_candidates(working, ["review_status", "寃??щ?", "寃?좎뿬遺"])
    working["notice_id"] = series_from_candidates(working, ["notice_id", "怨듦퀬ID"])
    working["d_day"] = series_from_candidates(working, ["d_day", "?⑥??좎껌湲곌컙"])
    working["author"] = series_from_candidates(working, ["author", "?묒꽦??])
    working["_sort_date"] = parse_date_column(working["registered_at"])

    working["?깅줉??] = working["registered_at"]
    working["?좎껌湲곌컙"] = working["period"]
    working["?ъ뾽紐?] = working["business_name"]
    working["?대떦遺??] = working["agency"]
    working["?꾨Ц湲곌?"] = working["agency"]
    working["怨듦퀬紐?] = working["notice_title"]
    working["怨듦퀬踰덊샇"] = working["notice_no"]
    working["?곹깭"] = working["status"]
    working["怨듦퀬?곹깭"] = working["status"]
    working["?곸꽭留곹겕"] = working["detail_link"]
    working["寃??щ?"] = working["review_status"]
    working["怨듦퀬ID"] = working["notice_id"]
    working["?묒꽦??] = working["author"]
    working["?⑥??좎껌湲곌컙"] = working["d_day"]
    return working.sort_values(by=["_sort_date", "怨듦퀬踰덊샇", "怨듦퀬紐?], ascending=[False, False, True], na_position="last")


@st.cache_data(ttl=1800, show_spinner=False)
def load_live_mss_notice_df(max_pages: int = 3) -> pd.DataFrame:
    if crawl_mss_list is None:
        return pd.DataFrame()
    try:
        rows = crawl_mss_list(max_pages=max_pages)
    except Exception:
        return pd.DataFrame()
    return normalize_mss_notice_df(pd.DataFrame(rows))


def filter_source_notice_view(df: pd.DataFrame, *, past: bool) -> pd.DataFrame:
    if df.empty:
        return df

    working = df.copy()
    archive_mask = build_notice_archive_mask(working)
    if not archive_mask.empty and archive_mask.any():
        return working[archive_mask].copy() if past else working[~archive_mask].copy()

    if "is_current" in working.columns:
        current_values = working["is_current"].fillna("").astype(str).str.strip()
        if current_values.ne("").any():
            if past:
                return working[current_values.ne("Y")]
            return working[current_values.eq("Y")]

    return working


def load_source_notice_sheet(
    *,
    primary_sheet_name: str,
    master_sheet_name: str,
    normalize_func,
    past: bool,
    live_fallback=None,
) -> tuple[pd.DataFrame, str]:
    sheet_df = load_optional_sheet_as_dataframe(primary_sheet_name)
    if not sheet_df.empty:
        return normalize_func(sheet_df), f"Google Sheet: {primary_sheet_name}"

    master_df = load_optional_sheet_as_dataframe(master_sheet_name)
    if not master_df.empty:
        normalized = normalize_func(master_df)
        filtered = filter_source_notice_view(normalized, past=past)
        return filtered, f"Google Sheet: {master_sheet_name} (fallback)"

    if live_fallback is not None:
        live_df = live_fallback()
        if not live_df.empty:
            return live_df, "Live crawl fallback"

    return pd.DataFrame(), "No data"


def load_mss_notice_df() -> tuple[pd.DataFrame, str]:
    sheet_name = get_env("MSS_CURRENT_SHEET") or get_env("MSS_NOTICE_SHEET", "MSS_CURRENT")
    return load_source_notice_sheet(
        primary_sheet_name=sheet_name,
        master_sheet_name=get_env("MSS_NOTICE_MASTER_SHEET", "MSS_NOTICE_MASTER"),
        normalize_func=normalize_mss_notice_df,
        past=False,
        live_fallback=lambda: load_live_mss_notice_df(max_pages=3),
    )


def load_mss_past_df() -> tuple[pd.DataFrame, str]:
    sheet_name = get_env("MSS_PAST_SHEET", "MSS_PAST")
    return load_source_notice_sheet(
        primary_sheet_name=sheet_name,
        master_sheet_name=get_env("MSS_NOTICE_MASTER_SHEET", "MSS_NOTICE_MASTER"),
        normalize_func=normalize_mss_notice_df,
        past=True,
    )


@st.cache_data(ttl=1800, show_spinner=False)
def load_live_nipa_notice_df(max_pages: int = 3) -> pd.DataFrame:
    if crawl_nipa_list is None:
        return pd.DataFrame()
    try:
        rows = crawl_nipa_list(max_pages=max_pages)
    except Exception:
        return pd.DataFrame()
    return normalize_nipa_notice_df(pd.DataFrame(rows))


def load_nipa_notice_df() -> tuple[pd.DataFrame, str]:
    sheet_name = get_env("NIPA_CURRENT_SHEET", "NIPA_CURRENT")
    return load_source_notice_sheet(
        primary_sheet_name=sheet_name,
        master_sheet_name=get_env("NIPA_NOTICE_MASTER_SHEET", "NIPA_NOTICE_MASTER"),
        normalize_func=normalize_nipa_notice_df,
        past=False,
        live_fallback=lambda: load_live_nipa_notice_df(max_pages=3),
    )


def load_nipa_past_df() -> tuple[pd.DataFrame, str]:
    sheet_name = get_env("NIPA_PAST_SHEET", "NIPA_PAST")
    return load_source_notice_sheet(
        primary_sheet_name=sheet_name,
        master_sheet_name=get_env("NIPA_NOTICE_MASTER_SHEET", "NIPA_NOTICE_MASTER"),
        normalize_func=normalize_nipa_notice_df,
        past=True,
    )


@st.cache_data(ttl=1800, show_spinner=False)
def load_mss_opportunity_df() -> pd.DataFrame:
    sheet_name = resolve_mss_opportunity_current_sheet(get_env)
    return enrich_opportunity_df(load_optional_sheet_as_dataframe(sheet_name))


@st.cache_data(ttl=1800, show_spinner=False)
def load_mss_opportunity_archive_df() -> pd.DataFrame:
    sheet_name = get_env("MSS_OPPORTUNITY_ARCHIVE_SHEET", "MSS_OPPORTUNITY_ARCHIVE")
    return enrich_opportunity_df(load_optional_sheet_as_dataframe(sheet_name))


@st.cache_data(ttl=1800, show_spinner=False)
def load_nipa_opportunity_df() -> pd.DataFrame:
    sheet_name = resolve_nipa_opportunity_current_sheet(get_env)
    return enrich_opportunity_df(load_optional_sheet_as_dataframe(sheet_name))


@st.cache_data(ttl=1800, show_spinner=False)
def load_nipa_opportunity_archive_df() -> pd.DataFrame:
    sheet_name = get_env("NIPA_OPPORTUNITY_ARCHIVE_SHEET", "NIPA_OPPORTUNITY_ARCHIVE")
    return enrich_opportunity_df(load_optional_sheet_as_dataframe(sheet_name))


def find_header_column(header: list[str], candidates: list[str]) -> int | None:
    for candidate in candidates:
        if candidate in header:
            return header.index(candidate) + 1
    return None


def update_notice_review_status(notice_id: str, review_status: str) -> None:
    notice_id = clean(notice_id)
    if not notice_id:
        raise RuntimeError("怨듦퀬ID媛 ?놁뼱 寃??щ?瑜???ν븷 ??놁뒿?덈떎.")

    notice_master_sheet = get_env("NOTICE_MASTER_SHEET", "IRIS_NOTICE_MASTER")
    ws = get_worksheet(notice_master_sheet)
    values = run_gspread_call(ws.get_all_values)
    if not values:
        raise RuntimeError("IRIS_NOTICE_MASTER ?쒗듃媛 鍮꾩뼱 ?덉뒿?덈떎.")

    header = [clean(x) for x in values[0]]
    notice_id_col = find_header_column(header, ["怨듦퀬ID", "notice_id"])
    review_col = find_header_column(header, ["寃??щ?", "寃?좎뿬遺", "review_status"])
    if not notice_id_col:
        raise RuntimeError("?꾩닔 而щ읆??놁뒿?덈떎: 怨듦퀬ID/notice_id")
    if not review_col:
        review_col = len(header) + 1
        ws.update_cell(1, review_col, "review_status")

    for row_index, row in enumerate(values[1:], start=2):
        current_notice_id = clean(row[notice_id_col - 1] if notice_id_col - 1 < len(row) else "")
        if current_notice_id == notice_id:
            ws.update_cell(row_index, review_col, clean(review_status))
            load_sheet_as_dataframe.clear()
            load_app_datasets.clear()
            return

    raise RuntimeError(f"IRIS_NOTICE_MASTER?먯꽌 怨듦퀬ID {notice_id}瑜?李얠? 紐삵뻽?듬땲??")


def update_mss_review_status(notice_id: str, review_status: str) -> None:
    notice_id = clean(notice_id)
    if not notice_id:
        raise RuntimeError("怨듦퀬ID媛 ?놁뼱 寃??щ?瑜???ν븷 ??놁뒿?덈떎.")

    sheet_names = [
        get_env("MSS_CURRENT_SHEET") or get_env("MSS_NOTICE_SHEET", "MSS_CURRENT"),
        get_env("MSS_PAST_SHEET", "MSS_PAST"),
    ]
    checked_sheets = []
    for sheet_name in dict.fromkeys([name for name in sheet_names if clean(name)]):
        checked_sheets.append(sheet_name)
        try:
            ws = get_worksheet(sheet_name)
        except Exception:
            continue

        values = run_gspread_call(ws.get_all_values)
        if not values:
            continue

        header = [clean(x) for x in values[0]]
        notice_id_col = find_header_column(header, ["怨듦퀬ID", "notice_id"])
        if not notice_id_col:
            continue

        review_col = find_header_column(header, ["寃??щ?", "寃?좎뿬遺", "review_status"])
        if not review_col:
            review_col = len(header) + 1
            ws.update_cell(1, review_col, "review_status")

        for row_index, row in enumerate(values[1:], start=2):
            current_notice_id = clean(row[notice_id_col - 1] if notice_id_col - 1 < len(row) else "")
            if current_notice_id == notice_id:
                ws.update_cell(row_index, review_col, clean(review_status))
                load_sheet_as_dataframe.clear()
                build_source_datasets.clear()
                load_app_datasets.clear()
                return

    raise RuntimeError(f"以묒냼湲곗뾽踰ㅼ쿂遺 ?쒗듃({', '.join(checked_sheets)})?먯꽌 怨듦퀬ID {notice_id}瑜?李얠? 紐삵뻽?듬땲??")


def update_nipa_review_status(notice_id: str, review_status: str) -> None:
    notice_id = clean(notice_id)
    if not notice_id:
        raise RuntimeError("怨듦퀬ID媛 ?놁뼱 寃??щ?瑜???ν븷 ??놁뒿?덈떎.")

    sheet_names = [
        get_env("NIPA_CURRENT_SHEET", "NIPA_CURRENT"),
        get_env("NIPA_PAST_SHEET", "NIPA_PAST"),
        get_env("NIPA_NOTICE_MASTER_SHEET", "NIPA_NOTICE_MASTER"),
    ]
    checked_sheets = []
    for sheet_name in dict.fromkeys([name for name in sheet_names if clean(name)]):
        checked_sheets.append(sheet_name)
        try:
            ws = get_worksheet(sheet_name)
        except Exception:
            continue

        values = run_gspread_call(ws.get_all_values)
        if not values:
            continue

        header = [clean(x) for x in values[0]]
        notice_id_col = find_header_column(header, ["怨듦퀬ID", "notice_id"])
        if not notice_id_col:
            continue

        review_col = find_header_column(header, ["寃??щ?", "寃?좎뿬遺", "review_status"])
        if not review_col:
            review_col = len(header) + 1
            ws.update_cell(1, review_col, "review_status")

        for row_index, row in enumerate(values[1:], start=2):
            current_notice_id = clean(row[notice_id_col - 1] if notice_id_col - 1 < len(row) else "")
            if current_notice_id == notice_id:
                ws.update_cell(row_index, review_col, clean(review_status))
                load_sheet_as_dataframe.clear()
                build_source_datasets.clear()
                load_app_datasets.clear()
                return

    raise RuntimeError(f"NIPA ?쒗듃({', '.join(checked_sheets)})?먯꽌 怨듦퀬ID {notice_id}瑜?李얠? 紐삵뻽?듬땲??")


def enrich_notice_df(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    enriched = df.copy()
    enriched["怨듦퀬ID"] = series_from_candidates(enriched, ["怨듦퀬ID", "notice_id"])
    enriched["怨듦퀬?쇱옄"] = series_from_candidates(enriched, ["怨듦퀬?쇱옄", "registered_at", "ancm_de"])
    enriched["怨듦퀬踰덊샇"] = series_from_candidates(enriched, ["怨듦퀬踰덊샇", "notice_no", "ancm_no"])
    enriched["怨듦퀬紐?] = series_from_candidates(enriched, ["怨듦퀬紐?, "notice_title", "title"])
    enriched["?꾨Ц湲곌?"] = series_from_candidates(enriched, ["?꾨Ц湲곌?", "agency"])
    enriched["?뚭?遺泥?] = series_from_candidates(enriched, ["?뚭?遺泥?, "ministry"])
    enriched["寃??щ?"] = series_from_candidates(enriched, ["寃??щ?", "寃?좎뿬遺", "review_status"])
    enriched["?곸꽭留곹겕"] = series_from_candidates(enriched, ["?곸꽭留곹겕", "detail_link"])
    if "??쒖젏?? in enriched.columns:
        enriched["??쒖젏??] = to_numeric_column(enriched["??쒖젏??])
    else:
        enriched["??쒖젏??] = 0
    enriched["怨듦퀬?곹깭"] = series_from_candidates(enriched, ["怨듦퀬?곹깭", "status", "rcve_status"])
    enriched["?묒닔湲곌컙"] = series_from_candidates(enriched, ["?묒닔湲곌컙", "period"])
    enriched["?곹깭??] = series_from_candidates(enriched, ["?곹깭??, "status_key"])
    enriched["rcve_status"] = series_from_candidates(enriched, ["rcve_status", "怨듦퀬?곹깭"])
    enriched["period"] = series_from_candidates(enriched, ["period", "?묒닔湲곌컙"])
    enriched["status_key"] = series_from_candidates(enriched, ["status_key", "?곹깭??])
    enriched["?곸꽭留곹겕"] = enriched.apply(resolve_external_detail_link, axis=1)
    enriched["detail_link"] = enriched["?곸꽭留곹겕"]
    enriched["_view_status"] = enriched.apply(classify_notice_status_for_view, axis=1)
    enriched["怨듦퀬?곹깭"] = series_from_candidates(enriched, ["_view_status", "怨듦퀬?곹깭", "rcve_status"])
    enriched["rcve_status"] = series_from_candidates(enriched, ["_view_status", "rcve_status", "怨듦퀬?곹깭"])
    if "怨듦퀬?쇱옄" in enriched.columns:
        enriched["_sort_date"] = parse_date_column(enriched["怨듦퀬?쇱옄"])
    else:
        enriched["_sort_date"] = pd.NaT
    return enriched.sort_values(
        by=["_sort_date", "??쒖젏??, "怨듦퀬紐?],
        ascending=[False, False, True],
        na_position="last",
    )


def enrich_opportunity_df(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    enriched = df.copy()

    enriched["rfp_score"] = to_numeric_column(series_from_candidates(enriched, ["?먯닔", "rfp_score"]))
    enriched["budget"] = series_from_candidates(enriched, ["?덉궛", "budget"]).fillna("").astype(str).apply(extract_budget_summary)

    enriched["怨듦퀬?쇱옄"] = series_from_candidates(enriched, ["怨듦퀬?쇱옄", "registered_at", "ancm_de"])
    enriched["怨듦퀬踰덊샇"] = series_from_candidates(enriched, ["怨듦퀬踰덊샇", "notice_no", "ancm_no"])
    enriched["?꾨Ц湲곌?紐?] = series_from_candidates(enriched, ["?꾨Ц湲곌?紐?, "?꾨Ц湲곌?", "agency"])
    enriched["怨듦퀬紐?] = series_from_candidates(enriched, ["怨듦퀬紐?, "notice_title"])
    enriched["異붿쿇?щ?"] = series_from_candidates(enriched, ["異붿쿇?щ?", "recommendation"])
    enriched["怨듦퀬?곹깭"] = series_from_candidates(enriched, ["怨듦퀬?곹깭", "status", "rcve_status"])
    enriched["?묒닔湲곌컙"] = series_from_candidates(enriched, ["?묒닔湲곌컙", "period"])
    enriched["寃?좎뿬遺"] = series_from_candidates(enriched, ["寃?좎뿬遺", "寃??щ?", "review_status"])
    enriched["?곸꽭留곹겕"] = series_from_candidates(enriched, ["?곸꽭留곹겕", "detail_link"])
    enriched["?대떦 怨쇱젣紐?] = series_from_candidates(enriched, ["?대떦 怨쇱젣紐?, "怨쇱젣紐?, "project_name", "llm_project_name"])
    enriched["?먯닔"] = series_from_candidates(enriched, ["?먯닔", "rfp_score", "llm_fit_score"])
    enriched["?덉궛"] = series_from_candidates(
        enriched,
        ["?덉궛", "budget", "llm_total_budget_text", "total_budget_text"],
    ).fillna("").astype(str).apply(extract_budget_summary)

    enriched["notice_title"] = series_from_candidates(enriched, ["怨듦퀬紐?, "notice_title"])
    enriched["project_name"] = series_from_candidates(enriched, ["怨쇱젣紐?, "project_name"])
    enriched["rfp_title"] = series_from_candidates(enriched, ["RFP ?쒕ぉ", "rfp_title"])
    enriched["recommendation"] = series_from_candidates(enriched, ["異붿쿇?щ?", "recommendation"])
    enriched["agency"] = series_from_candidates(enriched, ["?꾨Ц湲곌?紐?, "?꾨Ц湲곌?", "agency"])
    enriched["ministry"] = series_from_candidates(enriched, ["?뚭?遺泥?, "ministry"])
    enriched["ancm_de"] = series_from_candidates(enriched, ["怨듦퀬?쇱옄", "registered_at", "ancm_de"])
    enriched["ancm_no"] = series_from_candidates(enriched, ["怨듦퀬踰덊샇", "notice_no", "ancm_no"])
    enriched["rcve_status"] = series_from_candidates(enriched, ["怨듦퀬?곹깭", "status", "rcve_status"])
    enriched["period"] = series_from_candidates(enriched, ["?묒닔湲곌컙", "period"])
    enriched["detail_link"] = series_from_candidates(enriched, ["?곸꽭留곹겕", "detail_link"])
    enriched["review_status"] = series_from_candidates(enriched, ["寃?좎뿬遺", "寃??щ?", "review_status"])
    enriched["notice_id"] = series_from_candidates(enriched, ["怨듦퀬ID", "notice_id"])
    enriched["?곸꽭留곹겕"] = enriched.apply(resolve_external_detail_link, axis=1)
    enriched["detail_link"] = enriched["?곸꽭留곹겕"]
    enriched["document_id"] = series_from_candidates(enriched, ["臾몄꽌ID", "document_id"])
    enriched["keywords"] = series_from_candidates(enriched, ["?ㅼ썙??, "keywords"])
    enriched["reason"] = series_from_candidates(enriched, ["異붿쿇?댁쑀", "reason"])
    enriched["concept_and_development"] = series_from_candidates(enriched, ["媛쒕뀗 諛?媛쒕컻 ?댁슜", "concept_and_development"])
    enriched["support_necessity"] = series_from_candidates(enriched, ["吏?먰븘?붿꽦(怨쇱젣 諛곌꼍)", "support_necessity"])
    enriched["application_field"] = series_from_candidates(enriched, ["?쒖슜遺꾩빞", "application_field"])
    enriched["support_plan"] = series_from_candidates(enriched, ["吏?먭린媛?諛??덉궛쨌異붿쭊泥닿퀎", "support_plan"])
    enriched["technical_background"] = series_from_candidates(enriched, ["湲곗닠媛쒕컻 諛곌꼍 諛?吏?먰븘?붿꽦", "technical_background"])
    enriched["development_content"] = series_from_candidates(enriched, ["湲곗닠媛쒕컻 ?댁슜", "development_content"])
    enriched["support_need"] = series_from_candidates(enriched, ["吏?먰븘?붿꽦", "support_need"])
    enriched["document_type"] = series_from_candidates(enriched, ["臾몄꽌?좏삎", "document_type"])
    enriched["file_type"] = series_from_candidates(enriched, ["?뚯씪?좏삎", "file_type"])
    enriched["source_site"] = series_from_candidates(enriched, ["異쒖쿂?ъ씠??, "source_site"])
    enriched["notice_is_current"] = series_from_candidates(enriched, ["notice_is_current", "is_current"])
    enriched["notice_status"] = series_from_candidates(enriched, ["notice_status", "怨듦퀬?곹깭", "rcve_status"])
    enriched["notice_period"] = series_from_candidates(enriched, ["notice_period", "?묒닔湲곌컙", "period"])
    enriched["file_name"] = series_from_candidates(enriched, ["?뚯씪紐?, "file_name"])
    enriched["file_path"] = series_from_candidates(enriched, ["?뚯씪寃쎈줈", "file_path"])
    enriched["document_role"] = series_from_candidates(enriched, ["臾몄꽌??븷", "document_role"])
    enriched["project_name_source"] = series_from_candidates(enriched, ["怨쇱젣紐낃렐嫄?, "project_name_source"])
    enriched["project_name_confidence"] = series_from_candidates(enriched, ["怨쇱젣紐낆떊猶곕룄", "project_name_confidence"])
    enriched["rfp_title_source"] = series_from_candidates(enriched, ["RFP?쒕ぉ洹쇨굅", "rfp_title_source"])
    enriched["evidence"] = series_from_candidates(enriched, ["洹쇨굅臾몄옣", "evidence"])
    enriched["conflict_flags"] = series_from_candidates(enriched, ["異⑸룎?뚮옒洹?, "conflict_flags"])

    if "怨듦퀬?쇱옄" in enriched.columns:
        enriched["_sort_date"] = parse_date_column(enriched["怨듦퀬?쇱옄"])
    else:
        enriched["_sort_date"] = parse_date_column(enriched["ancm_de"])
    return enriched.sort_values(
        by=["_sort_date", "rfp_score", "notice_title", "project_name"],
        ascending=[False, False, True, True],
        na_position="last",
    )


def enrich_opportunity_with_notice_meta(opportunity_df: pd.DataFrame, notice_df: pd.DataFrame) -> pd.DataFrame:
    if opportunity_df.empty:
        return opportunity_df

    enriched = opportunity_df.copy()
    if notice_df.empty or "怨듦퀬ID" not in notice_df.columns:
        return enriched

    notice_meta = notice_df.copy()
    notice_meta["怨듦퀬ID"] = notice_meta["怨듦퀬ID"].fillna("").astype(str).str.strip()
    keep_columns = [
        "怨듦퀬ID",
        "怨듦퀬?쇱옄",
        "怨듦퀬踰덊샇",
        "?꾨Ц湲곌?",
        "怨듦퀬紐?,
        "怨듦퀬?곹깭",
        "?묒닔湲곌컙",
        "寃??щ?",
        "?곸꽭留곹겕",
        "?뚭?遺泥?,
        "?곹깭??,
        "status_key",
        "is_current",
    ]
    available_columns = [column for column in keep_columns if column in notice_meta.columns]
    notice_meta = notice_meta[available_columns].drop_duplicates(subset=["怨듦퀬ID"], keep="first")

    enriched["notice_id"] = series_from_candidates(enriched, ["notice_id", "怨듦퀬ID"])
    merged = enriched.merge(notice_meta, left_on="notice_id", right_on="怨듦퀬ID", how="left", suffixes=("", "_notice"))

    fallback_pairs = {
        "怨듦퀬?쇱옄": ["怨듦퀬?쇱옄", "ancm_de"],
        "怨듦퀬踰덊샇": ["怨듦퀬踰덊샇", "ancm_no"],
        "?꾨Ц湲곌?紐?: ["?꾨Ц湲곌?紐?, "agency", "?꾨Ц湲곌?"],
        "怨듦퀬紐?: ["怨듦퀬紐?, "notice_title"],
        "異붿쿇?щ?": ["異붿쿇?щ?", "recommendation"],
        "怨듦퀬?곹깭": ["怨듦퀬?곹깭", "rcve_status"],
        "?묒닔湲곌컙": ["?묒닔湲곌컙", "period"],
        "寃?좎뿬遺": ["寃?좎뿬遺", "review_status", "寃??щ?"],
        "?곸꽭留곹겕": ["?곸꽭留곹겕", "detail_link"],
        "?뚭?遺泥?: ["?뚭?遺泥?, "ministry"],
        "?곹깭??: ["?곹깭??, "status_key"],
        "notice_is_current": ["notice_is_current", "is_current"],
        "notice_status": ["notice_status", "怨듦퀬?곹깭", "rcve_status"],
        "notice_period": ["notice_period", "?묒닔湲곌컙", "period"],
    }
    for target, candidates in fallback_pairs.items():
        candidate_columns = [target]
        for candidate in candidates:
            candidate_columns.append(candidate)
            notice_candidate = f"{candidate}_notice"
            if notice_candidate in merged.columns:
                candidate_columns.append(notice_candidate)
        merged[target] = series_from_candidates(merged, candidate_columns)

    # Keep canonical internal fields aligned with the notice-level fallback columns
    # so detail views do not show blanks when only the display alias was filled.
    merged["notice_title"] = series_from_candidates(merged, ["notice_title", "怨듦퀬紐?])
    merged["agency"] = series_from_candidates(merged, ["agency", "?꾨Ц湲곌?", "?꾨Ц湲곌?紐?])
    merged["ministry"] = series_from_candidates(merged, ["ministry", "二쇨?遺泥?])
    merged["ancm_de"] = series_from_candidates(merged, ["ancm_de", "怨듦퀬?쇱옄"])
    merged["ancm_no"] = series_from_candidates(merged, ["ancm_no", "怨듦퀬踰덊샇"])
    merged["rcve_status"] = series_from_candidates(merged, ["rcve_status", "怨듦퀬?곹깭"])
    merged["period"] = series_from_candidates(merged, ["period", "?묒닔湲곌컙"])
    merged["detail_link"] = series_from_candidates(merged, ["detail_link", "?곸꽭留곹겕"])
    merged["?곸꽭留곹겕"] = merged.apply(resolve_external_detail_link, axis=1)
    merged["detail_link"] = merged["?곸꽭留곹겕"]
    merged["review_status"] = series_from_candidates(merged, ["review_status", "寃?좎뿬遺", "寃?좎셿猷뚯뿬遺"])
    merged["notice_is_current"] = series_from_candidates(merged, ["notice_is_current", "is_current", "is_current_notice"])
    merged["notice_status"] = series_from_candidates(merged, ["notice_status", "怨듦퀬?곹깭", "rcve_status"])
    merged["notice_period"] = series_from_candidates(merged, ["notice_period", "?묒닔湲곌컙", "period"])

    return merged


def enrich_summary_df(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    enriched = df.copy()
    if "??쒖젏?? in enriched.columns:
        enriched["??쒖젏??] = to_numeric_column(enriched["??쒖젏??])
    if "?덉궛" in enriched.columns:
        enriched["?덉궛"] = enriched["?덉궛"].fillna("").astype(str).apply(extract_budget_summary)
    if "怨듦퀬?쇱옄" in enriched.columns:
        enriched["_sort_date"] = parse_date_column(enriched["怨듦퀬?쇱옄"])
    else:
        enriched["_sort_date"] = pd.NaT
    return enriched.sort_values(
        by=["_sort_date", "??쒖젏??, "怨듦퀬紐?],
        ascending=[False, False, True],
        na_position="last",
    )


def enrich_error_df(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    enriched = df.copy()
    enriched["source_site"] = series_from_candidates(enriched, ["source_site", "異쒖쿂?ъ씠??])
    enriched["notice_id"] = series_from_candidates(enriched, ["notice_id", "怨듦퀬ID"])
    enriched["notice_title"] = series_from_candidates(enriched, ["notice_title", "怨듦퀬紐?])
    enriched["project_name"] = series_from_candidates(enriched, ["project_name", "怨쇱젣紐?])
    enriched["rfp_title"] = series_from_candidates(enriched, ["rfp_title", "RFP ?쒕ぉ"])
    enriched["file_name"] = series_from_candidates(enriched, ["file_name", "?뚯씪紐?])
    enriched["validation_errors"] = series_from_candidates(enriched, ["validation_errors", "寃利앹삤瑜?])
    enriched["updated_at"] = series_from_candidates(enriched, ["updated_at", "?섏젙?쇱떆"])
    return enriched


def is_closed_status_value(value: object) -> bool:
    text = clean(value).lower()
    if not text or any(marker in text for marker in OPEN_STATUS_MARKERS):
        return False
    compact_text = "".join(text.split())
    compact_closed_values = {"".join(status.split()) for status in CLOSED_STATUS_VALUES}
    return (
        text in CLOSED_STATUS_VALUES
        or compact_text in compact_closed_values
        or compact_text.endswith("留덇컧")
        or compact_text.endswith("醫낅즺")
    )


def normalize_notice_status_label(value: object) -> str:
    text = clean(value)
    lowered = text.lower()
    if not text:
        return ""
    if "?덉젙" in text or "pre" in lowered:
        return "?덉젙"
    if "?묒닔以? in text or "怨듦퀬以? in text or "吏꾪뻾" in text or "ing" in lowered or "open" in lowered:
        return "?묒닔以?
    if "留덇컧" in text or "醫낅즺" in text or "closed" in lowered or "end" in lowered:
        return "留덇컧"
    return text


def normalize_review_status_label(value: object) -> str:
    return clean(value).replace(" ", "")


def is_archived_review_status_value(value: object) -> bool:
    return normalize_review_status_label(value) in {
        status.replace(" ", "")
        for status in ARCHIVE_REVIEW_STATUS_VALUES
    }


def derive_archive_reason_for_app(row: dict[str, object] | pd.Series) -> str:
    row_dict = row.to_dict() if isinstance(row, pd.Series) else dict(row or {})
    manual_archive = clean(first_non_empty(row_dict, "manual_archive")).upper() == "Y"
    review_status = first_non_empty(row_dict, "review_status", "寃?좎뿬遺", "寃??щ?")
    current_value = first_non_empty(row_dict, "notice_is_current", "is_current")
    status_text = first_non_empty(row_dict, "notice_status", "status", "rcve_status", "怨듦퀬?곹깭")
    period_text = first_non_empty(row_dict, "notice_period", "period", "?묒닔湲곌컙", "?좎껌湲곌컙")
    period_end = extract_period_end(period_text)

    if manual_archive:
        return "manual_archive"
    if is_archived_review_status_value(review_status):
        return "review_archived"
    if pd.notna(period_end):
        period_end_ts = pd.Timestamp(period_end).normalize()
        if period_end_ts < pd.Timestamp.now().normalize():
            if clean(current_value) == "N" or normalize_notice_status_label(status_text) == "留덇컧":
                return "notice_closed"
            return "application_closed"
    return ""


def derive_archive_reason_label_for_app(row: dict[str, object] | pd.Series) -> str:
    row_dict = row.to_dict() if isinstance(row, pd.Series) else dict(row or {})
    existing = first_non_empty(row_dict, "archive_reason_label")
    if existing:
        return existing
    mapping = {
        "notice_closed": "怨듦퀬 留덇컧",
        "application_closed": "?묒닔 留덇컧",
        "manual_archive": "?섎룞 蹂닿?",
        "review_archived": "寃??蹂닿?",
    }
    return mapping.get(derive_archive_reason_for_app(row_dict), "")


def build_notice_archive_mask(df: pd.DataFrame) -> pd.Series:
    if df.empty:
        return pd.Series(dtype="bool")

    status_series = series_from_candidates(
        df,
        ["怨듦퀬?곹깭", "?곹깭", "status", "rcve_status", "notice_status"],
    )
    review_series = series_from_candidates(
        df,
        ["寃??щ?", "寃?좎뿬遺", "review_status"],
    )
    closed_mask = status_series.fillna("").astype(str).apply(is_closed_status_value)
    review_mask = review_series.fillna("").astype(str).apply(is_archived_review_status_value)
    return closed_mask | review_mask


def build_notice_status_scope_mask(df: pd.DataFrame, status_scope: str) -> pd.Series:
    if df.empty:
        return pd.Series(dtype="bool")
    if status_scope == "?꾩껜":
        return pd.Series(True, index=df.index)

    status_series = series_from_candidates(
        df,
        ["怨듦퀬?곹깭", "?곹깭", "status", "rcve_status", "notice_status"],
    )
    normalized = status_series.fillna("").astype(str).apply(normalize_notice_status_label)
    return normalized.eq(status_scope)


def build_opportunity_status_scope_mask(df: pd.DataFrame, status_scope: str) -> pd.Series:
    if df.empty:
        return pd.Series(dtype="bool")
    if status_scope == "?꾩껜":
        return pd.Series(True, index=df.index)

    status_series = series_from_candidates(
        df,
        ["notice_status", "怨듦퀬?곹깭", "status", "rcve_status"],
    )
    normalized = status_series.fillna("").astype(str).apply(normalize_notice_status_label)
    return normalized.eq(status_scope)


def build_summary_current_mask(df: pd.DataFrame) -> pd.Series:
    if df.empty:
        return pd.Series(dtype="bool")

    mask = pd.Series(True, index=df.index)
    if "is_current" in df.columns:
        mask = mask & df["is_current"].fillna("").astype(str).str.strip().eq("Y")
    if "怨듦퀬?곹깭" in df.columns:
        mask = mask & ~df["怨듦퀬?곹깭"].apply(is_closed_status_value)
    return mask


def build_opportunity_archive_mask(df: pd.DataFrame) -> pd.Series:
    if df.empty:
        return pd.Series(dtype="bool")

    archive_mask = pd.Series(False, index=df.index)
    review_series = series_from_candidates(df, ["review_status", "寃?좎뿬遺", "寃??щ?"])
    archive_mask = archive_mask | review_series.apply(is_archived_review_status_value)

    for status_source in ["notice_status", "status", "rcve_status", "怨듦퀬?곹깭", "?⑤벀??怨밴묶"]:
        if status_source in df.columns:
            archive_mask = archive_mask | df[status_source].apply(is_closed_status_value)

    status_key_source = "status_key" if "status_key" in df.columns else "?곹깭??
    if status_key_source in df.columns:
        status_key = df[status_key_source].fillna("").astype(str).str.strip()
        archive_mask = archive_mask | status_key.isin(["ancmCls", "ancmEnd"])

    period_source = "notice_period" if "notice_period" in df.columns else "period" if "period" in df.columns else "?묒닔湲곌컙"
    if period_source in df.columns:
        missing_period_mask = df[period_source].fillna("").astype(str).str.strip().eq("")
        archive_mask = archive_mask | missing_period_mask

    return archive_mask


def build_current_opportunity_mask(df: pd.DataFrame) -> pd.Series:
    if df.empty:
        return pd.Series(dtype="bool")

    mask = ~build_opportunity_archive_mask(df)
    current_source = "notice_is_current" if "notice_is_current" in df.columns else "is_current"
    if current_source in df.columns:
        mask = mask & df[current_source].fillna("").astype(str).str.strip().eq("Y")
    return mask


def filter_current_notice_rows(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    archive_mask = build_notice_archive_mask(df)
    if archive_mask.empty:
        return df
    return df[~archive_mask].copy()


def filter_archived_notice_rows(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    archive_mask = build_notice_archive_mask(df)
    if archive_mask.empty:
        return pd.DataFrame(columns=df.columns)
    return df[archive_mask].copy()


def filter_notice_status_scope(df: pd.DataFrame, status_scope: str) -> pd.DataFrame:
    if df.empty or status_scope == "?꾩껜":
        return df
    scope_mask = build_notice_status_scope_mask(df, status_scope)
    return df[scope_mask].copy()


def filter_current_summary_rows(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    return df[build_summary_current_mask(df)].copy()


def filter_current_opportunity_rows(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    return df[build_current_opportunity_mask(df)].copy()


def filter_archived_opportunity_rows(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    return df[build_opportunity_archive_mask(df)].copy()


def _is_placeholder_opportunity_text(value: object) -> bool:
    text = clean(value)
    if not text:
        return False
    compact = re.sub(r"\s+", " ", text).strip()
    normalized = compact.lower()
    if normalized in {
        "?뺤씤 ??대떦 rfp??묒닔",
        "湲곗닠 遺꾨쪟",
        "?곌뎄媛쒕컻怨꾪쉷??묒꽦?쒖떇",
        "r&d ?먯쑉?깊듃??,
    }:
        return True
    if compact.startswith("><") or compact.count("><") >= 2:
        return True
    return any(
        marker in compact
        for marker in [
            "愿由щ쾲??,
            "?좎젙?덉젙 怨쇱젣??,
            "?뱁빐 ?곌뎄鍮?,
            "?댁뿭 ?ъ뾽紐?,
            "?遺꾨쪟",
            "以묐텇瑜?,
            "?뚮텇瑜?,
            "吏?먭린媛?吏?먭퇋紐?,
            "?묒꽦?쒖떇",
        ]
    )


def build_placeholder_opportunity_mask(df: pd.DataFrame) -> pd.Series:
    if df.empty:
        return pd.Series(dtype=bool)
    project_name = series_from_candidates(df, ["llm_project_name", "project_name", "怨쇱젣紐?]).fillna("").astype(str).str.strip()
    rfp_title = series_from_candidates(df, ["llm_rfp_title", "rfp_title", "RFP ?쒕ぉ"]).fillna("").astype(str).str.strip()
    project_placeholder = project_name.apply(_is_placeholder_opportunity_text)
    rfp_placeholder = rfp_title.apply(_is_placeholder_opportunity_text)
    return project_placeholder | ((project_name == "") & rfp_placeholder)


def filter_rankable_opportunity_rows(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    placeholder_mask = build_placeholder_opportunity_mask(df)
    filtered = df[~placeholder_mask].copy()
    return filtered if not filtered.empty else df.copy()


def enrich_summary_with_notice_meta(summary_df: pd.DataFrame, notice_df: pd.DataFrame) -> pd.DataFrame:
    if summary_df.empty:
        return summary_df

    enriched = summary_df.copy()
    if notice_df.empty or "怨듦퀬ID" not in enriched.columns or "怨듦퀬ID" not in notice_df.columns:
        return enriched

    notice_meta = notice_df.copy()
    notice_meta["怨듦퀬ID"] = notice_meta["怨듦퀬ID"].fillna("").astype(str).str.strip()
    keep_columns = ["怨듦퀬ID", "?곸꽭留곹겕", "寃??щ?", "?꾨Ц湲곌?", "?뚭?遺泥?, "怨듦퀬?곹깭", "?묒닔湲곌컙", "怨듦퀬?쇱옄", "?곹깭??, "status_key", "is_current"]
    available_columns = [column for column in keep_columns if column in notice_meta.columns]
    notice_meta = notice_meta[available_columns].drop_duplicates(subset=["怨듦퀬ID"], keep="first")

    enriched["怨듦퀬ID"] = enriched["怨듦퀬ID"].fillna("").astype(str).str.strip()
    merged = enriched.merge(notice_meta, on="怨듦퀬ID", how="left", suffixes=("", "_notice"))

    for target in ["?곸꽭留곹겕", "寃??щ?", "?꾨Ц湲곌?", "?뚭?遺泥?, "怨듦퀬?곹깭", "?묒닔湲곌컙", "怨듦퀬?쇱옄", "?곹깭??, "status_key", "is_current"]:
        candidate_columns = [target]
        notice_target = f"{target}_notice"
        if notice_target in merged.columns:
            candidate_columns.append(notice_target)
        merged[target] = series_from_candidates(merged, candidate_columns)

    merged["?곸꽭留곹겕"] = merged.apply(resolve_external_detail_link, axis=1)
    return merged


def build_notice_analysis_summary(opportunity_df: pd.DataFrame) -> pd.DataFrame:
    if opportunity_df.empty or "notice_id" not in opportunity_df.columns:
        return pd.DataFrame(
            columns=["怨듦퀬ID", "??쒖텛泥쒕룄", "??쒖젏??, "??쒓낵?쒕챸", "??쒖삁??, "??쒖텛泥쒖씠??, "??쒗궎?뚮뱶"]
        )

    working = opportunity_df.copy()
    if "rfp_score" in working.columns:
        working["rfp_score"] = to_numeric_column(working["rfp_score"])
    else:
        working["rfp_score"] = 0

    recommendation_rank = {
        "異붿쿇": 3,
        "寃?좉텒??: 2,
        "蹂댄넻": 1,
        "鍮꾩텛泥?: 0,
    }
    working["_recommendation_rank"] = (
        working.get("recommendation", pd.Series("", index=working.index))
        .map(recommendation_rank)
        .fillna(-1)
    )
    working["_project_name"] = (
        working.get("llm_project_name", working.get("project_name", pd.Series("", index=working.index)))
        .fillna("")
        .astype(str)
        .str.strip()
    )
    working["_budget_source"] = (
        working.get(
            "llm_total_budget_text",
            working.get(
                "total_budget_text",
                working.get(
                    "llm_per_project_budget_text",
                    working.get("per_project_budget_text", working.get("budget", pd.Series("", index=working.index))),
                ),
            ),
        )
        .fillna("")
        .astype(str)
    )
    working["_budget"] = working["_budget_source"].apply(extract_budget_summary)
    working["_reason"] = (
        working.get("llm_reason", working.get("reason", pd.Series("", index=working.index)))
        .fillna("")
        .astype(str)
        .str.strip()
    )
    working["_keywords"] = (
        working.get("llm_keywords", working.get("keywords", pd.Series("", index=working.index)))
        .fillna("")
        .astype(str)
        .str.strip()
    )
    working["_placeholder_rank"] = build_placeholder_opportunity_mask(working).astype(int)

    working = working.sort_values(
        by=["notice_id", "_placeholder_rank", "rfp_score", "_recommendation_rank", "_project_name"],
        ascending=[True, True, False, False, True],
        na_position="last",
    )
    best = working.drop_duplicates(subset=["notice_id"], keep="first").copy()

    return pd.DataFrame(
        {
            "怨듦퀬ID": best["notice_id"].fillna("").astype(str).str.strip(),
            "??쒖텛泥쒕룄": best.get("llm_recommendation", best.get("recommendation", "")).fillna("").astype(str).str.strip(),
            "??쒖젏??: best.get("llm_fit_score", best["rfp_score"]),
            "??쒓낵?쒕챸": best["_project_name"],
            "??쒖삁??: best["_budget"],
            "??쒖텛泥쒖씠??: best["_reason"],
            "??쒗궎?뚮뱶": best["_keywords"],
        }
    )


def merge_notice_with_analysis(notice_df: pd.DataFrame, opportunity_df: pd.DataFrame) -> pd.DataFrame:
    if notice_df.empty:
        return notice_df

    summary_df = build_notice_analysis_summary(opportunity_df)
    merged = notice_df.copy()

    if summary_df.empty or "怨듦퀬ID" not in merged.columns:
        for column in ["??쒖텛泥쒕룄", "??쒖젏??, "??쒓낵?쒕챸", "??쒖삁??, "??쒖텛泥쒖씠??, "??쒗궎?뚮뱶"]:
            if column not in merged.columns:
                merged[column] = ""
        return merged

    merged["怨듦퀬ID"] = merged["怨듦퀬ID"].fillna("").astype(str).str.strip()
    merged = merged.merge(summary_df, on="怨듦퀬ID", how="left", suffixes=("", "_analysis"))

    for column in ["??쒖텛泥쒕룄", "??쒓낵?쒕챸", "??쒖삁??, "??쒖텛泥쒖씠??, "??쒗궎?뚮뱶"]:
        merged[column] = merged[column].fillna("").astype(str).str.strip()
    merged["??쒖젏??] = to_numeric_column(merged["??쒖젏??])
    return merged


def build_contains_mask(df: pd.DataFrame, columns: list[str], query: str) -> pd.Series:
    if not query:
        return pd.Series(True, index=df.index)

    mask = pd.Series(False, index=df.index)
    for column in columns:
        if column in df.columns:
            mask = mask | df[column].fillna("").str.contains(query, case=False, na=False)
    return mask


def apply_multiselect_filter(df: pd.DataFrame, column: str, label: str, key: str) -> pd.DataFrame:
    if column not in df.columns:
        return df

    values = sorted([x for x in df[column].dropna().unique().tolist() if clean(x)])
    widget_key = unified_sidebar_filter_key(key)
    if widget_key in st.session_state and isinstance(st.session_state[widget_key], list):
        allowed = set(values)
        st.session_state[widget_key] = [
            value for value in st.session_state[widget_key] if value in allowed
        ]
    selected = st.sidebar.multiselect(label, values, key=widget_key)
    if selected:
        return df[df[column].isin(selected)]
    return df


def unified_sidebar_filter_key(key: str) -> str:
    suffix = clean(key).rsplit("_", 1)[-1]
    common_suffixes = {
        "search",
        "agency",
        "ministry",
        "status",
        "review",
        "recommendation",
        "scope",
        "current",
    }
    if suffix in common_suffixes:
        return f"sidebar_{suffix}"
    return key


def render_sidebar_search(key: str = "sidebar_search") -> str:
    st.sidebar.markdown("## Common Filters")
    return st.sidebar.text_input("?듯빀 寃??, "", key=unified_sidebar_filter_key(key))


def render_notice_filter_sidebar(
    key_prefix: str,
    *,
    current_only_default: bool = True,
    status_default: str = "?꾩껜",
    show_current_only: bool = True,
    show_status_scope: bool = True,
) -> tuple[str, bool, str]:
    search_text = render_sidebar_search(f"{key_prefix}_search")

    current_only = current_only_default
    if show_current_only:
        current_only = st.sidebar.checkbox(
            "?꾩옱 怨듦퀬留?,
            value=current_only_default,
            key=unified_sidebar_filter_key(f"{key_prefix}_current"),
        )

    status_scope = status_default
    if show_status_scope:
        status_options = ["?꾩껜", "?묒닔以?, "?덉젙", "留덇컧"]
        default_status = status_default if status_default in status_options else "?꾩껜"
        status_scope = st.sidebar.selectbox(
            "怨듦퀬 ?곹깭",
            status_options,
            index=status_options.index(default_status),
            key=unified_sidebar_filter_key(f"{key_prefix}_scope"),
        )

    return search_text, current_only, status_scope


def render_page_header(title: str, subtitle: str, *, eyebrow: str | None = None) -> None:
    eyebrow_html = ""
    if clean(eyebrow):
        eyebrow_html = f'<div class="page-shell-eyebrow">{escape(clean(eyebrow))}</div>'
    st.markdown(
        (
            '<div class="page-shell-header">'
            '<div class="page-shell-header-row">'
            '<div class="page-shell-header-copy">'
            f"{eyebrow_html}"
            f'<div class="page-shell-title">{escape(clean(title))}</div>'
            f'<div class="page-shell-subtitle">{escape(clean(subtitle))}</div>'
            '</div>'
            '</div>'
            "</div>"
        ),
        unsafe_allow_html=True,
    )


def _inject_viewer_sync_surface_styles() -> None:
    if st.session_state.get("_viewer_sync_surface_styles_injected"):
        return
    st.session_state["_viewer_sync_surface_styles_injected"] = True
    st.markdown(
        """
        <style>
        .workspace-shell {
            border: 1px solid #e2e8f0;
            border-radius: 16px;
            background: rgba(255, 255, 255, 0.96);
            box-shadow: 0 12px 30px rgba(15, 23, 42, 0.04);
            padding: 0.9rem 1.15rem;
        }
        .workspace-title {
            color: #15233b;
            font-size: 1.08rem;
            font-weight: 800;
            letter-spacing: -0.02em;
            line-height: 1.15;
        }
        .workspace-subtitle {
            margin-top: 0.18rem;
            color: #6c7f9d;
            font-size: 0.82rem;
            line-height: 1.45;
            max-width: 34rem;
        }
        .workspace-meta-row {
            display: flex;
            flex-wrap: wrap;
            gap: 0.45rem;
            margin-top: 0.55rem;
        }
        .workspace-meta-pill {
            display: inline-flex;
            align-items: center;
            gap: 0.35rem;
            padding: 0.28rem 0.64rem;
            border-radius: 999px;
            border: 1px solid #dbe6f3;
            background: #f8fbff;
            color: #2563eb;
            font-size: 0.74rem;
            font-weight: 700;
            letter-spacing: 0.01em;
        }
        .workspace-updated {
            margin-top: 0.35rem;
            color: #94a3b8;
            font-size: 0.74rem;
        }
        .workspace-action-spacer {
            min-height: 0.2rem;
        }
        .workspace-toolbar {
            display: flex;
            align-items: center;
            justify-content: flex-end;
            gap: 0.65rem;
            min-height: 100%;
        }
        .workspace-toolbar-note {
            display: inline-flex;
            align-items: center;
            justify-content: center;
            min-width: 36px;
            height: 36px;
            border-radius: 999px;
            border: 1px solid #e2e8f0;
            background: #ffffff;
            color: #6c7f9d;
            font-size: 0.9rem;
            font-weight: 800;
        }
        .workspace-user-chip {
            display: inline-flex;
            align-items: center;
            gap: 0.45rem;
            padding: 0.45rem 0.8rem;
            border-radius: 999px;
            border: 1px solid #e2e8f0;
            background: #ffffff;
            color: #21314d;
            font-size: 0.78rem;
            font-weight: 700;
            white-space: nowrap;
        }
        .workspace-nav-shell {
            margin: 0.8rem auto 0.35rem;
            max-width: 860px;
        }
        .workspace-nav-shell div.stButton > button {
            min-height: 42px !important;
            border-radius: 0 !important;
            border-width: 0 0 2px 0 !important;
            border-color: transparent !important;
            background: transparent !important;
            color: #6c7f9d !important;
            font-size: 0.95rem !important;
            font-weight: 700 !important;
            box-shadow: none !important;
        }
        .workspace-nav-shell div.stButton > button[kind="primary"] {
            color: #2563eb !important;
            border-color: #2563eb !important;
            background: transparent !important;
        }
        .workspace-nav-shell div.stButton > button:hover {
            color: #15233b !important;
            border-color: #cbd5e1 !important;
            background: transparent !important;
        }
        .detail-hero,
        .analysis-hero {
            border: 1px solid rgba(191, 203, 226, 0.7);
            border-radius: 24px;
            background: linear-gradient(180deg, rgba(255,255,255,0.98), rgba(246,249,255,0.96));
            box-shadow: 0 18px 42px rgba(15, 23, 42, 0.08);
            padding: 1.35rem 1.4rem;
        }
        .detail-card {
            border: 1px solid rgba(203, 213, 225, 0.82);
            border-radius: 20px;
            background: rgba(255, 255, 255, 0.98);
            box-shadow: 0 12px 28px rgba(15, 23, 42, 0.05);
            padding: 1rem 1.05rem;
            height: 100%;
        }
        .detail-card-title {
            color: #1d4ed8;
            font-size: 0.84rem;
            font-weight: 800;
            letter-spacing: 0.01em;
            text-transform: none;
        }
        .detail-field {
            margin-top: 0.55rem;
        }
        .detail-label {
            color: #64748b;
            font-size: 0.76rem;
            font-weight: 700;
            letter-spacing: 0.01em;
            margin-bottom: 0.18rem;
        }
        .detail-value {
            color: #0f172a;
            font-size: 0.97rem;
            font-weight: 700;
            line-height: 1.55;
            word-break: keep-all;
        }
        .detail-more-body {
            color: #334155;
            line-height: 1.7;
        }
        .detail-section-title {
            display: flex;
            align-items: center;
            gap: 0.7rem;
            margin: 1.1rem 0 0.7rem;
            color: #0f172a;
            font-size: 1.08rem;
            font-weight: 800;
            letter-spacing: -0.02em;
        }
        .detail-section-title::after {
            content: "";
            flex: 1;
            height: 1px;
            background: linear-gradient(90deg, rgba(148, 163, 184, 0.45), rgba(148, 163, 184, 0));
        }
        .list-table-wrap {
            border: 1px solid rgba(203, 213, 225, 0.78);
            border-radius: 24px;
            background: rgba(255, 255, 255, 0.98);
            box-shadow: 0 20px 40px rgba(15, 23, 42, 0.06);
            padding: 0.4rem 0.5rem 0.65rem;
        }
        .list-table thead th {
            background: #eff6ff;
            color: #1e3a8a;
            font-weight: 800;
            border-bottom: 1px solid rgba(191, 219, 254, 0.9);
        }
        .list-table tbody td {
            border-bottom: 1px solid rgba(226, 232, 240, 0.85);
            vertical-align: top;
        }
        .list-row-link,
        .list-row-link:hover,
        .list-row-link:focus,
        .list-link-out,
        .list-link-out:hover,
        .list-link-out:focus {
            color: inherit !important;
            text-decoration: none !important;
        }
        @media (max-width: 900px) {
            .workspace-shell,
            .detail-hero,
            .analysis-hero,
            .list-table-wrap {
                border-radius: 18px;
                padding: 1rem 1rem 0.95rem;
            }
            .workspace-title {
                font-size: 1.55rem;
            }
            .detail-section-title {
                font-size: 1rem;
                margin-top: 0.9rem;
            }
        }
        </style>
        """,
        unsafe_allow_html=True,
    )


def render_workspace_header(mode_config: AppModeConfig) -> None:
    _inject_viewer_sync_surface_styles()
    user_id = get_current_user_id()
    user_label = get_current_user_label()
    header_cols = st.columns([3.8, 4.2, 2.0])
    with header_cols[0]:
        st.markdown(
            (
                '<div class="workspace-shell">'
                '<div class="workspace-title">R&amp;D Opportunity</div>'
                '</div>'
            ),
            unsafe_allow_html=True,
        )
    with header_cols[1]:
        st.text_input(
            "workspace_top_search",
            key=f"{mode_config.mode}_workspace_top_search",
            placeholder="怨듦퀬紐?/ 怨쇱젣紐?/ 湲곌?紐?寃??,
            label_visibility="collapsed",
        )
    with header_cols[2]:
        st.markdown('<div class="workspace-toolbar">', unsafe_allow_html=True)
        st.markdown('<div class="workspace-toolbar-note">?뵒</div>', unsafe_allow_html=True)
        st.markdown(f'<div class="workspace-user-chip">?뫀 {escape(user_label or user_id or "User")}</div>', unsafe_allow_html=True)
        st.markdown('</div>', unsafe_allow_html=True)


def render_section_label(text: str) -> None:
    st.markdown(
        f'<div class="section-label">{escape(clean(text))}</div>',
        unsafe_allow_html=True,
    )


def render_metrics(items: list[tuple[str, str]]) -> None:
    if not items:
        return
    cards = []
    for label, value in items:
        cards.append(
            (
                '<div class="stat-card">'
                f'<div class="stat-label">{escape(clean(label))}</div>'
                f'<div class="stat-value">{escape(clean(value))}</div>'
                "</div>"
            )
        )
    st.markdown(
        '<div class="stat-grid">{}</div>'.format("".join(cards)),
        unsafe_allow_html=True,
    )


def inject_page_styles() -> None:
    css_path = BASE_DIR.parent / "assets" / "styles.css"
    st.markdown(f"<style>{css_path.read_text(encoding='utf-8')}</style>", unsafe_allow_html=True)
    return
    '''
        [data-testid="stMetric"] {
          background: rgba(15, 23, 42, 0.02);
          border: 1px solid var(--linear-border);
          border-radius: var(--linear-radius-lg);
          box-shadow: var(--linear-shadow);
          padding: 0.9rem 1rem;
        }
        [data-testid="stMetricLabel"] p {
          color: var(--linear-text-muted) !important;
          font-size: 0.78rem !important;
          font-weight: 510 !important;
          letter-spacing: -0.01em;
        }
        [data-testid="stMetricValue"] {
          color: var(--linear-text) !important;
          font-size: 1.6rem !important;
          font-weight: 590 !important;
          letter-spacing: -0.04em !important;
        }
        [data-testid="stMetricDelta"] {
          color: var(--linear-text-secondary) !important;
        }
        div.stButton > button {
          background: rgba(15, 23, 42, 0.03) !important;
          color: var(--linear-text) !important;
          border: 1px solid var(--linear-border) !important;
          border-radius: var(--linear-radius-sm) !important;
          min-height: 38px !important;
          font-weight: 510 !important;
          box-shadow: none !important;
        }
        div.stButton > button:hover {
          background: rgba(15, 23, 42, 0.06) !important;
          border-color: rgba(15, 23, 42, 0.16) !important;
          color: var(--linear-text) !important;
        }
        div.stButton > button[kind="primary"] {
          background: var(--linear-accent-bg) !important;
          border-color: rgba(113, 112, 255, 0.65) !important;
          color: #ffffff !important;
        }
        div.stButton > button[kind="primary"]:hover {
          background: var(--linear-accent-hover) !important;
          border-color: rgba(130, 143, 255, 0.85) !important;
        }
        div[data-baseweb="select"] > div,
        div[data-testid="stTextInputRootElement"] > div,
        div[data-testid="stTextArea"] textarea,
        div[data-testid="stDateInputField"] input,
        div[data-testid="stNumberInput"] input {
          background: #ffffff !important;
          color: var(--linear-text) !important;
          border: 1px solid var(--linear-border) !important;
          border-radius: var(--linear-radius-sm) !important;
        }
        div[data-baseweb="select"] * ,
        div[data-testid="stTextInputRootElement"] input,
        div[data-testid="stTextArea"] textarea,
        div[data-testid="stNumberInput"] input {
          color: var(--linear-text) !important;
        }
        div[data-baseweb="select"]:hover > div,
        div[data-testid="stTextInputRootElement"] > div:hover,
        div[data-testid="stTextArea"] textarea:hover {
          border-color: rgba(15, 23, 42, 0.14) !important;
        }
        div[data-testid="stSelectbox"] label,
        div[data-testid="stTextInput"] label,
        div[data-testid="stTextArea"] label,
        div[data-testid="stMultiSelect"] label,
        div[data-testid="stDateInput"] label,
        div[data-testid="stNumberInput"] label {
          color: var(--linear-text-muted) !important;
          font-size: 0.78rem !important;
          font-weight: 510 !important;
        }
        div[data-testid="stRadio"] > div {
          gap: 0.5rem;
        }
        div[data-testid="stRadio"] label[data-baseweb="radio"],
        div[data-testid="stRadio"] div[role="radiogroup"] label {
          background: rgba(15, 23, 42, 0.02);
          border: 1px solid var(--linear-border-subtle);
          border-radius: var(--linear-radius-md);
          padding: 0.35rem 0.75rem;
          min-height: 36px;
          transition: all 120ms ease;
        }
        div[data-testid="stRadio"] label[data-baseweb="radio"]:hover,
        div[data-testid="stRadio"] div[role="radiogroup"] label:hover {
          background: rgba(15, 23, 42, 0.05);
          border-color: var(--linear-border);
        }
        div[data-testid="stRadio"] p {
          color: var(--linear-text-secondary) !important;
          font-size: 0.92rem !important;
          font-weight: 510 !important;
        }
        div[data-testid="stRadio"] input:checked + div p,
        div[data-testid="stRadio"] label[data-baseweb="radio"][aria-checked="true"] p {
          color: var(--linear-text) !important;
        }
        div[data-testid="stTabs"] {
          gap: 0.5rem;
        }
        div[data-testid="stTabs"] button {
          background: rgba(15, 23, 42, 0.02) !important;
          border: 1px solid var(--linear-border-subtle) !important;
          border-radius: var(--linear-radius-md) !important;
          color: var(--linear-text-secondary) !important;
          font-weight: 510 !important;
        }
        div[data-testid="stTabs"] button[aria-selected="true"] {
          background: rgba(94, 106, 210, 0.2) !important;
          border-color: rgba(113, 112, 255, 0.45) !important;
          color: var(--linear-text) !important;
        }
        [data-testid="stInfo"] {
          background: rgba(15, 23, 42, 0.03);
          border: 1px solid var(--linear-border-subtle);
          color: var(--linear-text-secondary);
        }
        [data-testid="stDataFrame"] > div {
          border: 1px solid var(--linear-border);
          border-radius: var(--linear-radius-lg);
          background: #ffffff;
          overflow: hidden;
        }
        [data-testid="stDataFrameGlideDataEditor"] {
          background: #ffffff !important;
        }
        [data-testid="stDataFrameGlideDataEditor"] * {
          font-family: Inter, "Segoe UI", "Noto Sans KR", sans-serif !important;
        }
        [data-testid="stDataFrameGlideDataEditor"] [role="grid"] {
          background: #ffffff !important;
        }
        [data-testid="stDataFrameGlideDataEditor"] [data-testid="stDataFrameResizable"] {
          background: #ffffff !important;
        }
        [data-testid="stDataFrameGlideDataEditor"] canvas {
          border-radius: var(--linear-radius-lg);
        }
        [data-testid="stVerticalBlockBorderWrapper"] {
          border-radius: var(--linear-radius-lg);
        }
        .page-header-block,
        .page-shell-header {
          margin: 0 0 22px 0;
          padding: 0 0 18px 0;
          border-bottom: 1px solid var(--linear-border-subtle);
        }
        .page-shell-header-row {
          display: flex;
          align-items: flex-start;
          justify-content: space-between;
          gap: 1rem;
        }
        .page-shell-header-copy {
          min-width: 0;
          flex: 1 1 auto;
        }
        .page-eyebrow,
        .page-shell-eyebrow,
        .section-label {
          font-size: 11px;
          font-weight: 510;
          color: var(--linear-text-faint);
          text-transform: uppercase;
          letter-spacing: 0.07em;
          margin-bottom: 8px;
        }
        .page-header-title,
        .page-shell-title {
          color: var(--linear-text);
          font-size: 20px;
          font-weight: 650;
          letter-spacing: -0.03em;
          margin-bottom: 4px;
          line-height: 1.2;
        }
        .page-header-subtitle,
        .page-shell-subtitle {
          color: var(--linear-text-faint);
          font-size: 13px;
          font-weight: 400;
          letter-spacing: -0.01em;
          line-height: 1.55;
        }
        .page-note {
          color: var(--linear-text-muted);
          font-size: 12px;
          font-family: ui-monospace, "SF Mono", Menlo, monospace;
          margin: 10px 0 12px 0;
        }
        .queue-list-shell {
          display: flex;
          flex-direction: column;
          gap: 0.9rem;
        }
        .queue-list-link {
          display: block;
          color: inherit;
          text-decoration: none !important;
        }
        .queue-list-link:visited,
        .queue-list-link:hover,
        .queue-list-link:active,
        .queue-list-link *,
        .queue-list-link:hover * {
          text-decoration: none !important;
        }
        .queue-card {
          background: #ffffff;
          border: 1px solid var(--linear-border);
          border-radius: 20px;
          padding: 1.15rem 1.2rem;
          box-shadow: var(--linear-shadow);
        }
        .queue-list-link:hover .queue-card {
          border-color: rgba(15, 23, 42, 0.18);
          background: #fbfdff;
          transform: translateY(-1px);
        }
        .queue-list-card {
          transition: border-color 120ms ease, transform 120ms ease, background-color 120ms ease;
        }
        .queue-list-card-title {
          color: var(--linear-text);
          font-size: 1.08rem;
          line-height: 1.38;
          font-weight: 700;
          letter-spacing: -0.02em;
          margin-bottom: 0.45rem;
        }
        .queue-list-card-subtitle {
          color: var(--linear-text-muted);
          font-size: 0.92rem;
          line-height: 1.52;
          margin-bottom: 0.85rem;
        }
        .queue-list-card-meta {
          display: grid;
          grid-template-columns: repeat(3, minmax(0, 1fr));
          gap: 0.8rem;
          margin-bottom: 0.85rem;
        }
        .queue-list-card-meta-item {
          background: rgba(15, 23, 42, 0.02);
          border: 1px solid var(--linear-border-subtle);
          border-radius: 14px;
          padding: 0.72rem 0.8rem;
        }
        .queue-list-card-meta-label {
          color: var(--linear-text-muted);
          font-size: 0.78rem;
          font-weight: 600;
          margin-bottom: 0.24rem;
        }
        .queue-list-card-meta-value {
          color: var(--linear-text);
          font-size: 0.94rem;
          font-weight: 650;
          line-height: 1.4;
        }
        .queue-list-card-reason {
          color: var(--linear-text-secondary);
          font-size: 0.94rem;
          line-height: 1.62;
        }
        .queue-badge-row,
        .detail-badge-row {
          display: flex;
          flex-wrap: wrap;
          gap: 0.5rem;
          margin-bottom: 0.95rem;
        }
        .queue-badge,
        .detail-badge {
          display: inline-flex;
          align-items: center;
          padding: 0.4rem 0.75rem;
          border-radius: 999px;
          font-size: 0.84rem;
          font-weight: 650;
          line-height: 1;
        }
        .badge-green { background: rgba(16, 185, 129, 0.12); color: #0f9f6e; }
        .badge-blue { background: rgba(94, 106, 210, 0.12); color: var(--linear-accent-bg); }
        .badge-rose { background: rgba(248, 113, 113, 0.14); color: #d64b4b; }
        .badge-amber { background: rgba(245, 158, 11, 0.16); color: #c27b09; }
        .badge-slate { background: rgba(15, 23, 42, 0.06); color: var(--linear-text-muted); }
        .queue-shell-note {
          color: var(--linear-text-muted);
          font-size: 0.94rem;
          line-height: 1.65;
          margin: -0.1rem 0 1rem 0;
        }
        .queue-filter-label,
        .queue-results-label {
          color: var(--linear-text);
          font-size: 0.98rem;
          font-weight: 650;
          letter-spacing: -0.02em;
          margin-bottom: 0.6rem;
        }
        .queue-filter-help {
          color: var(--linear-text-muted);
          font-size: 0.88rem;
          line-height: 1.55;
          margin: 0.12rem 0 0.85rem 0;
        }
        .stat-grid {
          display: grid;
          grid-template-columns: repeat(auto-fit, minmax(160px, 1fr));
          gap: 10px;
          margin: 0 0 22px 0;
        }
        .stat-card {
          background: #ffffff;
          border: 1px solid var(--linear-border);
          border-radius: var(--linear-radius-md);
          padding: 14px 16px;
          box-shadow: var(--linear-shadow);
        }
        .stat-label {
          color: var(--linear-text-faint);
          font-size: 11px;
          font-weight: 510;
          text-transform: uppercase;
          letter-spacing: 0.07em;
          margin-bottom: 10px;
        }
        .stat-value {
          color: var(--linear-text);
          font-size: 26px;
          line-height: 1;
          font-weight: 510;
          letter-spacing: -0.03em;
        }
        .grant-search-header {
          display: flex;
          align-items: center;
          justify-content: space-between;
          gap: 24px;
          padding: 22px 8px 34px 8px;
          border-bottom: 1px solid #eeeeee;
          margin: -4px 0 46px 0;
        }
        .grant-search-brand-row {
          display: flex;
          align-items: center;
          gap: 28px;
        }
        .grant-search-brand {
          color: #0f2024;
          font-size: 27px;
          font-weight: 900;
          line-height: 1;
          white-space: nowrap;
        }
        .grant-search-divider {
          width: 3px;
          height: 28px;
          background: #d6d6d6;
        }
        .grant-search-nav {
          display: flex;
          align-items: center;
          gap: 34px;
          color: #d6d6d6;
          font-size: 20px;
          font-weight: 900;
        }
        .grant-search-nav .active {
          color: #001eff;
        }
        .grant-search-auth {
          display: flex;
          gap: 32px;
          color: #111827;
          font-size: 17px;
          font-weight: 900;
          white-space: nowrap;
        }
        .grant-search-hero {
          margin: 0 0 54px 0;
        }
        .grant-search-title {
          color: #0f2024;
          font-size: 37px;
          font-weight: 900;
          line-height: 1.22;
          letter-spacing: 0;
          margin-bottom: 16px;
        }
        .grant-search-subtitle {
          color: #9b9b9b;
          font-size: 21px;
          font-weight: 800;
          margin-bottom: 26px;
        }
        .grant-search-shell {
          display: grid;
          grid-template-columns: 1fr 128px;
          gap: 14px;
          align-items: stretch;
          margin-bottom: 50px;
        }
        .grant-search-shell div[data-testid="stTextArea"] textarea {
          min-height: 210px !important;
          border: 5px solid #e0e0ff !important;
          border-radius: 22px !important;
          padding: 24px 28px !important;
          color: #111827 !important;
          font-size: 20px !important;
          font-weight: 700 !important;
          resize: none !important;
        }
        .grant-search-shell div[data-testid="stTextArea"] textarea::placeholder {
          color: #c9c9c9 !important;
        }
        .grant-search-button-wrap div.stButton > button {
          height: 210px !important;
          min-height: 210px !important;
          border-radius: 13px !important;
          border-color: #0716ff !important;
          background: #0716ff !important;
          color: #ffffff !important;
          font-size: 21px !important;
          font-weight: 900 !important;
        }
        .grant-filter-head {
          display: flex;
          justify-content: space-between;
          align-items: center;
          margin: 0 0 22px 0;
        }
        .grant-filter-title {
          color: #111827;
          font-size: 21px;
          font-weight: 900;
        }
        .grant-filter-grid {
          display: grid;
          grid-template-columns: repeat(3, minmax(0, 1fr));
          border: 1px solid #e8e8e8;
          border-radius: 22px;
          overflow: hidden;
          background: #ffffff;
          margin-bottom: 36px;
        }
        .grant-filter-cell {
          min-height: 174px;
          padding: 30px 30px 26px 30px;
          border-right: 1px solid #e8e8e8;
          border-bottom: 1px solid #e8e8e8;
        }
        .grant-filter-cell:nth-child(3n) {
          border-right: 0;
        }
        .grant-filter-cell:nth-last-child(-n+3) {
          border-bottom: 0;
        }
        .grant-filter-label {
          color: #111827;
          font-size: 18px;
          font-weight: 900;
          margin-bottom: 18px;
        }
        .grant-chip-row {
          display: flex;
          flex-wrap: wrap;
          gap: 8px;
        }
        .grant-chip {
          display: inline-flex;
          align-items: center;
          justify-content: center;
          min-height: 35px;
          padding: 0 16px;
          border: 1px solid #e5e7eb;
          border-radius: 6px;
          background: #ffffff;
          color: #9b9b9b;
          font-size: 15px;
          font-weight: 800;
        }
        .grant-chip.active {
          background: #dedfff;
          border-color: #dedfff;
          color: #001eff;
        }
        .grant-filter-input {
          display: grid;
          grid-template-columns: 1fr 56px;
          height: 42px;
          border: 1px solid #e5e7eb;
          border-radius: 6px;
          overflow: hidden;
          color: #b5b5b5;
          font-size: 16px;
          font-weight: 800;
        }
        .grant-filter-placeholder {
          display: flex;
          align-items: center;
          padding-left: 16px;
        }
        .grant-filter-unit {
          display: flex;
          align-items: center;
          justify-content: center;
          border-left: 1px solid #e5e7eb;
          background: #fafafa;
        }
        @media (max-width: 900px) {
          .grant-search-header,
          .grant-search-brand-row,
          .grant-search-auth {
            align-items: flex-start;
            flex-direction: column;
          }
          .grant-search-shell,
          .grant-filter-grid {
            grid-template-columns: 1fr;
          }
          .grant-filter-cell,
          .grant-filter-cell:nth-child(3n),
          .grant-filter-cell:nth-last-child(-n+3) {
            border-right: 0;
            border-bottom: 1px solid #e8e8e8;
          }
          .grant-filter-cell:last-child {
            border-bottom: 0;
          }
        }
        .public-notice-card {
          margin: 10px 0 26px 0;
          padding: 54px 54px 42px 54px;
          border: 1px solid rgba(15, 23, 42, 0.12);
          border-radius: 34px;
          background: #ffffff;
          box-shadow: 0 0 0 1px rgba(15, 23, 42, 0.02);
        }
        .public-notice-top {
          display: flex;
          justify-content: space-between;
          align-items: flex-start;
          gap: 24px;
        }
        .public-notice-badges {
          display: flex;
          gap: 6px;
          margin-bottom: 20px;
        }
        .public-badge {
          display: inline-flex;
          align-items: center;
          justify-content: center;
          min-height: 34px;
          padding: 0 13px;
          border-radius: 3px;
          color: #ffffff;
          font-size: 18px;
          font-weight: 800;
          line-height: 1;
        }
        .public-badge.new {
          background: #12c91d;
        }
        .public-badge.dday {
          background: #ff9f0a;
        }
        .public-save-button {
          display: none;
          align-items: center;
          gap: 8px;
          height: 48px;
          padding: 0 18px;
          border: 1px solid #e5e7eb;
          border-radius: 6px;
          background: #fafafa;
          color: #111827;
          font-size: 19px;
          font-weight: 800;
          white-space: nowrap;
          box-shadow: 0 0 0 1px rgba(15, 23, 42, 0.02);
        }
        .public-save-icon {
          font-size: 22px;
          line-height: 1;
        }
        .public-notice-title {
          max-width: 980px;
          color: #172327;
          font-size: 23px;
          font-weight: 800;
          line-height: 1.38;
          margin-bottom: 11px;
          letter-spacing: 0;
        }
        .public-notice-subtitle {
          color: #9ca3af;
          font-size: 17px;
          font-weight: 700;
          line-height: 1.5;
          letter-spacing: 0;
        }
        .public-notice-divider {
          height: 1px;
          background: #e5e7eb;
          margin: 38px 0 31px 0;
        }
        .public-notice-body {
          display: grid;
          grid-template-columns: repeat(auto-fit, minmax(260px, 1fr));
          gap: 40px;
          align-items: stretch;
        }
        .public-info-panel {
          background: #fafafa;
          border-radius: 6px;
          padding: clamp(18px, 2vw, 31px) clamp(18px, 2.5vw, 36px);
          min-height: 0;
        }
        .public-info-row {
          display: grid;
          grid-template-columns: 140px 1fr;
          gap: 20px;
          align-items: start;
          padding: 10px 0;
        }
        .public-info-label {
          color: #9ca3af;
          font-size: 16px;
          font-weight: 800;
          line-height: 1.5;
        }
        .public-info-value {
          color: #3f3f46;
          font-size: 20px;
          font-weight: 800;
          line-height: 1.5;
          word-break: keep-all;
        }
        .public-info-value.budget {
          color: #001aff;
        }
        .public-budget-highlight {
          display: inline-block;
          padding: 0 7px 1px 7px;
          background: #dfe0ff;
          color: #001aff;
        }
        .public-fit-head {
          display: grid;
          grid-template-columns: 132px 1fr;
          gap: 18px;
          align-items: center;
          margin-bottom: 18px;
        }
        .public-fit-label {
          display: none;
          color: #ff6666;
          font-size: 16px;
          font-weight: 800;
        }
        .public-fit-head,
        .public-fit-grid,
        .public-fit-box {
          display: none;
        }
        .public-fit-bar {
          height: 19px;
          border: 2px solid #ffc6c6;
          border-radius: 999px;
          background: #fff1f1;
          position: relative;
          overflow: hidden;
        }
        .public-fit-bar-fill {
          height: 100%;
          min-width: 18px;
          border-radius: 999px;
          background: #ff6666;
        }
        .public-fit-grid {
          display: grid;
          grid-template-columns: repeat(2, minmax(0, 1fr));
          gap: 18px 20px;
        }
        .public-fit-box {
          min-height: 111px;
          padding: 26px 28px;
          border-radius: 6px;
          background: #fff0f0;
        }
        .public-fit-box-label {
          color: #ffa2a2;
          font-size: 16px;
          font-weight: 800;
          margin-bottom: 10px;
        }
        .public-fit-box-value {
          color: #ff5d5d;
          font-size: 20px;
          font-weight: 900;
          line-height: 1.55;
          word-break: keep-all;
        }
        .public-notice-footer {
          display: flex;
          justify-content: space-between;
          align-items: flex-end;
          gap: 24px;
          margin-top: 36px;
        }
        .public-tag-row {
          display: flex;
          flex-wrap: wrap;
          gap: 8px;
        }
        .public-tag {
          display: inline-flex;
          align-items: center;
          min-height: 40px;
          padding: 0 10px;
          border-radius: 3px;
          background: #dedfff;
          color: #0532ff;
          font-size: 17px;
          font-weight: 900;
          text-decoration: underline;
          text-underline-offset: 3px;
        }
        .public-alert-link {
          display: none;
          color: #001eff;
          font-size: 18px;
          font-weight: 900;
          text-decoration: underline;
          text-underline-offset: 4px;
          white-space: nowrap;
        }
        .rnd-detail-stack {
          max-width: 1080px;
          margin: 0 auto 30px auto;
        }
        .rnd-section {
          padding: 30px 0;
          border-top: 1px solid #e5e7eb;
        }
        .rnd-section:first-child {
          border-top: 0;
          padding-top: 0;
        }
        .rnd-section-title {
          color: #111827;
          font-size: 24px;
          font-weight: 900;
          line-height: 1.35;
          margin: 0 0 18px 0;
          letter-spacing: 0;
        }
        .rnd-info-grid {
          display: grid;
          grid-template-columns: repeat(2, minmax(0, 1fr));
          gap: 16px 18px;
        }
        .rnd-info-item {
          min-height: 88px;
          padding: 20px 22px;
          border: 1px solid #edf0f2;
          border-radius: 8px;
          background: #ffffff;
        }
        .rnd-info-label {
          color: #8b949e;
          font-size: 14px;
          font-weight: 800;
          margin-bottom: 8px;
        }
        .rnd-info-value {
          color: #111827;
          font-size: 18px;
          font-weight: 800;
          line-height: 1.55;
          word-break: keep-all;
        }
        .rnd-section-body {
          color: #1f2937;
          font-size: 17px;
          font-weight: 500;
          line-height: 1.9;
          white-space: pre-wrap;
          word-break: keep-all;
        }
        .rnd-requirement-list {
          display: grid;
          grid-template-columns: repeat(2, minmax(0, 1fr));
          gap: 14px 18px;
        }
        .rnd-requirement-item {
          padding: 20px 22px;
          border-radius: 8px;
          background: #fff4f4;
        }
        .rnd-requirement-label {
          color: #ff8f8f;
          font-size: 14px;
          font-weight: 900;
          margin-bottom: 8px;
        }
        .rnd-requirement-value {
          color: #f05f5f;
          font-size: 18px;
          font-weight: 900;
          line-height: 1.6;
        }
        @media (max-width: 900px) {
          .rnd-info-grid,
          .rnd-requirement-list {
            grid-template-columns: 1fr;
          }
        }
        @media (max-width: 900px) {
          .public-notice-card {
            padding: 28px 22px;
            border-radius: 22px;
          }
          .public-notice-body,
          .public-fit-grid {
            grid-template-columns: 1fr;
          }
          .public-notice-footer,
          .public-notice-top {
            flex-direction: column;
            align-items: stretch;
          }
          .public-info-row {
            grid-template-columns: 104px 1fr;
          }
          .public-fit-head {
            grid-template-columns: 1fr;
          }
        }
        .detail-hero {
          padding: 28px 30px;
          border: 1px solid rgba(113, 112, 255, 0.22);
          border-radius: 22px;
          background:
            radial-gradient(circle at top right, rgba(113, 112, 255, 0.12), transparent 28%),
            linear-gradient(180deg, #ffffff 0%, #f8fbff 100%);
          box-shadow: var(--linear-shadow);
          margin: 8px 0 24px 0;
        }
        .detail-kicker {
          font-size: 12px;
          font-weight: 700;
          letter-spacing: 0.12em;
          text-transform: uppercase;
          color: #6d79ff;
          margin-bottom: 10px;
        }
        .detail-title {
          font-size: 40px;
          font-weight: 800;
          line-height: 1.16;
          letter-spacing: -0.05em;
          color: var(--linear-text);
          margin-bottom: 16px;
          max-width: 100%;
        }
        .detail-meta-row {
          display: flex;
          flex-wrap: wrap;
          gap: 8px;
        }
        .detail-chip {
          display: inline-flex;
          align-items: center;
          padding: 6px 10px;
          border-radius: 999px;
          font-size: 12px;
          font-weight: 510;
          background: rgba(94, 106, 210, 0.18);
          border: 1px solid rgba(113, 112, 255, 0.35);
          color: var(--linear-text);
        }
        .detail-chip.neutral {
          background: rgba(255, 255, 255, 0.03);
          border-color: var(--linear-border-subtle);
          color: var(--linear-text-secondary);
        }
        .detail-card {
          border: 1px solid rgba(15, 23, 42, 0.10);
          border-radius: 20px;
          padding: 18px 20px;
          background: linear-gradient(180deg, #ffffff 0%, #fbfcff 100%);
          height: 100%;
          box-shadow: var(--linear-shadow);
        }
        .detail-card-title {
          font-size: 18px;
          font-weight: 800;
          color: var(--linear-text);
          letter-spacing: -0.02em;
          margin-bottom: 14px;
          padding-bottom: 10px;
          border-bottom: 1px solid rgba(15, 23, 42, 0.08);
        }
        .detail-field {
          padding: 12px 14px;
          border: 1px solid rgba(15, 23, 42, 0.06);
          border-radius: 14px;
          background: #f8fafc;
          margin-bottom: 10px;
        }
        .detail-field:last-child {
          margin-bottom: 0;
        }
        .detail-label {
          font-size: 13px;
          font-weight: 800;
          letter-spacing: -0.01em;
          color: #64748b;
          margin-bottom: 6px;
        }
        .detail-value {
          font-size: 17px;
          font-weight: 700;
          line-height: 1.65;
          color: #111827;
          word-break: break-word;
          font-family: Inter, "Apple SD Gothic Neo", "Noto Sans KR", "Segoe UI", sans-serif;
          white-space: pre-wrap;
        }
        .detail-more {
          margin-top: 2px;
        }
        .detail-more summary {
          cursor: pointer;
          font-size: 15px;
          line-height: 1.6;
          color: #111827;
          list-style: none;
        }
        .detail-more summary .detail-preview-text {
          color: #111827;
          font-weight: 700;
        }
        .detail-more summary .detail-toggle-text {
          margin-left: 6px;
          font-size: 13px;
          font-weight: 800;
          color: var(--linear-accent);
        }
        .detail-more summary::-webkit-details-marker {
          display: none;
        }
        .detail-more[open] summary {
          display: none;
        }
        .detail-more-body {
          margin-top: 8px;
          font-size: 17px;
          font-weight: 600;
          line-height: 1.7;
          color: #1f2937;
          white-space: pre-wrap;
          word-break: break-word;
          font-family: Inter, "Apple SD Gothic Neo", "Noto Sans KR", "Segoe UI", sans-serif;
        }
        .detail-section-title {
          display: flex;
          align-items: center;
          gap: 10px;
          font-size: 24px;
          font-weight: 900;
          color: var(--linear-text);
          letter-spacing: -0.03em;
          margin: 30px 0 14px 0;
          padding: 0 0 0 12px;
          border-left: 4px solid #6d79ff;
        }
        .detail-section-title::after {
          content: "";
          flex: 1;
          height: 1px;
          background: linear-gradient(90deg, rgba(113, 112, 255, 0.20), rgba(15, 23, 42, 0.04));
        }
        .list-table-wrap {
          width: 100%;
          overflow-x: auto;
          border: 1px solid var(--linear-border);
          border-radius: var(--linear-radius-lg);
          background: #ffffff;
          box-shadow: var(--linear-shadow);
        }
        .list-table {
          width: 100%;
          min-width: 100%;
          border-collapse: collapse;
          table-layout: fixed;
        }
        .list-table thead th {
          background: #f8fafc;
          color: var(--linear-text-muted);
          font-size: 13px;
          font-weight: 510;
          text-align: left;
          padding: 12px 10px;
          border-bottom: 1px solid var(--linear-border-subtle);
          white-space: normal;
          word-break: keep-all;
        }
        .list-table tbody td {
          padding: 9px 10px;
          border-bottom: 1px solid rgba(15, 23, 42, 0.05);
          vertical-align: middle;
          height: 48px;
          color: var(--linear-text-secondary);
          white-space: normal;
          word-break: break-word;
        }
        .list-table thead th,
        .list-table tbody td {
          overflow: hidden;
          text-overflow: ellipsis;
        }
        .list-table tbody tr:hover {
          background: rgba(15, 23, 42, 0.03);
        }
        .list-table tbody td a {
          color: var(--linear-text);
          text-decoration: none;
        }
        .list-table tbody td a:hover {
          color: var(--linear-accent);
        }
        .list-link-cell,
        .list-title-cell,
        .list-link-cell {
          text-align: center;
        }
        .list-title-cell {
          text-align: left;
        }
        .list-row-link,
        .list-link-out {
          display: inline-flex;
          align-items: center;
          max-width: 100%;
          color: var(--linear-text) !important;
          font-weight: 510;
          text-decoration: none !important;
        }
        .list-row-link:hover {
          color: var(--linear-accent-hover) !important;
        }
        .list-link-out {
          justify-content: center;
          min-width: 84px;
          min-height: 34px;
          padding: 0 12px;
          border: 1px solid var(--linear-border);
          border-radius: var(--linear-radius-sm);
          background: #ffffff;
          font-size: 13px;
          font-weight: 510;
          white-space: nowrap;
        }
        .list-link-out:hover {
          border-color: rgba(113, 112, 255, 0.38);
          background: rgba(94, 106, 210, 0.16);
        }
        .list-cell-text,
        .list-cell-empty {
          display: block;
          color: var(--linear-text-secondary);
          white-space: normal;
          overflow: hidden;
          text-overflow: ellipsis;
        }
        .list-title-cell .list-cell-text,
        .list-title-cell .list-row-link {
          white-space: normal;
          line-height: 1.45;
          word-break: keep-all;
        }
        .list-cell-empty {
          color: var(--linear-text-faint);
          text-align: center;
        }
        .faux-tabs-wrap {
          display: flex;
          gap: 8px;
          margin: 8px 0 14px 0;
          border-bottom: 1px solid var(--linear-border-subtle);
          padding-bottom: 8px;
          flex-wrap: wrap;
        }
        .faux-tab {
          display: inline-flex;
          align-items: center;
          justify-content: center;
          padding: 10px 14px;
          border: 1px solid var(--linear-border-subtle);
          border-radius: var(--linear-radius-md);
          background: #ffffff;
          color: var(--linear-text-secondary) !important;
          text-decoration: none !important;
          font-weight: 510;
          min-width: 112px;
        }
        .faux-tab:hover {
          background: rgba(15, 23, 42, 0.04);
          color: var(--linear-text) !important;
        }
        .faux-tab-active {
          background: rgba(94, 106, 210, 0.18);
          border-color: rgba(113, 112, 255, 0.42);
          color: var(--linear-text) !important;
        }
        .dashboard-kpi-grid {
          display: grid;
          grid-template-columns: repeat(auto-fit, minmax(150px, 1fr));
          gap: 12px;
          margin: 8px 0 18px 0;
        }
        .dashboard-kpi-card {
          border: 1px solid var(--linear-border);
          border-radius: var(--linear-radius-lg);
          background: linear-gradient(180deg, #ffffff 0%, #fbfbfd 100%);
          padding: 12px 14px;
          min-height: 78px;
          box-shadow: var(--linear-shadow);
        }
        .dashboard-kpi-label {
          color: var(--linear-text-muted);
          font-size: 12px;
          font-weight: 510;
          margin-bottom: 10px;
        }
        .dashboard-kpi-value {
          color: var(--linear-text);
          font-size: 24px;
          line-height: 1;
          font-weight: 590;
          letter-spacing: -0.04em;
          margin-bottom: 6px;
        }
        .dashboard-kpi-caption {
          color: var(--linear-text-faint);
          font-size: 12px;
          font-weight: 400;
        }
        .dashboard-rank-list {
          display: flex;
          flex-direction: column;
          gap: 10px;
          margin: 8px 0 4px 0;
        }
        .dashboard-rank-row {
          display: grid;
          grid-template-columns: 28px minmax(0, 1fr) auto;
          gap: 12px;
          align-items: center;
          border: 1px solid var(--linear-border);
          border-radius: var(--linear-radius-lg);
          padding: 12px 14px;
          background: #ffffff;
          box-shadow: var(--linear-shadow);
        }
        .dashboard-rank-order {
          color: var(--linear-accent);
          font-size: 18px;
          font-weight: 590;
          text-align: center;
        }
        .dashboard-rank-main {
          min-width: 0;
        }
        .dashboard-rank-head {
          display: flex;
          align-items: center;
          gap: 8px;
          min-width: 0;
          margin-bottom: 5px;
        }
        .dashboard-rank-title,
        .dashboard-rank-title-link {
          color: var(--linear-text);
          font-size: 14px;
          font-weight: 510;
          min-width: 0;
          overflow: hidden;
          text-overflow: ellipsis;
          white-space: nowrap;
          text-decoration: none;
        }
        .dashboard-rank-title-link:hover {
          color: var(--linear-accent-hover);
        }
        .dashboard-rank-badges {
          display: flex;
          flex-wrap: wrap;
          gap: 6px;
        }
        .dashboard-rank-badge {
          display: inline-flex;
          align-items: center;
          padding: 4px 8px;
          border-radius: 999px;
          background: rgba(15, 23, 42, 0.04);
          border: 1px solid var(--linear-border-subtle);
          color: var(--linear-text-secondary);
          font-size: 11px;
          font-weight: 510;
          white-space: nowrap;
        }
        .dashboard-rank-meta {
          display: flex;
          gap: 10px;
          flex-wrap: wrap;
          color: var(--linear-text-muted);
          font-size: 12px;
          font-weight: 400;
        }
        .dashboard-rank-value {
          color: var(--linear-accent-hover);
          font-size: 18px;
          font-weight: 590;
          white-space: nowrap;
          padding-left: 8px;
        }
        @media (max-width: 1200px) {
          [data-testid="stHorizontalBlock"] {
            flex-wrap: wrap;
          }
          [data-testid="stHorizontalBlock"] > [data-testid="column"] {
            min-width: 0 !important;
          }
        }
        @media (max-width: 980px) {
          [data-testid="stHorizontalBlock"] > [data-testid="column"] {
            flex: 1 1 100% !important;
            width: 100% !important;
          }
          .main .block-container {
            max-width: calc(100vw - 1rem);
            padding-left: 0.7rem;
            padding-right: 0.7rem;
          }
          .list-table-wrap {
            overflow-x: visible;
          }
          .list-table {
            min-width: 0;
          }
        }
        @media (max-width: 900px) {
          h1 {
            font-size: 2.1rem !important;
          }
          .detail-title {
            font-size: 28px;
          }
          .dashboard-kpi-grid {
            grid-template-columns: repeat(2, minmax(0, 1fr));
          }
        }
        @media (max-width: 640px) {
          .page-header-title,
          .page-shell-title {
            font-size: 18px;
          }
          .page-header-subtitle,
          .page-shell-subtitle,
          .page-note {
            font-size: 12px;
          }
          .queue-list-card-meta {
            grid-template-columns: 1fr;
          }
          .public-notice-card,
          .detail-card,
          .dashboard-rank-row {
            border-radius: 16px;
          }
          .list-table thead th,
          .list-table tbody td {
            padding: 8px 7px;
            font-size: 12px;
          }
          .faux-tab {
            min-width: 0;
            flex: 1 1 calc(50% - 8px);
          }
        }
        </style>
        """,
        unsafe_allow_html=True,
    )
    '''


def render_detail_header(title: str, kicker: str, chips: list[tuple[str, str]]) -> None:
    chip_html = []
    for text, kind in chips:
        safe_text = sanitize_display_text("chip", text)
        if not clean(safe_text):
            continue
        css_class = "detail-chip" if kind == "accent" else "detail-chip neutral"
        chip_html.append(f'<span class="{css_class}">{escape(safe_text)}</span>')

    st.markdown(
        f"""
        <div class="detail-hero">
          <div class="detail-kicker">{escape(kicker)}</div>
          <div class="detail-title">{escape(sanitize_display_title(title))}</div>
          <div class="detail-meta-row">{''.join(chip_html)}</div>
        </div>
        """,
        unsafe_allow_html=True,
    )


def extract_period_start(value: object) -> pd.Timestamp:
    text = clean(value)
    if not text:
        return pd.NaT
    start_text = re.split(r"\s*[~??]\s*", text, maxsplit=1)[0].strip()
    return pd.to_datetime(start_text.replace(".", "-"), errors="coerce")


def build_public_d_day(period_value: object) -> str:
    period_end = extract_period_end(period_value)
    if pd.isna(period_end):
        return ""
    days = int((period_end.normalize() - pd.Timestamp.now().normalize()).days)
    if days < 0:
        return "留덇컧"
    if days == 0:
        return "D-DAY"
    return f"D-{days}"


def is_recent_notice_date(value: object, *, days: int = 14) -> bool:
    notice_date = pd.to_datetime(clean(value).replace(".", "-"), errors="coerce")
    if pd.isna(notice_date):
        return False
    age_days = int((pd.Timestamp.now().normalize() - notice_date.normalize()).days)
    return 0 <= age_days <= days


def split_public_tags(value: object, *, limit: int = 3) -> list[str]:
    text = clean(value)
    if not text:
        return []
    parts = re.split(r"[,/|#\n]+", text)
    tags = []
    for part in parts:
        tag = clean(part)
        if tag and tag not in tags:
            tags.append(tag)
        if len(tags) >= limit:
            break
    return tags


def public_first_non_empty(row: dict, *keys: str) -> str:
    for key in keys:
        value = row.get(key)
        if isinstance(value, list):
            value = ", ".join(clean(item) for item in value if clean(item))
        value = clean(value)
        if value:
            return value
    return ""


def render_public_notice_card(row: dict, *, top_related: dict | None = None, kind: str = "notice") -> None:
    top_related = top_related or {}
    merged = {**top_related, **(row or {})}
    period = public_first_non_empty(merged, "?묒닔湲곌컙", "notice_period", "period", "?좎껌湲곌컙")
    budget = extract_budget_summary(
        public_first_non_empty(
            merged,
            "??쒖삁??,
            "?ъ뾽鍮?,
            "?덉궛",
            "llm_total_budget_text",
            "total_budget_text",
            "budget",
            "llm_per_project_budget_text",
            "per_project_budget_text",
        )
    )
    title = public_first_non_empty(
        merged,
        "llm_project_name" if kind == "opportunity" else "怨듦퀬紐?,
        "project_name",
        "怨듦퀬紐?,
        "notice_title",
        "llm_rfp_title",
        "rfp_title",
    )
    notice_title = public_first_non_empty(merged, "怨듦퀬紐?, "notice_title")
    subtitle_parts = [
        public_first_non_empty(merged, "怨듦퀬?곹깭", "rcve_status"),
        public_first_non_empty(merged, "?ъ뾽紐?, "notice_title") if kind != "opportunity" else notice_title,
    ]
    subtitle = " | ".join(part for part in subtitle_parts if part and part != title)
    notice_date = public_first_non_empty(merged, "怨듦퀬?쇱옄", "ancm_de", "registered_at")
    ministry = public_first_non_empty(merged, "?뚭?遺泥?, "ministry", "二쇨?遺泥?)
    agency = public_first_non_empty(merged, "?꾨Ц湲곌?紐?, "?꾨Ц湲곌?", "agency", "?대떦遺??)
    org_type = public_first_non_empty(
        merged,
        "吏??媛??湲곌? ?좏삎",
        "吏?먭??κ린愿?좏삎",
        "eligible_org_type",
        "llm_eligible_org_type",
        "applicant_type",
    )
    region = public_first_non_empty(
        merged,
        "吏??媛??뚯옱吏",
        "吏?먭??μ냼?ъ?",
        "eligible_region",
        "llm_eligible_region",
        "region",
    )
    sales = public_first_non_empty(
        merged,
        "吏??媛??留ㅼ텧??/ ?ъ뾽?곗닔",
        "留ㅼ텧??,
        "?ъ뾽?곗닔",
        "eligible_sales",
        "llm_eligible_sales",
    )
    lab = public_first_non_empty(
        merged,
        "遺??곌뎄??꾩슂 ?좊Т",
        "遺?ㅼ뿰援ъ냼",
        "lab_required",
        "llm_lab_required",
    )
    requirement_values = [org_type, region, sales, lab]
    requirement_count = sum(1 for value in requirement_values if value and value not in {"-", "-/-"})
    score = clean(public_first_non_empty(merged, "llm_fit_score", "rfp_score", "??쒖젏??))
    if score:
        try:
            requirement_count = max(requirement_count, min(4, round(float(score) / 25)))
        except Exception:
            pass
    display_requirement_count = max(0, min(4, requirement_count))
    progress = max(1, display_requirement_count) * 25
    d_day = build_public_d_day(period)
    tags = split_public_tags(public_first_non_empty(merged, "??쒗궎?뚮뱶", "llm_keywords", "keywords", "keyword"), limit=3)

    info_rows = [
        ("?좎껌 湲곌컙", period),
        ("吏?먭툑", budget),
        ("遺泥?, ministry),
        ("?꾨Ц湲곌?紐?, agency),
        ("怨듦퀬?깅줉??, notice_date),
    ]
    fit_rows = [
        ("吏??媛??湲곌? ?좏삎", org_type or "-"),
        ("吏??媛??뚯옱吏", region or "?꾧뎅"),
        ("吏??媛??留ㅼ텧??/ ?ъ뾽?곗닔", sales or "-/-"),
        ("遺??곌뎄??꾩슂 ?좊Т", lab or "-"),
    ]
    info_html = []
    for label, value in info_rows:
        if label == "吏?먭툑" and value:
            value_html = f'<span class="public-budget-highlight">{escape(value)}</span>'
            value_class = "public-info-value budget"
        else:
            value_html = escape(value or "-")
            value_class = "public-info-value"
        info_html.append(
            f'<div class="public-info-row"><div class="public-info-label">{escape(label)}</div><div class="{value_class}">{value_html}</div></div>'
        )
    tag_html = "".join(f'<span class="public-tag">{escape(tag)}</span>' for tag in tags)
    new_badge = '<span class="public-badge new">NEW</span>' if is_recent_notice_date(notice_date) else ""
    dday_badge = f'<span class="public-badge dday">{escape(d_day)}</span>' if d_day else ""

    st.markdown(
        f"""
        <div class="public-notice-card">
          <div class="public-notice-top">
            <div>
              <div class="public-notice-badges">{new_badge}{dday_badge}</div>
              <div class="public-notice-title">{escape(sanitize_display_title(title))}</div>
              <div class="public-notice-subtitle">{escape(subtitle)}</div>
            </div>
            <div class="public-save-button"><span class="public-save-icon">??/span><span>??ν븯湲?/span></div>
          </div>
          <div class="public-notice-divider"></div>
          <div class="public-notice-body">
            <div class="public-info-panel">{''.join(info_html)}</div>
          </div>
          <div class="public-notice-footer">
            <div class="public-tag-row">{tag_html}</div>
          </div>
        </div>
        """,
        unsafe_allow_html=True,
    )


def render_rndcircle_like_sections(row: dict, *, top_related: dict | None = None, kind: str = "notice") -> None:
    top_related = top_related or {}
    merged = {**top_related, **(row or {})}
    period = public_first_non_empty(merged, "?묒닔湲곌컙", "notice_period", "period", "?좎껌湲곌컙")
    d_day = build_public_d_day(period)
    support_type = public_first_non_empty(merged, "吏??좏삎", "怨듬え?좏삎", "pbofr_type", "project_type") or "?곌뎄媛쒕컻"
    keywords = split_public_tags(public_first_non_empty(merged, "??쒗궎?뚮뱶", "llm_keywords", "keywords", "keyword"), limit=8)
    org_type = public_first_non_empty(merged, "吏??媛??湲곌? ?좏삎", "吏?먭??κ린愿?좏삎", "eligible_org_type", "llm_eligible_org_type", "applicant_type")
    region = public_first_non_empty(merged, "吏??媛??뚯옱吏", "吏?먭??μ냼?ъ?", "eligible_region", "llm_eligible_region", "region") or "?꾧뎅"
    sales = public_first_non_empty(merged, "吏??媛??留ㅼ텧??/ ?ъ뾽?곗닔", "留ㅼ텧??, "?ъ뾽?곗닔", "eligible_sales", "llm_eligible_sales") or "-/-"
    lab = public_first_non_empty(merged, "遺??곌뎄??꾩슂 ?좊Т", "遺?ㅼ뿰援ъ냼", "lab_required", "llm_lab_required") or "-"
    total_budget = extract_budget_summary(public_first_non_empty(merged, "?ъ뾽 洹쒕え", "?ъ뾽鍮?, "??쒖삁??, "llm_total_budget_text", "total_budget_text", "budget"))
    grant = extract_budget_summary(public_first_non_empty(merged, "吏?먭툑", "怨쇱젣蹂??덉궛", "llm_per_project_budget_text", "per_project_budget_text")) or total_budget
    deadline = extract_period_end(period)
    deadline_text = deadline.strftime("%Y-%m-%d") if pd.notna(deadline) else ""
    summary = public_first_non_empty(
        merged,
        "怨쇱젣 遺꾩꽍",
        "llm_summary",
        "summary",
        "??쒖텛泥쒖씠??,
        "llm_reason",
        "reason",
        "text_preview",
    )
    summary = build_project_analysis_text(merged) if clean(summary) else summary
    overview = public_first_non_empty(
        merged,
        "?ъ뾽 媛쒖슂 諛?諛곌꼍",
        "怨쇱젣 媛쒖슂",
        "llm_concept_and_development",
        "concept_and_development",
        "吏?먰븘?붿꽦(怨쇱젣 諛곌꼍)",
        "support_necessity",
        "technical_background",
    )
    objective = public_first_non_empty(
        merged,
        "怨쇱젣 紐⑺몴",
        "llm_application_field",
        "application_field",
        "?쒖슜遺꾩빞",
    )
    detail = public_first_non_empty(
        merged,
        "怨쇱젣 ?댁슜",
        "吏??댁슜",
        "llm_support_plan",
        "support_plan",
        "吏?먭린媛?諛??덉궛쨌異붿쭊泥닿퀎",
        "?띿뒪??誘몃━蹂닿린",
        "text_preview",
    )
    requirement_history = public_first_non_empty(
        merged,
        "怨쇱젣 ?섑뻾 ?대젰 ?붽굔",
        "湲고? 吏??議곌굔",
        "other_requirements",
        "llm_other_requirements",
    )
    contribution = public_first_non_empty(
        merged,
        "湲곌? 遺꾨떞瑜?,
        "matching_fund",
        "llm_matching_fund",
    )
    extra_detail = public_first_non_empty(
        merged,
        "湲고? ?몃? ?ы빆",
        "湲고? 吏??議곌굔",
        "llm_requirements",
        "requirements",
    )

    info_items = [
        ("吏??좏삎", support_type),
        ("?듭떖 ?ㅼ썙??, " ".join(keywords)),
        ("?ъ뾽 洹쒕え", total_budget),
        ("吏?먭툑", grant),
        ("吏??媛??湲곌?", org_type),
        ("怨듦퀬 ?깅줉??, public_first_non_empty(merged, "怨듦퀬?쇱옄", "ancm_de", "registered_at")),
        ("怨듦퀬 留덇컧??, deadline_text),
        ("?좎껌 湲곌컙", f"{d_day}\n{period}" if d_day else period),
    ]
    requirements = [
        ("吏??媛??湲곌? ?좏삎", org_type or "-"),
        ("吏??媛??뚯옱吏", region),
        ("吏??媛??留ㅼ텧??/ ?ъ뾽?곗닔", sales),
        ("遺??곌뎄??꾩슂 ?좊Т", lab),
    ]
    detail_items = [
        ("怨듬え ?좏삎", public_first_non_empty(merged, "怨듬え?좏삎", "pbofr_type")),
        ("怨쇱젣 湲곌컙", public_first_non_empty(merged, "怨쇱젣 湲곌컙", "project_period", "support_period")),
        ("?ъ뾽 洹쒕え", total_budget),
        ("吏?먭툑", grant),
        ("吏??댁슜", detail),
        ("湲곌? 遺꾨떞瑜?, contribution),
        ("湲고? ?몃? ?ы빆", extra_detail),
    ]

    def info_grid(items: list[tuple[str, str]]) -> str:
        return "".join(
            f'<div class="rnd-info-item"><div class="rnd-info-label">{escape(label)}</div><div class="rnd-info-value">{escape(value or "-")}</div></div>'
            for label, value in items
            if clean(value)
        )

    requirement_html = "".join(
        f'<div class="rnd-requirement-item"><div class="rnd-requirement-label">{escape(label)}</div><div class="rnd-requirement-value">{escape(value or "-")}</div></div>'
        for label, value in requirements
    )
    sections = [
        f'<div class="rnd-section"><div class="rnd-section-title">二쇱슂 ?뺣낫</div><div class="rnd-info-grid">{info_grid(info_items)}</div></div>',
    ]
    if summary:
        sections.append(f'<div class="rnd-section"><div class="rnd-section-title">怨쇱젣 遺꾩꽍</div><div class="rnd-section-body">{escape(summary)}</div></div>')
    sections.append(f'<div class="rnd-section"><div class="rnd-section-title">?붽굔 異⑹”??/div><div class="rnd-requirement-list">{requirement_html}</div></div>')
    support_requirements = [("湲곗뾽遺?ㅼ뿰援ъ냼 ?붽굔", lab), ("怨쇱젣 ?섑뻾 ?대젰 ?붽굔", requirement_history)]
    sections.append(f'<div class="rnd-section"><div class="rnd-section-title">吏??붽굔</div><div class="rnd-info-grid">{info_grid(support_requirements)}</div></div>')
    overview_body = "\n\n".join(part for part in [overview, objective] if clean(part))
    if overview_body:
        sections.append(f'<div class="rnd-section"><div class="rnd-section-title">怨쇱젣 媛쒖슂</div><div class="rnd-section-body">{escape(overview_body)}</div></div>')
    sections.append(f'<div class="rnd-section"><div class="rnd-section-title">怨쇱젣 ?몃? ?댁슜</div><div class="rnd-info-grid">{info_grid(detail_items)}</div></div>')

    st.markdown(f'<div class="rnd-detail-stack">{"".join(sections)}</div>', unsafe_allow_html=True)


def render_detail_card(title: str, fields: list[tuple[str, str]]) -> None:
    items = []
    for label, value in fields:
        raw_value = sanitize_display_text(label, value)
        display_value = display_value_for_label(label, value)
        if not clean(display_value):
            continue

        if should_use_expandable_value(label, raw_value):
            preview_text, _ = split_preview_and_remainder(
                raw_value,
                max_chars=preview_max_chars_for_label(label),
            )
            if "?덉궛" in clean(label):
                preview_text = display_value
            items.append(
                (
                    f'<div class="detail-field">'
                    f'<div class="detail-label">{escape(label)}</div>'
                    f'<details class="detail-more">'
                    f'<summary>'
                    f'<span class="detail-preview-text">{escape(preview_text)}</span>'
                    f'<span class="detail-toggle-text">?붾낫湲?/span>'
                    f'</summary>'
                    f'<div class="detail-more-body">{escape(raw_value)}</div>'
                    f"</details>"
                    f"</div>"
                )
            )
            continue

        items.append(
            (
                f'<div class="detail-field">'
                f'<div class="detail-label">{escape(label)}</div>'
                f'<div class="detail-value">{escape(display_value)}</div>'
                f"</div>"
            )
        )

    if not items:
        items.append(
            '<div class="detail-field"><div class="detail-value">?쒖떆??뺣낫媛 ?놁뒿?덈떎.</div></div>'
        )

    st.markdown(
        (
            f'<div class="detail-card">'
            f'<div class="detail-card-title">{escape(title)}</div>'
            f"{''.join(items)}"
            f"</div>"
        ),
        unsafe_allow_html=True,
    )


def _analysis_clause(value: object, *, max_chars: int = 120) -> str:
    text = sanitize_display_text("analysis", value)
    text = text.replace("|", " ")
    text = re.sub(r"\s+", " ", text).strip()
    text = re.sub(
        r"^(?꾨왂?곹빀??꾨왂 ?곹빀??湲곗닠?곹빀??湲곗닠 愿?⑤룄|湲곗닠愿?⑤룄|?쒖옣?뺣젹|?쒖옣 ?뺣젹|?쒖옣?뺥빀??쒖옣 ?뺥빀??湲닿툒??湲닿툒??뚰봽?몄썾?댁쟻?⑸룄|?뚰봽?몄썾??곹빀??섎뱶?⑥뼱?섏〈??섎뱶?⑥뼱 ?섏〈??\s*:\s*",
        "",
        text,
    )
    if not text:
        return ""
    if len(text) > max_chars:
        text = text[:max_chars].rsplit(" ", 1)[0].strip() or text[:max_chars].strip()
    return text.rstrip(". ")


def _ensure_analysis_sentence(text: str) -> str:
    sentence = clean(text)
    if not sentence:
        return ""
    if sentence.endswith((".", "!", "?")):
        return sentence
    return f"{sentence}."


def _append_analysis_paragraph(paragraphs: list[str], text: str) -> None:
    normalized = clean(text)
    if not normalized:
        return
    comparable = re.sub(r"\s+", " ", normalized)
    if any(re.sub(r"\s+", " ", existing) == comparable for existing in paragraphs):
        return
    paragraphs.append(normalized)


def build_project_analysis_text(*rows: dict | None) -> str:
    merged: dict[str, object] = {}
    for row in rows:
        if isinstance(row, dict):
            merged.update(row)
        elif row is not None:
            merged.update(dict(row))

    objective = _analysis_clause(
        first_non_empty(
            merged,
            "llm_concept_and_development",
            "concept_and_development",
            "llm_support_necessity",
            "support_necessity",
            "llm_technical_background",
            "technical_background",
        ),
        max_chars=100,
    )
    development = _analysis_clause(
        first_non_empty(
            merged,
            "llm_development_content",
            "development_content",
            "llm_support_plan",
            "support_plan",
        ),
        max_chars=110,
    )
    market_fields = split_public_tags(
        first_non_empty(
            merged,
            "target_market",
            "llm_application_field",
            "application_field",
            "llm_score_target_markets",
        ),
        limit=4,
    )
    keywords = split_public_tags(first_non_empty(merged, "llm_keywords", "keywords", "keyword"), limit=5)
    support_need = _analysis_clause(first_non_empty(merged, "llm_support_need", "support_need"), max_chars=90)
    support_plan = _analysis_clause(first_non_empty(merged, "llm_support_plan", "support_plan"), max_chars=90)
    reason_text = _analysis_clause(
        first_non_empty(
            merged,
            "llm_reason",
            "reason",
            "llm_summary",
            "summary",
            "llm_candidate_reason",
            "candidate_reason",
        ),
        max_chars=140,
    )
    total_budget = extract_budget_summary(
        first_non_empty(merged, "llm_total_budget_text", "total_budget_text", "budget", "??쒖삁??, "?ъ뾽鍮?)
    )
    period_text = _analysis_clause(
        first_non_empty(merged, "rfp_period", "project_period", "support_period", "notice_period", "period", "?묒닔湲곌컙"),
        max_chars=60,
    )
    merged_blob = " ".join(
        clean(part)
        for part in [
            objective,
            development,
            support_need,
            support_plan,
            reason_text,
            " ".join(market_fields),
            " ".join(keywords),
            first_non_empty(
                merged,
                "llm_score_software_delivery_fit_reason",
                "software_delivery_fit_reason",
                "llm_score_hardware_dominance_reason",
                "hardware_dominance_reason",
            ),
        ]
        if clean(part)
    )

    software_markers = ["ai", "?곗씠??, "platform", "?뚮옯??, "api", "cloud", "saas", "?뚭퀬由ъ쬁", "遺꾩꽍", "?쒕퉬??, "?쒕??덉씠??]
    hardware_markers = ["?쇱꽌", "遺??, "?λ퉬", "?붾컮?댁뒪", "紐⑤뱢", "?쒖“", "諛섎룄泥?, "諛고꽣由?, "?뚯옱", "濡쒕큸", "?쒖젣??, "?묒궛"]
    sw_hits = sum(1 for marker in software_markers if marker in merged_blob.lower())
    hw_hits = sum(1 for marker in hardware_markers if marker in merged_blob.lower())

    paragraphs: list[str] = []
    if objective:
        _append_analysis_paragraph(paragraphs, _ensure_analysis_sentence(f"??怨쇱젣??{objective}??紐⑺몴濡??쒕떎"))
    elif reason_text:
        _append_analysis_paragraph(paragraphs, _ensure_analysis_sentence(reason_text))

    if development:
        _append_analysis_paragraph(paragraphs, _ensure_analysis_sentence(f"?듭떖 媛쒕컻 踰붿쐞??{development} 以묒떖?쇰줈 援ъ꽦?쒕떎"))
    elif keywords:
        _append_analysis_paragraph(
            paragraphs,
            _ensure_analysis_sentence(f"?듭떖 湲곗닠 ?붿냼??{', '.join(keywords[:4])} 以묒떖?쇰줈 ?댁꽍?쒕떎"),
        )

    if market_fields:
        _append_analysis_paragraph(
            paragraphs,
            _ensure_analysis_sentence(
                f"?뱁엳 {', '.join(market_fields[:3])} 遺꾩빞???곌껐?깆씠 ?믪븘 ?ㅼ젣 ?ъ뾽?붿? ?몄젒 ?쒖옣 ?뺤옣 媛?μ꽦??④퍡 寃?좏븷 留뚰븯??
            ),
        )
    elif support_need:
        _append_analysis_paragraph(
            paragraphs,
            _ensure_analysis_sentence(f"{support_need} ?섏슂? 吏곸젒 ?곌껐??媛?μ꽦??덉뼱 ?ъ뾽 湲고쉶 愿?먯뿉??寃??媛移섍? ?덈떎"),
        )

    if sw_hits >= max(2, hw_hits + 1):
        _append_analysis_paragraph(
            paragraphs,
            "?곗씠?걔텮I쨌?뚮옯??곌퀎 鍮꾩쨷??믪븘 ?뚰봽?몄썾?는룻뵆?ロ뤌 以묒떖 湲곗뾽??곹빀??Opportunity濡??먮떒?쒕떎.",
        )
    elif hw_hits >= max(2, sw_hits + 1):
        _append_analysis_paragraph(
            paragraphs,
            "?λ퉬쨌遺?댟룹젣議??곌퀎 鍮꾩쨷??믪븘 ?섎뱶?⑥뼱 ?듯빀怨??ㅼ쬆 ?섑뻾 ??웾??以묒슂??怨쇱젣濡??먮떒?쒕떎.",
        )
    else:
        _append_analysis_paragraph(
            paragraphs,
            "?뚰봽?몄썾?댁? ?꾩옣 ?ㅼ쬆 ?붿냼媛 ?④퍡 ?붽뎄?섎뒗 ?듯빀??怨쇱젣濡? ?쒕퉬??댁쁺 ??웾怨?湲곗닠 援ы쁽 ??웾??④퍡 媛뽰텣 議곗쭅??곹빀?섎떎.",
        )

    execution_bits: list[str] = []
    if period_text:
        execution_bits.append(f"?ъ뾽湲곌컙? {period_text} ?섏??대떎")
    if total_budget:
        execution_bits.append(f"?덉궛 洹쒕え??{total_budget}濡??뺤씤?쒕떎")
    if support_plan:
        execution_bits.append(f"{support_plan} ?깆쓣 怨좊젮?섎㈃ ?ㅼ쬆 諛??댁쁺 ?곌퀎 媛?μ꽦??寃?좏븷 留뚰븯??)
    if execution_bits:
        _append_analysis_paragraph(paragraphs, _ensure_analysis_sentence(". ".join(execution_bits)))

    if reason_text and len(paragraphs) < 5:
        _append_analysis_paragraph(paragraphs, _ensure_analysis_sentence(reason_text))

    if not paragraphs:
        return "?곌껐??RFP 遺꾩꽍??꾩쭅 ?놁뒿?덈떎.\n\n怨듦퀬 ?먮Ц怨??곌껐 Opportunity瑜??④퍡 ?뺤씤?댁＜?몄슂."
    return "\n\n".join(paragraphs[:5])


def switch_to_detail(page_key: str, identifier: str) -> None:
    current_route = route_core.get_current_route()
    next_route = route_core.normalize_route(
        {
            **current_route,
            "page": normalize_route_page_key(page_key),
            "view": "detail",
            "item_id": clean(identifier),
        }
    )
    if "rfp" in clean(page_key):
        next_route["item_type"] = "rfp"
    else:
        next_route["item_type"] = "notice"
    navigate_to_route_state(next_route, push=True)


def switch_to_table(page_key: str) -> None:
    current_page = normalize_route_page_key(page_key)
    fallback_map = {
        "rfp_queue": route_core.build_rfp_queue_route(),
        "notice_queue": route_core.build_notice_queue_route(),
        "favorites": route_core.build_favorites_route(),
    }
    fallback_route = fallback_map.get(current_page, route_core.build_dashboard_route())
    previous_route = route_core.go_back(fallback_route)
    replace_query_params(with_auth_params(route_core.serialize_route(previous_route)))
    st.rerun()


def get_query_param(name: str) -> str:
    value = st.query_params.get(name, "")
    if isinstance(value, list):
        return clean(value[0]) if value else ""
    return clean(value)


LEGACY_PAGE_KEY_MAP = {
    "opportunity": "rfp_queue",
    "notice": "notice_queue",
    "mss_current": "tipa_current",
    "mss_past": "tipa_archive",
    "mss_archive": "tipa_archive",
    "mss_opportunity": "tipa_opportunity",
}


def normalize_route_page_key(page_key: str) -> str:
    return route_core.normalize_page_key(page_key)


def get_route_state(page_key: str) -> tuple[str, str]:
    current_route = route_core.get_current_route()
    current_page = normalize_route_page_key(current_route.get("page") or "rfp_queue")
    if current_page != page_key:
        return "list", ""

    current_view = clean(current_route.get("view")) or "list"
    selected_id = clean(current_route.get("item_id"))
    return current_view, selected_id


def build_page_href(page_key: str) -> str:
    current_route = route_core.get_current_route()
    params = route_core.serialize_route(
        {
            **current_route,
            "page": normalize_route_page_key(page_key),
            "view": "list",
            "item_id": "",
        }
    )
    params = with_auth_params(params)
    return f"?{urlencode(params)}"


def render_page_tabs(current_page_key: str, tabs: list[tuple[str, str]], *, key: str) -> str:
    page_options = {page_key: label for page_key, label in tabs}
    if current_page_key not in page_options:
        current_page_key = next(iter(page_options))

    cols = st.columns(len(tabs))
    selected_page_key = current_page_key
    for col, (page_key, label) in zip(cols, tabs):
        with col:
            button_type = "primary" if page_key == current_page_key else "secondary"
            if st.button(
                label,
                key=f"{key}_{page_key}",
                type=button_type,
                use_container_width=True,
            ):
                selected_page_key = page_key
    if selected_page_key != current_page_key:
        current_route = route_core.get_current_route()
        next_source = clean(current_route.get("source")) or "iris"
        if selected_page_key == "notice_queue":
            next_source = "notices"
        elif selected_page_key == "favorites":
            next_source = "favorites"
        elif selected_page_key == "dashboard":
            next_source = "dashboard"
        elif selected_page_key == "rfp_queue":
            next_source = "iris"
        next_route = route_core.normalize_route(
            {
                **current_route,
                "source": next_source,
                "source_key": next_source,
                "page": selected_page_key,
                "view": "list",
                "item_id": "",
            }
        )
        route_core.navigate_to(next_route, push=True)
        replace_query_params(with_auth_params(route_core.serialize_route(next_route)))
        st.rerun()
    return selected_page_key


def build_route_href(page_key: str, identifier: str, *, source_key: str | None = None) -> str:
    current_route = route_core.get_current_route()
    params = route_core.serialize_route(
        {
            **current_route,
            "page": normalize_route_page_key(page_key),
            "view": "detail",
            "item_id": clean(identifier),
            "source": clean(source_key) or current_route.get("source"),
            "source_key": clean(source_key) or current_route.get("source_key"),
        }
    )
    params = with_auth_params(params)
    return f"?{urlencode(params)}"


def build_favorite_toggle_href(
    *,
    page_key: str,
    notice_id: str,
    current_value: str,
    source_key: str = "iris",
    notice_title: str = "",
) -> str:
    params = get_query_params_dict()
    params["page"] = normalize_route_page_key(page_key)
    params["view"] = "table"
    params["favorite_toggle"] = "1"
    params["favorite_notice_id"] = clean(notice_id)
    params["favorite_source_key"] = clean(source_key)
    params["favorite_current_value"] = clean(current_value)
    params["favorite_notice_title"] = clean(notice_title)
    params = with_auth_params(params)
    return f"?{urlencode(params)}"




NOTICE_QUEUE_DETAIL_PAGE_KEY = "notice_queue"
UNFAVORITE_REVIEW_STATUS = "??"
STATUS_FILTER_OPTIONS: list[tuple[str, str]] = [
    ("??", "??"),
    ("??", "??"),
    ("??", "??"),
    ("??", "??"),
]
RECOMMENDATION_FILTER_OPTIONS: list[tuple[str, str]] = [
    ("??", "??"),
    ("??", "??"),
]
TOP_TAB_OPTIONS: list[tuple[str, str]] = [
    ("IRIS", "iris"),
    ("MSS", "tipa"),
    ("NIPA", "nipa"),
    ("???", "favorite"),
    ("??/??", "archive"),
]
RECOMMENDATION_RANK = {
    "??": 3,
    "??": 1,
    "??": 0,
    "": -1,
}

def _replace_params(params: dict[str, str]) -> None:
    if callable(replace_query_params):
        replace_query_params(params)
        return
    st.query_params.clear()
    if params:
        st.query_params.update(params)

def _auth_params(params: dict[str, str]) -> dict[str, str]:
    if callable(with_auth_params):
        return with_auth_params(params)
    return params

def _clear_notice_caches() -> None:
    for name in (
        "load_sheet_as_dataframe",
        "load_optional_sheet_as_dataframe",
        "load_app_datasets",
        "build_source_datasets",
        "load_user_review_statuses",
        "clear_public_viewer_caches",
    ):
        fn = ns.get(name)
        clear_fn = getattr(fn, "clear", None)
        if callable(clear_fn):
            clear_fn()
        elif callable(fn) and name == "clear_public_viewer_caches":
            fn()

def _safe_series(rows: pd.DataFrame, columns: list[str]) -> pd.Series:
    if rows is None or rows.empty:
        return pd.Series(dtype="object")
    return series_from_candidates(rows, columns).fillna("").astype(str).str.strip()

def _normalize_key_text(value: object) -> str:
    return re.sub(r"\s+", " ", clean(value)).strip().lower()

def _truncate_queue_text(value: object, max_chars: int = 170) -> str:
    text = re.sub(r"\s+", " ", clean(value)).strip()
    if len(text) <= max_chars:
        return text
    trimmed = text[:max_chars].rsplit(" ", 1)[0].strip()
    return (trimmed or text[:max_chars].strip()).rstrip("., ") + "..."

def _compose_queue_analysis(row: dict | pd.Series | None) -> str:
    row_dict = row.to_dict() if isinstance(row, pd.Series) else dict(row or {})
    title_text = _normalize_key_text(first_non_empty(row_dict, "notice_title", "怨듦퀬紐?))
    project_text = clean(first_non_empty(row_dict, "_queue_project_name"))
    reason_text = clean(first_non_empty(row_dict, "_queue_reason"))
    field_text = clean(first_non_empty(row_dict, "_queue_application_field"))
    market_text = clean(first_non_empty(row_dict, "_queue_target_market"))
    keyword_text = clean(first_non_empty(row_dict, "_queue_keywords"))

    if callable(build_project_analysis_text):
        try:
            analysis_text = clean(build_project_analysis_text(row_dict))
            if analysis_text:
                first_paragraph = next(
                    (part.strip() for part in re.split(r"\n\s*\n", analysis_text) if clean(part)),
                    analysis_text,
                )
                compact = _truncate_queue_text(first_paragraph)
                if compact and _normalize_key_text(compact) != title_text:
                    return compact
        except Exception:
            pass

    if reason_text and _normalize_key_text(reason_text) != title_text:
        return _truncate_queue_text(reason_text)
    if project_text and _normalize_key_text(project_text) != title_text:
        if field_text:
            return _truncate_queue_text(f"{project_text}. {field_text} 遺꾩빞? ?곌껐??怨쇱젣濡?寃?좏븷 ??덉뒿?덈떎.")
        return _truncate_queue_text(project_text)
    if market_text and field_text:
        return _truncate_queue_text(f"{market_text}怨?{field_text} 遺꾩빞 ?뺤옣 媛?μ꽦??덈뒗 怨쇱젣濡?蹂댁엯?덈떎.")
    if field_text:
        return _truncate_queue_text(f"{field_text} 遺꾩빞 以묒떖??怨쇱젣濡??먮떒?⑸땲??")
    if market_text:
        return _truncate_queue_text(f"{market_text} ?쒖옣怨쇱쓽 ?곌껐?깆씠 ?믪? 怨쇱젣濡?蹂댁엯?덈떎.")
    if keyword_text:
        return _truncate_queue_text(f"{keyword_text} 以묒떖??湲곗닠 Opportunity濡?寃?좏븷 ??덉뒿?덈떎.")
    return ""

def _review_value(row: dict | pd.Series | None) -> str:
    row_dict = row.to_dict() if isinstance(row, pd.Series) else dict(row or {})
    return clean(first_non_empty(row_dict, "review_status", "寃??щ?", "寃?좎뿬遺"))

def _review_series(rows: pd.DataFrame) -> pd.Series:
    return _safe_series(rows, ["review_status", "寃??щ?", "寃?좎뿬遺"])

def _is_favorite(row_or_value: dict | pd.Series | str | None) -> bool:
    value = _review_value(row_or_value) if isinstance(row_or_value, (dict, pd.Series)) else clean(row_or_value)
    return value == FAVORITE_REVIEW_STATUS

def _favorite_button_label(current_value: str) -> tuple[bool, str]:
    is_favorite = _is_favorite(current_value)
    return is_favorite, "해제" if is_favorite else "등록"

def _favorite_badge_html() -> str:
    return '<span class="notice-chip notice-chip-favorite">愿??/span>'

def _sync_user_scoped_review(*, notice_id: str, source_key: str, notice_title: str, review_status: str) -> None:
    if not callable(is_user_scoped_operations_enabled) or not callable(upsert_user_review_status):
        return
    if not is_user_scoped_operations_enabled():
        return
    user_scope_key = get_current_operation_scope_key() if callable(get_current_operation_scope_key) else ""
    if not clean(user_scope_key):
        return
    upsert_user_review_status(
        user_id=user_scope_key,
        source_key=source_key,
        notice_id=notice_id,
        notice_title=notice_title,
        review_status=review_status,
    )

def _persist_review_status(*, notice_id: str, source_key: str, review_status: str, notice_title: str = "") -> None:
    normalized_source = clean(source_key) or "iris"
    if normalized_source == "tipa":
        if callable(update_mss_review_status):
            update_mss_review_status(notice_id, review_status)
        elif callable(save_review_status):
            save_review_status(notice_id=notice_id, review_status=review_status, source_key=normalized_source)
    elif normalized_source == "nipa":
        if callable(update_nipa_review_status):
            update_nipa_review_status(notice_id, review_status)
        elif callable(save_review_status):
            save_review_status(notice_id=notice_id, review_status=review_status, source_key=normalized_source)
    else:
        if callable(update_notice_review_status):
            update_notice_review_status(notice_id, review_status)
        elif callable(save_review_status):
            save_review_status(notice_id=notice_id, review_status=review_status, source_key=normalized_source)
    try:
        _sync_user_scoped_review(
            notice_id=notice_id,
            source_key=normalized_source,
            notice_title=notice_title,
            review_status=review_status,
        )
    finally:
        _clear_notice_caches()

def consume_favorite_toggle_query_action() -> None:
    return

def render_favorite_scrap_button(
    *,
    notice_id: str,
    current_value: str,
    source_key: str = "iris",
    notice_title: str = "",
    button_key: str,
    compact: bool = False,
    icon_only: bool = False,
    use_container_width: bool | None = None,
) -> None:
    if not clean(notice_id):
        return
    is_favorite, button_label, _ = favorite_button_props(current_value)
    if icon_only:
        button_label = "?? if is_favorite else "??
    next_value = UNFAVORITE_REVIEW_STATUS if is_favorite else FAVORITE_REVIEW_STATUS
    safe_key = _css_safe_key(button_key)
    if compact:
        active_bg = "#fff7ed" if is_favorite else "#ffffff"
        active_border = "#fdba74" if is_favorite else "#cbd5e1"
        active_color = "#c2410c" if is_favorite else "#64748b"
        min_width = "42px" if icon_only else "auto"
        padding = "0" if icon_only else "0.15rem 0.8rem"
        st.markdown(
            f"""
            <style>
            .st-key-{safe_key} {{
              display: flex;
              justify-content: flex-end;
            }}
            .st-key-{safe_key} button {{
              min-height: 36px !important;
              min-width: {min_width} !important;
              padding: {padding} !important;
              border-radius: 999px !important;
              border: 1px solid {active_border} !important;
              background: {active_bg} !important;
              color: {active_color} !important;
              font-size: {("1.02rem" if icon_only else "0.88rem")} !important;
              font-weight: 800 !important;
              white-space: nowrap !important;
              box-shadow: none !important;
            }}
            </style>
            """,
            unsafe_allow_html=True,
        )
    if use_container_width is None:
        use_container_width = not compact
    if st.button(button_label, key=button_key, use_container_width=use_container_width, type="secondary"):
        _persist_review_status(
            notice_id=notice_id,
            source_key=clean(source_key) or "iris",
            review_status=next_value,
            notice_title=clean(notice_title),
        )
        st.rerun()

def favorite_button_props(current_value: str) -> tuple[bool, str, str]:
    is_favorite, label = _favorite_button_label(current_value)
    return is_favorite, label, "primary" if is_favorite else "secondary"

def _normalize_status_filter(value: str) -> str:
    normalized = clean(value).lower()
    alias_map = {
        "all": "?꾩껜",
        "?꾩껜": "?꾩껜",
        "current": "吏꾪뻾以?,
        "吏꾪뻾以?: "吏꾪뻾以?,
        "scheduled": "?덉젙",
        "?덉젙": "?덉젙",
        "archive": "留덇컧",
        "closed": "留덇컧",
        "留덇컧": "留덇컧",
    }
    return alias_map.get(normalized, "?꾩껜")

def _normalize_recommendation_value(value: object) -> str:
    text = clean(value)
    lowered = text.lower()
    if not text:
        return ""
    if any(marker in lowered for marker in ("鍮꾩텛泥?, "誘몄텛泥?, "not recommend", "reject")):
        return "鍮꾩텛泥?
    if "寃?좉텒?? in text:
        return "蹂댄넻"
    if "蹂댄넻" in text:
        return "蹂댄넻"
    if "異붿쿇" in text or "recommend" in lowered:
        return "異붿쿇"
    if "寃?? in text or "蹂대쪟" in text or "hold" in lowered:
        return "蹂댄넻"
    return text

def _normalize_recommendation_filter(value: str) -> str:
    normalized = _normalize_recommendation_value(value)
    if normalized in {option for option, _ in RECOMMENDATION_FILTER_OPTIONS}:
        return normalized
    return RECOMMENDATION_FILTER_OPTIONS[0][0]

def _status_filter_state_key() -> str:
    return f"{NOTICE_QUEUE_DETAIL_PAGE_KEY}_selected_status_filter"

def _recommendation_filter_state_key() -> str:
    return f"{NOTICE_QUEUE_DETAIL_PAGE_KEY}_selected_recommendation_filter"

def _search_state_key() -> str:
    return f"{NOTICE_QUEUE_DETAIL_PAGE_KEY}_search_text"

def _selected_notice_state_key() -> str:
    return f"{NOTICE_QUEUE_DETAIL_PAGE_KEY}_selected_notice_id"

def _notice_detail_state_key() -> str:
    return f"{NOTICE_QUEUE_DETAIL_PAGE_KEY}_notice_detail_state"


def _notice_filter_widget_key(name: str) -> str:
    return f"{NOTICE_QUEUE_DETAIL_PAGE_KEY}_{clean(name)}"


def _get_notice_filters() -> dict[str, object]:
    current_route = route_core.get_current_route(route_core.build_notice_queue_route())
    route_filters = dict(current_route.get("filters") or {})
    return {
        "status": route_filters.get("status", st.session_state.get(_notice_filter_widget_key("status"), [])),
        "recommendation": route_filters.get(
            "recommendation",
            st.session_state.get(_notice_filter_widget_key("recommendation"), []),
        ),
        "search": clean(route_filters.get("search", st.session_state.get(_notice_filter_widget_key("search"), ""))),
        "source": route_filters.get("source", st.session_state.get(_notice_filter_widget_key("source"), [])),
        "page_size": int(route_filters.get("page_size") or st.session_state.get(_notice_filter_widget_key("page_size"), 20) or 20),
        "dday_max": int(route_filters.get("dday_max") or 0),
        "include_closed": bool(route_filters.get("include_closed", False)),
    }


def _reset_notice_filters() -> None:
    st.session_state[_notice_filter_widget_key("status")] = []
    st.session_state[_notice_filter_widget_key("recommendation")] = []
    st.session_state[_notice_filter_widget_key("search")] = ""
    st.session_state[_notice_filter_widget_key("source")] = []
    st.session_state[_notice_filter_widget_key("page_size")] = 20
    st.session_state[f"{NOTICE_QUEUE_DETAIL_PAGE_KEY}_page_index"] = 1

def _css_safe_key(value: str) -> str:
    return re.sub(r"[^0-9A-Za-z_-]", "-", clean(value))

def _resolve_notice_id(row: dict | pd.Series | None) -> str:
    if row is None:
        return ""
    return clean(first_non_empty(row, "?⑤벀?촇D", "notice_id"))

def _get_notice_row_by_id(rows: pd.DataFrame, notice_id: str) -> dict | pd.Series | None:
    selected_notice_id = clean(notice_id)
    if rows is None or rows.empty or not selected_notice_id:
        return None
    selected_row = get_row_by_column_value(rows, "?⑤벀?촇D", selected_notice_id)
    if selected_row:
        return selected_row
    return get_row_by_column_value(rows, "notice_id", selected_notice_id)

def _default_notice_detail_state() -> dict[str, str]:
    return {
        "view": "table",
        "selected_notice_id": "",
        "source": "",
    }

def _get_notice_detail_state() -> dict[str, str]:
    current_value = st.session_state.get(_notice_detail_state_key(), {})
    state = _default_notice_detail_state()
    if isinstance(current_value, dict):
        state.update(
            {
                "view": clean(current_value.get("view", state["view"])) or "table",
                "selected_notice_id": clean(current_value.get("selected_notice_id", "")),
                "source": clean(current_value.get("source", "")),
            }
        )

    route_page = clean(get_query_param("page"))
    route_view = clean(get_query_param("view"))
    route_notice_id = clean(get_query_param("id"))
    route_source = clean(get_query_param("source"))
    if route_page == NOTICE_QUEUE_DETAIL_PAGE_KEY and route_view == "detail" and route_notice_id:
        state.update(
            {
                "view": "notice_detail",
                "selected_notice_id": route_notice_id,
                "source": route_source,
            }
        )
    elif route_page == NOTICE_QUEUE_DETAIL_PAGE_KEY and route_view == "table":
        state = _default_notice_detail_state()

    st.session_state[_notice_detail_state_key()] = state
    return state

def _set_notice_detail_state(view: str, notice_id: str = "", source: str = "") -> dict[str, str]:
    next_state = {
        "view": clean(view) or "table",
        "selected_notice_id": clean(notice_id),
        "source": clean(source),
    }
    st.session_state[_notice_detail_state_key()] = next_state
    st.session_state["selected_notice_id"] = next_state["selected_notice_id"]
    st.session_state[_selected_notice_state_key()] = next_state["selected_notice_id"]

    params = get_query_params_dict()
    params["page"] = NOTICE_QUEUE_DETAIL_PAGE_KEY
    if next_state["source"]:
        params["source"] = next_state["source"]
    if next_state["view"] == "notice_detail" and next_state["selected_notice_id"]:
        params["view"] = "detail"
        params["id"] = next_state["selected_notice_id"]
    else:
        params["view"] = "table"
        params.pop("id", None)
    _replace_params(_auth_params(params))
    return next_state

def _open_notice_detail(row: pd.Series) -> None:
    notice_id = _resolve_notice_id(row)
    if not notice_id:
        return
    source_value = clean(first_non_empty(row, "source_site", "source_key", "_source_key"))
    _set_notice_detail_state("notice_detail", notice_id, source_value)
    st.rerun()

def _close_notice_detail() -> None:
    _set_notice_detail_state("table", "", "")
    st.rerun()

def _consume_notice_filter_query_actions() -> None:
    st.session_state.setdefault(_status_filter_state_key(), "all")
    st.session_state.setdefault(_recommendation_filter_state_key(), "all")
    status_param = get_query_param("notice_status_filter_select")
    recommendation_param = get_query_param("notice_recommendation_filter_select")
    if not clean(status_param) and not clean(recommendation_param):
        return
    params = get_query_params_dict()
    params["page"] = NOTICE_QUEUE_DETAIL_PAGE_KEY
    params["view"] = "table"
    params.pop("notice_source_filter_select", None)
    params.pop("notice_status_filter_select", None)
    params.pop("notice_recommendation_filter_select", None)
    _replace_params(_auth_params(params))
    st.rerun()

def _build_notice_analysis_summary(opportunity_df: pd.DataFrame) -> pd.DataFrame:
    if opportunity_df is None or opportunity_df.empty:
        return pd.DataFrame(
            columns=[
                "notice_id",
                "_queue_recommendation",
                "_queue_project_name",
                "_queue_budget",
                "_queue_reason",
                "_queue_keywords",
                "_queue_application_field",
                "_queue_target_market",
                "_queue_support_type",
                "_queue_notice_period",
                "_queue_notice_no",
                "_queue_notice_date",
            ]
        )

    working = opportunity_df.copy()
    working["notice_id"] = _safe_series(working, ["notice_id", "怨듦퀬ID", "Notice ID", "source_notice_id"])
    working = working[working["notice_id"].ne("")].copy()
    if working.empty:
        return pd.DataFrame(
            columns=[
                "notice_id",
                "_queue_recommendation",
                "_queue_project_name",
                "_queue_budget",
                "_queue_reason",
                "_queue_keywords",
                "_queue_application_field",
                "_queue_target_market",
                "_queue_support_type",
                "_queue_notice_period",
                "_queue_notice_no",
                "_queue_notice_date",
            ]
        )

    working["_queue_recommendation"] = _safe_series(
        working,
        ["llm_recommendation", "recommendation", "異붿쿇?щ?", "Recommendation"],
    ).apply(_normalize_recommendation_value)
    working["_queue_project_name"] = _safe_series(
        working,
        ["llm_project_name", "project_name", "rfp_title", "Project"],
    )
    working["_queue_budget"] = _safe_series(
        working,
        [
            "llm_total_budget_text",
            "total_budget_text",
            "llm_per_project_budget_text",
            "per_project_budget_text",
            "budget",
            "Budget",
        ],
    )
    working["_queue_reason"] = _safe_series(working, ["llm_reason", "reason", "Reason"])
    working["_queue_keywords"] = _safe_series(working, ["llm_keywords", "keywords", "Keywords"])
    working["_queue_application_field"] = _safe_series(
        working,
        ["llm_application_field", "application_field", "Application Field"],
    )
    working["_queue_target_market"] = _safe_series(
        working,
        ["target_market", "??쒓??ъ쁺??, "llm_score_target_markets"],
    )
    working["_queue_support_type"] = _safe_series(
        working,
        ["pbofr_type", "怨듬え?좏삎", "support_type", "project_type"],
    )
    working["_queue_notice_period"] = _safe_series(
        working,
        ["notice_period", "period", "?묒닔湲곌컙", "?좎껌湲곌컙", "?붿껌湲곌컙"],
    )
    working["_queue_notice_no"] = _safe_series(
        working,
        ["notice_no", "ancm_no", "怨듦퀬踰덊샇"],
    )
    working["_queue_notice_date"] = _safe_series(
        working,
        ["registered_at", "ancm_de", "怨듦퀬?쇱옄", "?깅줉??],
    )
    working["_queue_score"] = pd.to_numeric(
        series_from_candidates(working, ["llm_fit_score", "rfp_score", "Score"]),
        errors="coerce",
    ).fillna(0)
    working["_queue_recommendation_rank"] = (
        working["_queue_recommendation"].map(RECOMMENDATION_RANK).fillna(-1)
    )

    working = working.sort_values(
        by=["notice_id", "_queue_recommendation_rank", "_queue_score", "_queue_project_name"],
        ascending=[True, False, False, True],
        na_position="last",
    )
    best = working.drop_duplicates(subset=["notice_id"], keep="first").copy()
    return best[
        [
            "notice_id",
            "_queue_recommendation",
            "_queue_project_name",
            "_queue_budget",
            "_queue_reason",
            "_queue_keywords",
            "_queue_application_field",
            "_queue_target_market",
            "_queue_support_type",
            "_queue_notice_period",
            "_queue_notice_no",
            "_queue_notice_date",
        ]
    ].reset_index(drop=True)

def _enrich_notice_rows(rows: pd.DataFrame, opportunity_df: pd.DataFrame) -> pd.DataFrame:
    if rows is None or rows.empty:
        return pd.DataFrame()

    enriched = rows.copy()
    enriched["_notice_id"] = _safe_series(enriched, ["怨듦퀬ID", "notice_id"])
    summary_df = _build_notice_analysis_summary(opportunity_df)
    if not summary_df.empty:
        enriched = enriched.merge(
            summary_df,
            left_on="_notice_id",
            right_on="notice_id",
            how="left",
            suffixes=("", "_analysis"),
        )
    for column in (
        "_queue_recommendation",
        "_queue_project_name",
        "_queue_budget",
        "_queue_reason",
        "_queue_keywords",
        "_queue_application_field",
        "_queue_target_market",
        "_queue_support_type",
        "_queue_notice_period",
        "_queue_notice_no",
        "_queue_notice_date",
    ):
        if column not in enriched.columns:
            enriched[column] = ""
        enriched[column] = enriched[column].fillna("").astype(str).str.strip()
    enriched["notice_no"] = _safe_series(enriched, ["notice_no", "怨듦퀬踰덊샇", "ancm_no", "_queue_notice_no"])
    enriched["registered_at"] = _safe_series(enriched, ["registered_at", "怨듦퀬?쇱옄", "ancm_de", "_queue_notice_date"])
    enriched["pbofr_type"] = _safe_series(enriched, ["pbofr_type", "怨듬え?좏삎", "support_type", "_queue_support_type"])
    enriched["notice_period"] = _safe_series(enriched, ["notice_period", "?묒닔湲곌컙", "period", "?좎껌湲곌컙", "_queue_notice_period"])
    enriched["_queue_analysis"] = enriched.apply(_compose_queue_analysis, axis=1)
    return enriched

def _matches_search(rows: pd.DataFrame, search_text: str) -> pd.Series:
    query = clean(search_text).lower()
    if rows.empty or not query:
        return pd.Series(True, index=rows.index)

    columns = [
        "怨듦퀬紐?,
        "notice_title",
        "_queue_project_name",
        "?꾨Ц湲곌?",
        "agency",
        "?뚭?遺泥?,
        "二쇨?遺泥?,
        "ministry",
        "留ㅼ껜",
        "source_label",
        "怨듦퀬踰덊샇",
        "notice_no",
    ]
    stacked = pd.Series("", index=rows.index, dtype="object")
    for column in columns:
        if column in rows.columns:
            stacked = stacked + " " + rows[column].fillna("").astype(str)
    return stacked.str.lower().str.contains(query, na=False)

def _normalize_status_filter_values(value: object) -> list[str]:
    allowed_values = [option for option, _ in STATUS_FILTER_OPTIONS if clean(option) and option != "?꾩껜"]
    if isinstance(value, (list, tuple, set)):
        normalized: list[str] = []
        for item in value:
            item_value = _normalize_status_filter(item)
            if item_value in allowed_values and item_value not in normalized:
                normalized.append(item_value)
        return normalized
    normalized_value = _normalize_status_filter(value)
    return [normalized_value] if normalized_value in allowed_values else []

def _normalize_recommendation_filter_values(value: object) -> list[str]:
    allowed_values = [option for option, _ in RECOMMENDATION_FILTER_OPTIONS if clean(option) and option != "?꾩껜"]
    if isinstance(value, (list, tuple, set)):
        normalized: list[str] = []
        for item in value:
            item_value = _normalize_recommendation_filter(item)
            if item_value in allowed_values and item_value not in normalized:
                normalized.append(item_value)
        return normalized
    normalized_value = _normalize_recommendation_filter(value)
    return [normalized_value] if normalized_value in allowed_values else []

def _apply_notice_filters(
    rows: pd.DataFrame,
    status_filter: object,
    recommendation_filter: object,
    search_text: str,
    *,
    dday_max: int = 0,
    include_closed: bool = False,
) -> pd.DataFrame:
    if rows is None or rows.empty:
        return pd.DataFrame()

    filtered = rows.copy()
    normalized_statuses = _normalize_status_filter_values(status_filter)
    normalized_recommendations = _normalize_recommendation_filter_values(recommendation_filter)

    if normalized_statuses:
        scope_map = {
            "吏꾪뻾以?: "current",
            "?덉젙": "scheduled",
            "留덇컧": "archive",
        }
        allowed_scopes = [scope_map[value] for value in normalized_statuses if value in scope_map]
        if allowed_scopes:
            filtered = filtered[
                filtered["_notice_scope"].fillna("").astype(str).str.strip().isin(allowed_scopes)
            ].copy()
    elif not include_closed:
        filtered = filtered[
            filtered["_notice_scope"].fillna("").astype(str).str.strip().isin(["current", "scheduled"])
        ].copy()
    if normalized_recommendations:
        filtered = filtered[filtered["_queue_recommendation"].isin(normalized_recommendations)].copy()
    if dday_max and dday_max > 0:
        period_series = series_from_candidates(filtered, ["notice_period", "?묒닔湲곌컙", "period"]).fillna("").astype(str)
        status_series = series_from_candidates(filtered, ["status", "rcve_status", "怨듦퀬?곹깭"]).fillna("").astype(str)
        today = pd.Timestamp.now().normalize()

        def _within_deadline_limit(period_text: str, status_text: str) -> bool:
            normalized_status = normalize_notice_status_label(status_text)
            if "留덇컧" in normalized_status:
                return False
            period_end = extract_period_end(clean(period_text))
            if pd.isna(period_end):
                return False
            days_left = int((period_end.normalize() - today).days)
            return 0 <= days_left <= int(dday_max)

        deadline_mask = [
            _within_deadline_limit(period_text, status_text)
            for period_text, status_text in zip(period_series.tolist(), status_series.tolist())
        ]
        filtered = filtered[pd.Series(deadline_mask, index=filtered.index)].copy()

    search_mask = _matches_search(filtered, search_text)
    return filtered[search_mask].copy()


def _notice_queue_pagination_window(current_page: int, total_pages: int, *, window: int = 5) -> list[int]:
    if total_pages <= window + 1:
        return list(range(1, total_pages + 1))
    if current_page <= window:
        return list(range(1, window + 1))
    half_window = window // 2
    start_page = max(1, current_page - half_window)
    end_page = min(total_pages - 1, start_page + window - 1)
    start_page = max(1, end_page - window + 1)
    return list(range(start_page, end_page + 1))


def _render_workspace_pagination(
    *,
    route: dict[str, object],
    current_page: int,
    total_pages: int,
    total_rows: int,
) -> None:
    total_pages = max(1, int(total_pages or 1))
    current_page = max(1, min(int(current_page or 1), total_pages))

    def _page_href(page_number: int) -> str:
        next_route = route_core.normalize_route(route)
        next_route["view"] = "list"
        next_route["item_id"] = ""
        next_route["page_no"] = max(1, min(int(page_number), total_pages))
        params = with_auth_params(route_core.serialize_route(next_route))
        return f"?{urlencode(params)}"

    prev_href = _page_href(current_page - 1) if current_page > 1 else "#"
    next_href = _page_href(current_page + 1) if current_page < total_pages else "#"
    prev_class = "notice-queue-page-nav" + (" is-disabled" if current_page <= 1 else "")
    next_class = "notice-queue-page-nav" + (" is-disabled" if current_page >= total_pages else "")
    nav_html = (
        '<div class="notice-queue-pagination notice-queue-pagination-nav">'
        f'<a class="{prev_class}" href="{escape(prev_href, quote=True)}" target="_self">‹ 이전</a>'
        f'<a class="{next_class}" href="{escape(next_href, quote=True)}" target="_self">다음 ›</a>'
        "</div>"
    )

    page_links: list[str] = []
    page_numbers = _notice_queue_pagination_window(current_page, total_pages)
    for page_number in page_numbers:
        active_class = " is-active" if page_number == current_page else ""
        page_links.append(
            f'<a class="notice-queue-page-link{active_class}" href="{escape(_page_href(page_number), quote=True)}" target="_self">{page_number}</a>'
        )
    if total_pages > page_numbers[-1]:
        if total_pages - page_numbers[-1] > 1:
            page_links.append('<span class="notice-queue-page-ellipsis">…</span>')
        active_class = " is-active" if total_pages == current_page else ""
        page_links.append(
            f'<a class="notice-queue-page-link{active_class}" href="{escape(_page_href(total_pages), quote=True)}" target="_self">{total_pages}</a>'
        )
    number_html = f'<div class="notice-queue-pagination">{"".join(page_links)}</div>'

    form_route = route_core.normalize_route(route)
    form_route["view"] = "list"
    form_route["item_id"] = ""
    params = with_auth_params(route_core.serialize_route(form_route))
    params.pop("page_no", None)
    hidden_inputs = "".join(
        f'<input type="hidden" name="{escape(key, quote=True)}" value="{escape(value, quote=True)}">'
        for key, value in params.items()
        if clean(key) and clean(value)
    )
    jump_html = (
        '<form class="notice-queue-page-jump" method="get">'
        f"{hidden_inputs}"
        f'<input type="number" name="page_no" min="1" max="{total_pages}" value="{current_page}" aria-label="page number">'
        f'<span class="notice-queue-page-jump-total">/{total_pages}</span>'
        '<button type="submit">이동</button>'
        "</form>"
    )

    st.markdown(
        (
            '<div class="notice-queue-footer">'
            f'<div class="notice-queue-footer-meta">총 {total_rows:,}건 · 페이지 {current_page} / {total_pages}</div>'
            f'<nav class="notice-queue-pagination-wrap" aria-label="pagination">{nav_html}{number_html}{jump_html}</nav>'
            "</div>"
        ),
        unsafe_allow_html=True,
    )


def _render_filter_control(title: str, options: list[tuple[str, str]], state_key: str) -> str:
    option_values = [value for value, _ in options]
    option_labels = {value: label for value, label in options}
    current_value = clean(st.session_state.get(state_key, option_values[0]))
    if current_value not in option_labels:
        st.session_state[state_key] = option_values[0]

    st.markdown(f'<div class="notice-filter-group-title">{escape(title)}</div>', unsafe_allow_html=True)
    selected_value = st.radio(
        title,
        options=option_values,
        key=state_key,
        horizontal=True,
        label_visibility="collapsed",
        format_func=lambda value: option_labels.get(value, value),
    )
    return clean(selected_value)

def _inject_notice_queue_dashboard_styles() -> None:
    st.markdown(
        """
        <style>
        :root {
          --app-bg: #eef4fc;
          --surface: #ffffff;
          --surface-soft: #f8fbff;
          --surface-tint: #edf4ff;
          --border: #d8e3f2;
          --border-strong: #c8d7ec;
          --text-strong: #15233b;
          --text-body: #21314d;
          --text-muted: #6c7f9d;
          --text-subtle: #8fa0ba;
          --blue: #3c63f0;
          --blue-soft: #dfe9ff;
        }
        .notice-queue-note,
        .notice-row,
        .notice-row-link,
        .notice-row-topline,
        .notice-row-title,
        .notice-row-subtitle,
        .notice-row-meta,
        .notice-row-meta-item,
        .notice-row-meta-label,
        .notice-row-meta-value {
          font-family: "Segoe UI", "Noto Sans KR", "Apple SD Gothic Neo", sans-serif;
        }
        .notice-queue-note {
          margin: 0.85rem 0 0.35rem;
          color: var(--text-muted);
          font-size: 0.9rem;
          line-height: 1.55;
        }
        .notice-filter-shell {
          margin: 0 0 1.25rem;
        }
        .notice-filter-shell input,
        .notice-filter-shell textarea,
        .notice-filter-shell [data-baseweb="select"] > div {
          min-height: 54px;
          border-color: #e2e8f0 !important;
          border-radius: 28px !important;
          background: #ffffff !important;
          color: #111827 !important;
          font-size: 1rem !important;
          font-weight: 650 !important;
          box-shadow: 8px 0 0 #f1f5f9;
        }
        .notice-filter-shell input::placeholder {
          color: #6b7280 !important;
          font-size: 1rem !important;
          font-weight: 650 !important;
        }
        .notice-filter-shell button {
          min-height: 54px;
          border-radius: 24px !important;
          border-color: #e2e8f0 !important;
          background: #ffffff !important;
          color: #111827 !important;
          font-size: 1rem !important;
          font-weight: 800 !important;
        }
        .notice-table-shell {
          width: 100%;
          min-width: 1180px;
          border: 1px solid #e5e7eb;
          border-radius: 16px;
          background: #ffffff;
          overflow: hidden;
        }
        .notice-table-scroll {
          width: 100%;
          overflow-x: auto;
        }
        .notice-table-head,
        .notice-table-row {
          display: grid;
          grid-template-columns: 106px minmax(420px, 1fr) 180px 220px 92px 74px 94px;
          gap: 1rem;
          align-items: center;
          padding: 1rem 1.2rem;
        }
        .notice-table-head {
          background: #f8fafc;
          border-bottom: 1px solid #e5e7eb;
          color: #64748b;
          font-size: 0.88rem;
          font-weight: 800;
        }
        .notice-table-row {
          min-height: 88px;
          border-top: 1px solid #eef2f7;
        }
        .notice-table-row:first-of-type {
          border-top: none;
        }
        .notice-table-row.is-selected {
          background: #f8fbff;
        }
        .notice-table-cell {
          min-width: 0;
          color: #111827;
          font-size: 0.95rem;
          line-height: 1.48;
        }
        .notice-table-cell.is-center {
          text-align: center;
        }
        .notice-table-title {
          color: #111827 !important;
          font-size: 1.12rem;
          font-weight: 850;
          line-height: 1.4;
          text-decoration: none !important;
        }
        .notice-table-subtitle {
          margin-top: 0.22rem;
          color: #6b7280;
          font-size: 0.9rem;
          line-height: 1.4;
        }
        .notice-table-dday {
          color: #111827;
          font-size: 1.02rem;
          font-weight: 850;
        }
        .notice-table-dday.is-critical {
          color: #dc2626;
        }
        .notice-table-dday.is-warning {
          color: #d97706;
        }
        .notice-table-favorite {
          display: inline-flex;
          align-items: center;
          justify-content: center;
          min-width: 58px;
          height: 32px;
          padding: 0 0.65rem;
          border: 1px solid #cbd5e1;
          border-radius: 8px;
          background: #ffffff;
          color: #2563eb !important;
          font-size: 0.82rem;
          font-weight: 800;
          text-decoration: none !important;
          white-space: nowrap;
        }
        .notice-table-favorite.is-active {
          border-color: #bfdbfe;
          background: #eff6ff;
          color: #1d4ed8 !important;
        }
        .notice-queue-card-row {
          display: grid;
          grid-template-columns: minmax(0, 1fr) 44px;
          gap: 0.5rem;
          align-items: flex-start;
          margin: 0.45rem 0;
        }
        .notice-row-link {
          display: block;
          text-decoration: none !important;
        }
        .notice-row-topline {
          display: flex;
          flex-wrap: wrap;
          gap: 0.42rem;
          margin-bottom: 0.42rem;
        }
        .notice-row-favorite {
          padding-top: 0;
          display: flex;
          justify-content: flex-end;
          align-items: flex-start;
        }
        .notice-queue-header-label {
          color: #64748b;
          font-size: 0.78rem;
          font-weight: 800;
          text-transform: uppercase;
          letter-spacing: 0.04em;
        }
        .notice-queue-divider {
          width: 100%;
          height: 1px;
          background: rgba(226, 232, 240, 0.95);
          margin: 0.15rem 0;
        }
        .notice-chip {
          display: inline-flex;
          align-items: center;
          justify-content: center;
          min-height: 22px;
          padding: 0 8px;
          border-radius: 999px;
          border: 1px solid transparent;
          font-size: 0.72rem;
          font-weight: 700;
          line-height: 1;
          white-space: nowrap;
        }
        .notice-chip-status {
          background: #dcfce7;
          border-color: transparent;
          color: #15803d;
        }
        .notice-chip-status.is-archive {
          background: #fee2e2;
          border-color: transparent;
          color: #dc2626;
        }
        .notice-chip-status.is-scheduled {
          background: #eff6ff;
          border-color: transparent;
          color: #2563eb;
        }
        .notice-chip-recommend {
          background: #ffffff;
          border-color: rgba(17, 24, 39, 0.12);
          color: #111827;
        }
        .notice-chip-neutral {
          background: #ffffff;
          border-color: rgba(17, 24, 39, 0.12);
          color: #111827;
        }
        .notice-row {
          border: 1px solid #e2e8f0;
          border-radius: 20px;
          background: #ffffff;
          padding: 1rem 1.05rem;
          box-shadow: 0 10px 24px rgba(15, 23, 42, 0.04);
        }
        .notice-row-title {
          color: #15233b;
          font-size: 0.95rem;
          font-weight: 800;
          line-height: 1.35;
        }
        .notice-row-subtitle {
          margin-top: 0.18rem;
          color: #6c7f9d;
          font-size: 0.78rem;
          line-height: 1.35;
        }
        .notice-row-meta {
          display: grid;
          grid-template-columns: repeat(2, minmax(0, 1fr));
          gap: 0.55rem 0.8rem;
          margin-top: 0.72rem;
        }
        .notice-row-meta-item {
          min-width: 0;
        }
        .notice-row-meta-label {
          color: #6c7f9d;
          font-size: 0.72rem;
          font-weight: 800;
          line-height: 1.2;
          margin-bottom: 0.18rem;
          text-transform: uppercase;
          letter-spacing: 0.04em;
        }
        .notice-row-meta-value {
          color: #21314d;
          font-size: 0.82rem;
          line-height: 1.42;
          font-weight: 700;
        }
        .notice-queue-cell {
          padding: 0.45rem 0.1rem 0.45rem 0;
          min-width: 0;
        }
        .notice-queue-cell-text {
          color: #111827;
          font-size: 0.9rem;
          line-height: 1.4;
        }
        .notice-queue-cell-muted {
          color: #4b5563;
          font-size: 0.78rem;
          line-height: 1.35;
        }
        .notice-row-rail {
          width: 100%;
          max-width: 320px;
          margin-left: auto;
        }
        .notice-row-summary {
          color: #4b5563;
          font-size: 0.78rem;
          line-height: 1.35;
          display: -webkit-box;
          -webkit-line-clamp: 2;
          -webkit-box-orient: vertical;
          overflow: hidden;
          text-align: left;
          word-break: keep-all;
        }
        .notice-row-summary.is-empty {
          color: #6b7280;
        }
        .notice-queue-status-cell,
        .notice-queue-recommend-cell,
        .notice-queue-favorite-cell {
          display: flex;
          align-items: flex-start;
          justify-content: flex-start;
          min-height: 100%;
        }
        .notice-queue-favorite-cell {
          justify-content: center;
        }
        .notice-row-empty {
          color: #94a3b8;
          font-weight: 600;
        }
        @media (max-width: 640px) {
          .notice-row-title {
            font-size: 0.96rem;
          }
        }
        </style>
        """,
        unsafe_allow_html=True,
    )

def _status_badge_class(status: str) -> str:
    if status == "留덇컧":
        return "notice-chip notice-chip-status is-archive"
    if status == "?덉젙":
        return "notice-chip notice-chip-status is-scheduled"
    return "notice-chip notice-chip-status"

def _recommendation_badge_html(value: str) -> str:
    normalized = _normalize_recommendation_value(value)
    if not normalized:
        return '<span class="notice-chip notice-chip-neutral">遺꾩꽍?湲?/span>'
    if normalized == "異붿쿇":
        class_name = "notice-chip notice-chip-recommend"
    else:
        class_name = "notice-chip notice-chip-neutral"
    return f'<span class="{class_name}">{escape(normalized)}</span>'

def _ensure_collection_for_favorites(
    notice_view_df: pd.DataFrame,
    source_datasets: dict[str, object] | None,
) -> pd.DataFrame:
    if notice_view_df is not None and not notice_view_df.empty and "_collection_id" in notice_view_df.columns:
        return notice_view_df.copy()
    datasets = {
        "notice_current": notice_view_df if isinstance(notice_view_df, pd.DataFrame) else pd.DataFrame(),
        "pending": pd.DataFrame(),
        "notice_archive": pd.DataFrame(),
    }
    return build_crawled_notice_collection(datasets, source_datasets)

def _filter_rows_for_tab(rows: pd.DataFrame, tab_key: str) -> pd.DataFrame:
    if rows is None or rows.empty:
        return pd.DataFrame()
    if tab_key == "iris":
        return rows[rows["source_key"].fillna("").astype(str).str.strip().eq("iris")].copy()
    if tab_key == "tipa":
        return rows[rows["source_key"].fillna("").astype(str).str.strip().eq("tipa")].copy()
    if tab_key == "nipa":
        return rows[rows["source_key"].fillna("").astype(str).str.strip().eq("nipa")].copy()
    if tab_key == "favorite":
        return rows[_review_series(rows).eq(FAVORITE_REVIEW_STATUS)].copy()
    if tab_key == "archive":
        return rows[rows["_notice_scope"].fillna("").astype(str).str.strip().eq("archive")].copy()
    return rows.copy()

def _queue_click_href(row: pd.Series, collection_id: str, source_key: str) -> str:
    del collection_id
    notice_id = _resolve_notice_id(row)
    if notice_id:
        return build_route_href(NOTICE_QUEUE_DETAIL_PAGE_KEY, notice_id, source_key=source_key)
    return ""

def render_crawled_notice_rows(
    rows: pd.DataFrame,
    *,
    key_prefix: str,
    limit: int = 30,
    page_key: str = NOTICE_QUEUE_DETAIL_PAGE_KEY,
    empty_message: str = "?? ?? ???.",
    selected_notice_id: str = "",
    on_select=None,
) -> None:
    if rows is None or rows.empty:
        st.info(empty_message)
        return

    current_route = route_core.get_current_route()

    def _summary_href(notice_id: str, source_key: str) -> str:
        route_builder = route_core.build_favorites_route if page_key == "favorites" else route_core.build_notice_queue_route
        route = route_builder(
            filters=dict(current_route.get("filters") or {}),
            page_no=int(current_route.get("page_no") or 1),
            page_size=int(current_route.get("page_size") or 20),
            view="summary",
            item_id=notice_id,
            source_key=source_key or ("favorites" if page_key == "favorites" else "iris"),
        )
        return f"?{urlencode(with_auth_params(route_core.serialize_route(route)))}"

    def _deadline_class(value: str) -> str:
        if value in {"마감", "D-Day"}:
            return "is-critical"
        match = re.fullmatch(r"D-(\d+)", clean(value))
        if not match:
            return ""
        days_left = int(match.group(1))
        if days_left <= 7:
            return "is-critical"
        if days_left <= 30:
            return "is-warning"
        return ""

    row_html: list[str] = []
    for position, (_, row) in enumerate(rows.head(limit).iterrows()):
        del position
        notice_id = _resolve_notice_id(row)
        source_key = resolve_route_source_key_for_row(row, source_key=row.get("source_key"))
        title = clean(first_non_empty(row, "notice_title", "???")) or notice_id or "-"
        agency = clean(first_non_empty(row, "agency", "????", "????")) or "-"
        notice_no = clean(first_non_empty(row, "notice_no", "????", "ancm_no")) or "-"
        period_text = clean(first_non_empty(row, "notice_period", "????", "period", "_queue_notice_period", "????")) or "-"
        review_value = _review_value(row)
        source_label = clean(first_non_empty(row, "source_label", "source_site", "??")) or (source_key or "IRIS").upper()
        scope = clean(first_non_empty(row, "_notice_scope"))
        status = normalize_notice_status_label(first_non_empty(row, "status", "rcve_status", "???"))
        if not status:
            if scope == "archive":
                status = "마감"
            elif scope == "scheduled":
                status = "예정"
            else:
                status = "접수중"

        agency_text = agency if agency and agency != "-" else source_label
        analysis_text = clean(first_non_empty(row, "_queue_analysis", "_queue_reason", "_queue_project_name"))
        subtitle_parts = [source_label]
        if notice_no and notice_no != "-":
            subtitle_parts.append(notice_no)
        subtitle_text = " / ".join(subtitle_parts)
        is_selected = clean(selected_notice_id) == notice_id
        dday_text = format_dashboard_deadline_badge(period_text, status) or "-"
        dday_class = _deadline_class(dday_text)
        rfp_count = clean(first_non_empty(row, "rfp_count", "RFP Count", "_queue_rfp_count"))
        if not rfp_count:
            rfp_count = "1" if analysis_text else "0"
        favorite_href = build_favorite_toggle_href(
            page_key=page_key,
            notice_id=notice_id,
            current_value=review_value,
            source_key=source_key or "iris",
            notice_title=title,
        )
        favorite_label = "해제" if review_value == FAVORITE_REVIEW_STATUS else "등록"
        favorite_class = " is-active" if review_value == FAVORITE_REVIEW_STATUS else ""
        selected_class = " is-selected" if is_selected else ""
        row_html.append(
            "".join(
                [
                    f'<div class="notice-table-row{selected_class}">',
                    f'<div class="notice-table-cell"><span class="{_status_badge_class(status)}">{escape(status)}</span></div>',
                    '<div class="notice-table-cell">',
                    f'<a class="notice-table-title" href="{escape(_summary_href(notice_id, source_key or "iris"), quote=True)}" target="_self">{escape(_truncate_queue_text(title, max_chars=110))}</a>',
                    f'<div class="notice-table-subtitle">{escape(subtitle_text)}</div>',
                    '</div>',
                    f'<div class="notice-table-cell">{escape(_truncate_queue_text(agency_text, max_chars=24))}</div>',
                    f'<div class="notice-table-cell">{escape(_truncate_queue_text(period_text, max_chars=32))}</div>',
                    f'<div class="notice-table-cell"><span class="notice-table-dday {dday_class}">{escape(dday_text)}</span></div>',
                    f'<div class="notice-table-cell is-center"><strong>{escape(rfp_count)}</strong></div>',
                    f'<div class="notice-table-cell is-center"><a class="notice-table-favorite{favorite_class}" href="{escape(favorite_href, quote=True)}" target="_self">{escape(favorite_label)}</a></div>',
                    '</div>',
                ]
            )
        )

    del key_prefix, on_select
    st.markdown(
        (
            '<div class="notice-table-scroll"><div class="notice-table-shell">'
            '<div class="notice-table-head">'
            '<div>상태</div><div>공고명</div><div>기관</div><div>기간</div><div>D-day</div><div>RFP 수</div><div>관심</div>'
            '</div>'
            + "".join(row_html)
            + '</div></div>'
        ),
        unsafe_allow_html=True,
    )


def _inject_public_notice_queue_saas_styles() -> None:
    st.markdown(
        """
        <style>
        .stApp,
        [data-testid="stAppViewContainer"],
        .main {
          background: #ffffff;
        }
        .notice-saas-shell {
          margin: 0.2rem 0 1rem;
        }
        .notice-saas-header {
          display: flex;
          align-items: flex-end;
          justify-content: space-between;
          gap: 1rem;
          margin-bottom: 1rem;
        }
        .notice-saas-title-row {
          display: flex;
          align-items: center;
          gap: 0.75rem;
          flex-wrap: wrap;
        }
        .notice-saas-title {
          color: #0f172a;
          font-size: 2rem;
          font-weight: 900;
          letter-spacing: -0.04em;
          line-height: 1.08;
          margin: 0;
        }
        .notice-saas-count {
          display: inline-flex;
          align-items: center;
          min-height: 34px;
          padding: 0 0.95rem;
          border-radius: 999px;
          background: #e8f0ff;
          color: #2563eb;
          font-size: 0.9rem;
          font-weight: 900;
        }
        .notice-saas-copy {
          margin-top: 0.45rem;
          color: #64748b;
          font-size: 0.94rem;
          line-height: 1.6;
          max-width: 900px;
        }
        .notice-saas-layout {
          display: grid;
          grid-template-columns: minmax(240px, 278px) minmax(0, 1fr);
          gap: 1rem;
          align-items: start;
        }
        .notice-saas-filter-card,
        .notice-saas-table-card {
          border: 1px solid #dbe4f0;
          border-radius: 24px;
          background: #ffffff;
          box-shadow: 0 10px 24px rgba(15, 23, 42, 0.04);
        }
        .notice-saas-filter-card {
          padding: 1rem 1rem 1.1rem;
          position: sticky;
          top: 1rem;
        }
        .notice-saas-table-card {
          padding: 1rem;
        }
        .notice-saas-card-head {
          display: flex;
          align-items: center;
          justify-content: space-between;
          gap: 0.8rem;
          margin-bottom: 0.9rem;
        }
        .notice-saas-card-title {
          color: #0f172a;
          font-size: 1rem;
          font-weight: 900;
          margin: 0;
        }
        .notice-saas-card-note {
          color: #64748b;
          font-size: 0.8rem;
          font-weight: 700;
        }
        .notice-saas-table-head {
          display: grid;
          grid-template-columns: 0.92fr 3.45fr 1.1fr 1.15fr 1fr 1.4fr 0.9fr 1fr 0.7fr;
          gap: 0.75rem;
          align-items: center;
          padding: 0.78rem 0.95rem;
          border: 1px solid #e6edf7;
          border-radius: 18px;
          background: #f8fbff;
          color: #64748b;
          font-size: 0.76rem;
          font-weight: 900;
          letter-spacing: 0.04em;
          text-transform: uppercase;
        }
        .notice-saas-row {
          display: grid;
          grid-template-columns: 0.92fr 3.45fr 1.1fr 1.15fr 1fr 1.4fr 0.9fr 1fr 0.7fr;
          gap: 0.75rem;
          align-items: center;
          padding: 0.95rem 0.95rem;
          border-bottom: 1px solid #edf2f9;
        }
        .notice-saas-row:last-child {
          border-bottom: none;
        }
        .notice-saas-pill,
        .notice-saas-tag {
          display: inline-flex;
          align-items: center;
          justify-content: center;
          min-height: 24px;
          padding: 0 0.68rem;
          border-radius: 999px;
          font-size: 0.74rem;
          font-weight: 800;
          white-space: nowrap;
        }
        .notice-saas-pill.is-open {
          color: #2563eb;
          background: #eaf1ff;
        }
        .notice-saas-pill.is-scheduled {
          color: #475569;
          background: #edf2f7;
        }
        .notice-saas-pill.is-closed {
          color: #dc2626;
          background: #fee2e2;
        }
        .notice-saas-tag {
          color: #475569;
          background: #f8fafc;
        }
        .notice-saas-title-cell {
          min-width: 0;
        }
        .notice-saas-title-link {
          color: #0f172a !important;
          text-decoration: none !important;
          font-size: 0.96rem;
          font-weight: 850;
          line-height: 1.45;
        }
        .notice-saas-title-link:hover {
          color: #2563eb !important;
        }
        .notice-saas-subline {
          margin-top: 0.24rem;
          color: #64748b;
          font-size: 0.79rem;
          line-height: 1.45;
        }
        .notice-saas-tags {
          display: flex;
          flex-wrap: wrap;
          gap: 0.35rem;
          margin-top: 0.48rem;
        }
        .notice-saas-cell {
          color: #1e293b;
          font-size: 0.84rem;
          line-height: 1.45;
        }
        .notice-saas-cell.is-strong {
          font-weight: 800;
        }
        .notice-saas-cell.is-deadline {
          color: #2563eb;
          font-weight: 900;
        }
        .notice-saas-favorite {
          display: inline-flex;
          align-items: center;
          justify-content: center;
          width: 34px;
          height: 34px;
          border-radius: 999px;
          border: 1px solid #dbe4f0;
          background: #ffffff;
          color: #94a3b8 !important;
          text-decoration: none !important;
          font-size: 1rem;
          font-weight: 900;
        }
        .notice-saas-favorite.is-active {
          color: #f59e0b !important;
          background: #fff7db;
          border-color: #fde68a;
        }
        .notice-saas-footer-meta {
          color: #64748b;
          font-size: 0.84rem;
          font-weight: 700;
        }
        @media (max-width: 1180px) {
          .notice-saas-layout {
            grid-template-columns: 1fr;
          }
          .notice-saas-filter-card {
            position: static;
          }
        }
        @media (max-width: 960px) {
          .notice-saas-header {
            flex-direction: column;
            align-items: flex-start;
          }
          .notice-saas-table-head,
          .notice-saas-row {
            grid-template-columns: 1fr;
          }
        }
        </style>
        """,
        unsafe_allow_html=True,
    )


def _normalize_public_notice_queue_rows(rows: pd.DataFrame) -> pd.DataFrame:
    if rows is None or rows.empty:
        return pd.DataFrame(
            columns=[
                "Notice ID",
                "source_key",
                "Source",
                "Title",
                "Notice No",
                "Status",
                "Period",
                "Agency",
                "Ministry",
                "Date",
                "Recommendation",
                "Budget",
                "Summary",
                "Project",
                "Review",
                "D-Day",
                "_sort_date",
            ]
        )

    working = rows.copy()
    normalized = pd.DataFrame(index=working.index.copy())
    normalized["Notice ID"] = working.apply(lambda row: _resolve_notice_id(row), axis=1)
    normalized["source_key"] = working.apply(
        lambda row: resolve_route_source_key_for_row(row, source_key=row.get("source_key")) or "iris",
        axis=1,
    )
    normalized["Source"] = working.apply(
        lambda row: clean(first_non_empty(row, "source_label", "source_site", "source_key")) or "IRIS",
        axis=1,
    )
    normalized["Title"] = series_from_candidates(working, ["notice_title", "怨듦퀬紐?])
    normalized["Notice No"] = series_from_candidates(working, ["notice_no", "怨듦퀬踰덊샇", "ancm_no"])
    normalized["Status"] = series_from_candidates(working, ["status", "rcve_status", "怨듦퀬?곹깭"])
    normalized["Period"] = series_from_candidates(working, ["notice_period", "?묒닔湲곌컙", "period"])
    normalized["Agency"] = series_from_candidates(working, ["agency", "?꾨Ц湲곌?", "?대떦遺??])
    normalized["Ministry"] = series_from_candidates(working, ["ministry", "?뚭?遺泥?, "二쇨?遺泥?])
    normalized["Date"] = series_from_candidates(working, ["registered_at", "怨듦퀬?쇱옄", "ancm_de"])
    normalized["Recommendation"] = series_from_candidates(working, ["_queue_recommendation", "??쒖텛泥쒕룄", "recommendation"])
    normalized["Budget"] = series_from_candidates(working, ["_queue_budget", "budget", "??쒖삁??])
    normalized["Summary"] = series_from_candidates(working, ["_queue_analysis", "_queue_reason", "??쒖텛泥쒖씠??])
    normalized["Project"] = series_from_candidates(working, ["_queue_project_name", "??쒓낵?쒕챸"])
    normalized["Review"] = series_from_candidates(working, ["review_status", "寃?좎뿬遺", "寃??щ?"])
    normalized["D-Day"] = working.apply(
        lambda row: format_dashboard_deadline_badge(
            first_non_empty(row, "notice_period", "?묒닔湲곌컙", "period"),
            first_non_empty(row, "status", "rcve_status", "怨듦퀬?곹깭"),
        ),
        axis=1,
    )
    normalized["_sort_date"] = parse_date_column(normalized["Date"])
    return normalized


def _public_notice_queue_deadline_rank(value: object) -> int:
    text = clean(value)
    if not text or text == "-":
        return 9998
    if text.startswith("D-"):
        try:
            return int(text[2:])
        except ValueError:
            return 9998
    if text.startswith("D+"):
        try:
            return 5000 + int(text[2:])
        except ValueError:
            return 9999
    return 9997


def _public_notice_queue_sort_label(sort_key: str) -> str:
    labels = {
        "latest": "?뺣젹: 理쒖떊 ?깅줉??,
        "deadline": "?뺣젹: 留덇컧 ?꾨컯",
        "recommend": "?뺣젹: 異붿쿇 ?곗꽑",
        "agency": "?뺣젹: 湲곌??,
    }
    return labels.get(clean(sort_key), labels["latest"])


def _sort_public_notice_queue_rows(rows: pd.DataFrame, sort_key: str) -> pd.DataFrame:
    if rows.empty:
        return rows.copy()

    working = rows.copy()
    normalized_sort = clean(sort_key) or "latest"
    if normalized_sort == "deadline":
        working["_deadline_rank"] = working["D-Day"].apply(_public_notice_queue_deadline_rank)
        return working.sort_values(
            by=["_deadline_rank", "_sort_date", "Title"],
            ascending=[True, False, True],
            na_position="last",
        )
    if normalized_sort == "recommend":
        working["_recommend_rank"] = working["Recommendation"].apply(
            lambda value: 0 if is_positive_recommendation(clean(value)) else 1
        )
        return working.sort_values(
            by=["_recommend_rank", "_sort_date", "Title"],
            ascending=[True, False, True],
            na_position="last",
        )
    if normalized_sort == "agency":
        return working.sort_values(
            by=["Ministry", "Agency", "_sort_date", "Title"],
            ascending=[True, True, False, True],
            na_position="last",
        )
    return working.sort_values(
        by=["_sort_date", "Source", "Title"],
        ascending=[False, True, True],
        na_position="last",
    )


def _render_public_notice_queue_saas_table(rows: pd.DataFrame) -> None:
    if rows.empty:
        st.info("?쒖떆??怨듦퀬媛 ?놁뒿?덈떎.")
        return

    st.markdown(
        (
            '<div class="notice-saas-table-head">'
            '<div>?곹깭</div><div>怨듦퀬紐?/div><div>二쇨?遺泥?/div><div>二쇨?湲곌?</div><div>?깅줉??/div><div>?묒닔湲곌컙</div><div>D-day</div><div>?덉궛</div><div>愿??/div>'
            '</div>'
        ),
        unsafe_allow_html=True,
    )

    row_markup: list[str] = []
    for _, row in rows.iterrows():
        status_text = normalize_notice_status_label(row.get("Status")) or clean(row.get("Status")) or "-"
        status_class = "is-open"
        if "?덉젙" in status_text:
            status_class = "is-scheduled"
        elif "留덇컧" in status_text:
            status_class = "is-closed"
        notice_id = clean(row.get("Notice ID"))
        source_key = clean(row.get("source_key")) or "iris"
        detail_href = build_route_href(NOTICE_QUEUE_DETAIL_PAGE_KEY, notice_id, source_key=source_key) if notice_id else "#"
        favorite_href = build_favorite_toggle_href(
            page_key=NOTICE_QUEUE_DETAIL_PAGE_KEY,
            notice_id=notice_id,
            current_value=clean(row.get("Review")),
            source_key=source_key,
        ) if notice_id else "#"
        favorite_active = clean(row.get("Review")) == FAVORITE_REVIEW_STATUS
        favorite_class = " is-active" if favorite_active else ""
        favorite_label = "?? if favorite_active else "??
        tags = [
            f'<span class="notice-saas-tag">{escape(clean(row.get("Source")) or "-")}</span>',
        ]
        subtitle_parts = [part for part in [clean(row.get("Agency")), clean(row.get("Notice No"))] if clean(part)]
        subtitle_text = " / ".join(subtitle_parts) if subtitle_parts else "-"
        row_markup.append(
            (
                '<div class="notice-saas-row">'
                f'<div><span class="notice-saas-pill {status_class}">{escape(status_text)}</span></div>'
                '<div class="notice-saas-title-cell">'
                f'<a class="notice-saas-title-link" href="{escape(detail_href, quote=True)}" target="_self">{escape(truncate_text(row.get("Title"), max_chars=84))}</a>'
                f'<div class="notice-saas-subline">{escape(truncate_text(subtitle_text, max_chars=62))}</div>'
                f'<div class="notice-saas-tags">{"".join(tags)}</div>'
                '</div>'
                f'<div class="notice-saas-cell">{escape(truncate_text(clean(row.get("Ministry")) or "-", max_chars=18))}</div>'
                f'<div class="notice-saas-cell">{escape(truncate_text(clean(row.get("Agency")) or "-", max_chars=18))}</div>'
                f'<div class="notice-saas-cell">{escape(clean(row.get("Date")) or "-")}</div>'
                f'<div class="notice-saas-cell">{escape(truncate_text(clean(row.get("Period")) or "-", max_chars=28))}</div>'
                f'<div class="notice-saas-cell is-deadline">{escape(clean(row.get("D-Day")) or "-")}</div>'
                f'<div class="notice-saas-cell is-strong">{escape(truncate_text(clean(row.get("Budget")) or "-", max_chars=16))}</div>'
                f'<div><a class="notice-saas-favorite{favorite_class}" href="{escape(favorite_href, quote=True)}" target="_self">{favorite_label}</a></div>'
                '</div>'
            )
        )

    st.markdown("".join(row_markup), unsafe_allow_html=True)


def _render_notice_queue_screen(
    source_df: pd.DataFrame,
    opportunity_df: pd.DataFrame,
    detail_opportunity_df: pd.DataFrame,
) -> None:
    del opportunity_df
    source_df = _enrich_notice_rows(source_df, detail_opportunity_df)
    current_route = route_core.get_current_route(route_core.build_notice_queue_route())
    if clean(current_route.get("page")) == NOTICE_QUEUE_DETAIL_PAGE_KEY and clean(current_route.get("view")) == "detail":
        selected_row = _get_notice_row_by_id(source_df, clean(current_route.get("item_id")))
        back_col, info_col = st.columns([1.9, 4.1])
        with back_col:
            if st.button("??Notice Queue濡??뚯븘媛湲?, key=f"{NOTICE_QUEUE_DETAIL_PAGE_KEY}_back_to_table", use_container_width=False, type="secondary"):
                go_back_route(route_core.build_notice_queue_route())
                st.rerun()
        with info_col:
            st.markdown('<div class="page-note">Notice Queue / Notice Detail</div>', unsafe_allow_html=True)
        if not selected_row:
            st.info("?쒖떆??怨듦퀬媛 ?놁뒿?덈떎.")
            return
        render_notice_detail_from_row(selected_row, detail_opportunity_df)
        return

    render_notice_queue_ui_styles()
    _inject_public_notice_queue_saas_styles()
    if source_df is None or source_df.empty:
        st.info("?쒖떆??怨듦퀬媛 ?놁뒿?덈떎.")
        return

    filters = _get_notice_filters()
    status_widget_key = _notice_filter_widget_key("status")
    search_widget_key = _notice_filter_widget_key("search")
    source_widget_key = _notice_filter_widget_key("source")
    page_size_widget_key = _notice_filter_widget_key("page_size")
    page_index_state_key = f"{NOTICE_QUEUE_DETAIL_PAGE_KEY}_page_index"
    sort_widget_key = f"{NOTICE_QUEUE_DETAIL_PAGE_KEY}_saas_sort"
    ministry_widget_key = f"{NOTICE_QUEUE_DETAIL_PAGE_KEY}_saas_ministry"
    agency_widget_key = f"{NOTICE_QUEUE_DETAIL_PAGE_KEY}_saas_agency"
    st.session_state.setdefault(status_widget_key, filters["status"])
    st.session_state.setdefault(search_widget_key, filters["search"])
    st.session_state.setdefault(source_widget_key, filters.get("source", []))
    st.session_state.setdefault(page_size_widget_key, int(filters.get("page_size") or 20))
    st.session_state.setdefault(page_index_state_key, int(current_route.get("page_no") or 1))
    st.session_state.setdefault(sort_widget_key, "latest")

    base_view_rows = _normalize_public_notice_queue_rows(source_df)
    ministry_options = ["?꾩껜"] + sorted([value for value in base_view_rows["Ministry"].dropna().astype(str).unique().tolist() if clean(value)])
    agency_options = ["?꾩껜"] + sorted([value for value in base_view_rows["Agency"].dropna().astype(str).unique().tolist() if clean(value)])
    st.session_state.setdefault(ministry_widget_key, "?꾩껜")
    st.session_state.setdefault(agency_widget_key, "?꾩껜")

    st.markdown(
        (
            '<div class="notice-saas-shell">'
            '<div class="notice-saas-header">'
            '<div>'
            '<div class="notice-saas-title-row">'
            '<h1 class="notice-saas-title">Notice Queue</h1>'
            f'<span class="notice-saas-count">珥?{len(base_view_rows):,}嫄?/span>'
            '</div>'
            '<div class="notice-saas-copy">?먮Ц 怨듦퀬瑜?怨듦퀬 ?⑥쐞 mailbox ?붾㈃?쇰줈 ?뺣━?덉뒿?덈떎. ?곌껐??RFP 遺꾩꽍 ???怨듦퀬 硫뷀??곗씠??먯껜瑜?鍮좊Ⅴ寃??묎퀬 ?곸꽭濡??댁뼱??寃?좏븷 ??덉뒿?덈떎.</div>'
            '</div>'
            '</div>'
            '</div>'
        ),
        unsafe_allow_html=True,
    )

    layout_cols = st.columns([1.2, 5.2], gap="large")
    with layout_cols[0]:
        st.markdown('<div class="notice-saas-filter-card">', unsafe_allow_html=True)
        st.markdown(
            '<div class="notice-saas-card-head"><div class="notice-saas-card-title">?꾪꽣</div><div class="notice-saas-card-note">?먮룞 ?곸슜</div></div>',
            unsafe_allow_html=True,
        )
        st.multiselect(
            "怨듦퀬?곹깭",
            options=[value for value, _ in STATUS_FILTER_OPTIONS if value != "?꾩껜"],
            key=status_widget_key,
            placeholder="?꾩껜",
        )
        st.multiselect(
            "異쒖쿂",
            options=[label for label, _ in TOP_TAB_OPTIONS if label not in {"愿?ш났怨?, "蹂닿?/留덇컧"}],
            key=source_widget_key,
            placeholder="?꾩껜",
        )
        st.selectbox("二쇨?遺泥?, options=ministry_options, key=ministry_widget_key)
        st.selectbox("二쇨?湲곌?", options=agency_options, key=agency_widget_key)
        if st.button("?꾪꽣 珥덇린??, key=f"{NOTICE_QUEUE_DETAIL_PAGE_KEY}_saas_reset", use_container_width=True):
            _reset_notice_filters()
            st.session_state[ministry_widget_key] = "?꾩껜"
            st.session_state[agency_widget_key] = "?꾩껜"
            st.session_state[sort_widget_key] = "latest"
            st.session_state[page_index_state_key] = 1
            st.rerun()
        st.markdown('</div>', unsafe_allow_html=True)

    with layout_cols[1]:
        st.markdown('<div class="notice-saas-table-card">', unsafe_allow_html=True)
        toolbar_cols = st.columns([4.2, 1.35, 1.2, 1.15], gap="small")
        with toolbar_cols[0]:
            st.text_input(
                "search-filter",
                key=search_widget_key,
                placeholder="怨듦퀬紐?/ 怨듦퀬踰덊샇 / 湲곌? 寃??,
                label_visibility="collapsed",
            )
        with toolbar_cols[1]:
            st.selectbox(
                "?뺣젹",
                options=["latest", "deadline", "recommend", "agency"],
                key=sort_widget_key,
                format_func=_public_notice_queue_sort_label,
                label_visibility="collapsed",
            )
        with toolbar_cols[2]:
            st.selectbox(
                "Page size",
                options=[20, 50, 100],
                key=page_size_widget_key,
                label_visibility="collapsed",
            )

        filters = {
            "status": st.session_state.get(status_widget_key, []),
            "recommendation": [],
            "search": clean(st.session_state.get(search_widget_key, "")),
            "source": st.session_state.get(source_widget_key, []),
            "page_size": int(st.session_state.get(page_size_widget_key, 20) or 20),
            "dday_max": int(filters.get("dday_max") or 0),
            "include_closed": bool(filters.get("include_closed", False)),
        }
        filtered_source_df = _apply_notice_filters(
            source_df,
            filters["status"],
            filters["recommendation"],
            filters["search"],
            dday_max=int(filters.get("dday_max") or 0),
            include_closed=bool(filters.get("include_closed", False)),
        )
        selected_sources = filters["source"]
        if selected_sources:
            allowed_source_keys = {
                "IRIS": "iris",
                "MSS": "tipa",
                "NIPA": "nipa",
            }
            allowed_values = {allowed_source_keys.get(clean(value), clean(value).lower()) for value in selected_sources}
            filtered_source_df = filtered_source_df[
                filtered_source_df["source_key"].fillna("").astype(str).str.strip().isin(allowed_values)
            ].copy()

        filtered_view_rows = _normalize_public_notice_queue_rows(filtered_source_df)
        selected_ministry = clean(st.session_state.get(ministry_widget_key, "?꾩껜"))
        if selected_ministry and selected_ministry != "?꾩껜":
            filtered_view_rows = filtered_view_rows[
                filtered_view_rows["Ministry"].fillna("").astype(str).str.strip().eq(selected_ministry)
            ].copy()
        selected_agency = clean(st.session_state.get(agency_widget_key, "?꾩껜"))
        if selected_agency and selected_agency != "?꾩껜":
            filtered_view_rows = filtered_view_rows[
                filtered_view_rows["Agency"].fillna("").astype(str).str.strip().eq(selected_agency)
            ].copy()

        sort_key_value = clean(st.session_state.get(sort_widget_key, "latest")) or "latest"
        filtered_view_rows = _sort_public_notice_queue_rows(filtered_view_rows, sort_key_value)
        export_df = filtered_view_rows[
            ["Source", "Status", "Title", "Ministry", "Agency", "Date", "Period", "D-Day", "Budget", "Notice ID"]
        ].copy() if not filtered_view_rows.empty else pd.DataFrame(
            columns=["Source", "Status", "Title", "Ministry", "Agency", "Date", "Period", "D-Day", "Budget", "Notice ID"]
        )
        with toolbar_cols[3]:
            st.download_button(
                "?대낫?닿린",
                data=export_df.to_csv(index=False, encoding="utf-8-sig"),
                file_name="notice_queue_export.csv",
                mime="text/csv",
                use_container_width=True,
            )

        page_size = int(filters["page_size"] or 20)
        total_rows = len(filtered_view_rows)
        total_pages = max(1, math.ceil(total_rows / page_size)) if page_size else 1
        current_page = int(current_route.get("page_no") or st.session_state.get(page_index_state_key, 1) or 1)
        current_page = max(1, min(current_page, total_pages))
        st.session_state[page_index_state_key] = current_page
        start_idx = (current_page - 1) * page_size
        page_rows = filtered_view_rows.iloc[start_idx:start_idx + page_size].copy()

        start_idx = (current_page - 1) * page_size
        page_rows = filtered_source_df.iloc[start_idx:start_idx + page_size].copy()

        def _select_notice_preview(row: pd.Series) -> None:
            notice_id = _resolve_notice_id(row)
            route = route_core.build_notice_queue_route(
                filters=filters,
                page_no=current_page,
                page_size=page_size,
                view="summary",
                item_id=notice_id,
                source_key=resolve_route_source_key_for_row(row, source_key=row.get("source_key")) or "iris",
            )
            route_core.set_current_route(route)
            replace_query_params(with_auth_params(route_core.serialize_route(route)))

        render_crawled_notice_rows(
            page_rows,
            key_prefix=f"{NOTICE_QUEUE_DETAIL_PAGE_KEY}_browser_list",
            limit=page_size,
            page_key=NOTICE_QUEUE_DETAIL_PAGE_KEY,
            empty_message="표시할 공고가 없습니다.",
            selected_notice_id=selected_notice_id,
            on_select=_select_notice_preview,
        )
        pagination_route = route_core.build_notice_queue_route(
            filters=filters,
            page_no=current_page,
            page_size=page_size,
            view="list",
            item_id="",
            source_key=clean(current_route.get("source_key")) or "iris",
        )
        _render_workspace_pagination(
            route=pagination_route,
            current_page=current_page,
            total_pages=total_pages,
            total_rows=total_rows,
        )

    with summary_col:
        selected_notice_id = clean(current_route.get("item_id")) if clean(current_route.get("view")) == "summary" else ""
        selected_row = _get_notice_row_by_id(filtered_source_df, selected_notice_id) if selected_notice_id else None
        if selected_row is None and selected_notice_id:
            selected_row = _get_notice_row_by_id(source_df, selected_notice_id)
        _render_notice_preview_panel(
            selected_row,
            panel_key="notice_queue_preview",
            empty_title="Notice를 선택하면 Summary Panel이 열립니다.",
            empty_copy="리스트는 그대로 유지되고, 필요한 경우에만 우측 패널의 상세 버튼으로 이동합니다.",
            close_callback=lambda: (
                route_core.set_current_route(
                    route_core.build_notice_queue_route(
                        filters=filters,
                        page_no=int(st.session_state.get(page_index_state_key, 1) or 1),
                        page_size=int(st.session_state.get(page_size_widget_key, 20) or 20),
                    )
                ),
                replace_query_params(with_auth_params(route_core.serialize_route(route_core.get_current_route()))),
                st.rerun(),
            ),
        )

    route_snapshot = route_core.build_notice_queue_route(
        filters=filters,
        page_no=int(st.session_state.get(page_index_state_key, 1) or 1),
        page_size=int(st.session_state.get(page_size_widget_key, 20) or 20),
        view="list",
        item_id="",
        source_key=clean(current_route.get("source_key")) or "iris",
    )
    route_core.set_current_route(route_snapshot)
    replace_query_params(with_auth_params(route_core.serialize_route(route_snapshot)))

def render_favorite_notice_page(
    notice_view_df: pd.DataFrame,
    opportunity_df: pd.DataFrame,
    source_datasets: dict[str, object] | None = None,
) -> None:
    current_route = route_core.get_current_route(route_core.build_favorites_route())
    source_df = _ensure_collection_for_favorites(notice_view_df, source_datasets)
    source_df = _enrich_notice_rows(source_df, opportunity_df)
    if clean(current_route.get("page")) == "favorites" and clean(current_route.get("view")) == "detail":
        selected_row = _get_notice_row_by_id(source_df, clean(current_route.get("item_id")))
        back_col, info_col = st.columns([1.8, 4.2])
        with back_col:
            if st.button("??Favorites濡??뚯븘媛湲?, key="favorites_back_to_table", use_container_width=False, type="secondary"):
                go_back_route(route_core.build_favorites_route())
                st.rerun()
        with info_col:
            st.markdown('<div class="page-note">Favorites / Notice Detail</div>', unsafe_allow_html=True)
        render_notice_detail_from_row(selected_row, opportunity_df)
        return

    _inject_opportunity_workspace_styles()
    st.subheader("Favorites")
    st.caption("愿??怨듦퀬瑜?由ъ뒪?몄? Summary Panel濡??섎닠 寃?좏빀?덈떎.")
    if source_df is None or source_df.empty:
        st.info("?쒖떆??愿??怨듦퀬媛 ?놁뒿?덈떎.")
        return
    favorite_rows = source_df[_review_series(source_df).eq(FAVORITE_REVIEW_STATUS)].copy()
    if favorite_rows.empty:
        st.info("?쒖떆??愿??怨듦퀬媛 ?놁뒿?덈떎.")
        return
    favorite_rows["_favorite_type"] = favorite_rows["_queue_project_name"].fillna("").astype(str).str.strip().apply(
        lambda value: "RFP ?곌껐" if clean(value) else "Notice"
    )
    favorite_rows["_favorite_deadline"] = favorite_rows.apply(
        lambda row: format_dashboard_deadline_badge(
            clean(first_non_empty(row, "notice_period", "?묒닔湲곌컙", "period")),
            normalize_notice_status_label(first_non_empty(row, "status", "rcve_status", "怨듦퀬?곹깭")) or "-",
        ),
        axis=1,
    )
    route_filters = dict(current_route.get("filters") or {})
    type_key = "favorites_filter_type"
    review_key = "favorites_filter_review"
    deadline_key = "favorites_filter_deadline"
    page_size_key = "favorites_page_size"
    page_index_key = "favorites_page_index"
    st.session_state.setdefault(type_key, route_filters.get("type", []))
    st.session_state.setdefault(review_key, route_filters.get("review", []))
    st.session_state.setdefault(deadline_key, route_filters.get("deadline", []))
    st.session_state.setdefault(page_size_key, int(route_filters.get("page_size") or current_route.get("page_size") or 20))
    st.session_state.setdefault(page_index_key, int(current_route.get("page_no") or 1))

    display_col, summary_col = st.columns([5.4, 2.15], gap="large")
    with display_col:
        filter_cols = st.columns(4)
        with filter_cols[0]:
            st.multiselect("???, options=["Notice", "RFP ?곌껐"], key=type_key, placeholder="?꾩껜")
        with filter_cols[1]:
            review_options = sorted(
                {
                    value
                    for value in favorite_rows["review_status"].fillna("").astype(str).tolist()
                    if clean(value)
                }
            )
            st.multiselect("寃?좎긽??, options=review_options, key=review_key, placeholder="?꾩껜")
        with filter_cols[2]:
            st.multiselect("D-day", options=["吏꾪뻾以?, "7??대궡", "30??대궡", "?덉젙", "留덇컧"], key=deadline_key, placeholder="?꾩껜")
        with filter_cols[3]:
            st.selectbox("Page size", options=[20, 50, 100], key=page_size_key)

        filters = {
            "type": st.session_state.get(type_key, []),
            "review": st.session_state.get(review_key, []),
            "deadline": st.session_state.get(deadline_key, []),
            "page_size": int(st.session_state.get(page_size_key, 20) or 20),
        }
        filtered_rows = favorite_rows.copy()
        if filters["type"]:
            filtered_rows = filtered_rows[filtered_rows["_favorite_type"].isin(filters["type"])].copy()
        if filters["review"]:
            filtered_rows = filtered_rows[_review_series(filtered_rows).isin(filters["review"])].copy()
        if filters["deadline"]:
            def _favorite_deadline_match(row: pd.Series) -> bool:
                deadline_text = clean(row.get("_favorite_deadline"))
                status_text = normalize_notice_status_label(first_non_empty(row, "status", "rcve_status", "怨듦퀬?곹깭")) or "-"
                buckets: set[str] = set()
                if "留덇컧" in status_text:
                    buckets.add("留덇컧")
                elif "?덉젙" in status_text:
                    buckets.add("?덉젙")
                else:
                    buckets.add("吏꾪뻾以?)
                period_end = extract_period_end(clean(first_non_empty(row, "notice_period", "?묒닔湲곌컙", "period")))
                if pd.notna(period_end):
                    days_left = int((period_end.normalize() - pd.Timestamp.now().normalize()).days)
                    if days_left <= 7:
                        buckets.add("7??대궡")
                    if days_left <= 30:
                        buckets.add("30??대궡")
                if deadline_text == "-":
                    buckets.add("留덇컧")
                return any(option in buckets for option in filters["deadline"])
            filtered_rows = filtered_rows[filtered_rows.apply(_favorite_deadline_match, axis=1)].copy()

        page_size = int(filters["page_size"] or 20)
        total_rows = len(filtered_rows)
        total_pages = max(1, math.ceil(total_rows / page_size)) if page_size else 1
        current_page = int(current_route.get("page_no") or st.session_state.get(page_index_key, 1) or 1)
        current_page = max(1, min(current_page, total_pages))
        st.session_state[page_index_key] = current_page
        selected_notice_id = clean(current_route.get("item_id")) if clean(current_route.get("view")) == "summary" else ""

        def _select_favorite_preview(row: pd.Series) -> None:
            route = route_core.build_favorites_route(
                filters=filters,
                page_no=current_page,
                page_size=page_size,
                view="summary",
                item_id=_resolve_notice_id(row),
                source_key=resolve_route_source_key_for_row(row, source_key=row.get("source_key")) or "favorites",
            )
            route_core.set_current_route(route)
            replace_query_params(with_auth_params(route_core.serialize_route(route)))

        start_idx = (current_page - 1) * page_size
        page_rows = filtered_rows.iloc[start_idx:start_idx + page_size].copy()
        render_crawled_notice_rows(
            page_rows,
            key_prefix=f"{NOTICE_QUEUE_DETAIL_PAGE_KEY}_favorite_page",
            page_key="favorites",
            empty_message="?쒖떆??愿??怨듦퀬媛 ?놁뒿?덈떎.",
            selected_notice_id=selected_notice_id,
            on_select=_select_favorite_preview,
        )
        pagination_route = route_core.build_favorites_route(
            filters=filters,
            page_no=current_page,
            page_size=page_size,
            view="list",
            item_id="",
            source_key="favorites",
        )
        _render_workspace_pagination(
            route=pagination_route,
            current_page=current_page,
            total_pages=total_pages,
            total_rows=total_rows,
        )

    with summary_col:
        selected_notice_id = clean(current_route.get("item_id")) if clean(current_route.get("view")) == "summary" else ""
        selected_row = _get_notice_row_by_id(favorite_rows, selected_notice_id) if selected_notice_id else None
        _render_notice_preview_panel(
            selected_row,
            panel_key="favorites_preview",
            empty_title="愿??怨듦퀬瑜??좏깮?섎㈃ Summary Panel??대┰?덈떎.",
            empty_copy="Favorites 由ъ뒪?몃뒗 洹몃?濡??먭퀬, ?꾩슂??寃쎌슦?먮쭔 ?⑤꼸??곸꽭 踰꾪듉?쇰줈 ?대룞?⑸땲??",
            close_callback=lambda: (
                route_core.set_current_route(
                    route_core.build_favorites_route(
                        filters=filters,
                        page_no=int(st.session_state.get(page_index_key, 1) or 1),
                        page_size=int(st.session_state.get(page_size_key, 20) or 20),
                    )
                ),
                replace_query_params(with_auth_params(route_core.serialize_route(route_core.get_current_route()))),
                st.rerun(),
            ),
            detail_source_key=clean(current_route.get("source_key")) or "favorites",
        )

    route_snapshot = route_core.build_favorites_route(
        filters=filters,
        page_no=int(st.session_state.get(page_index_key, 1) or 1),
        page_size=int(st.session_state.get(page_size_key, 20) or 20),
        view="summary" if clean(current_route.get("item_id")) and clean(current_route.get("view")) == "summary" else "list",
        item_id=clean(current_route.get("item_id")) if clean(current_route.get("view")) == "summary" else "",
        source_key=clean(current_route.get("source_key")) or "favorites",
    )
    route_core.set_current_route(route_snapshot)
    replace_query_params(with_auth_params(route_core.serialize_route(route_snapshot)))

def render_notice_queue_page(datasets: dict[str, pd.DataFrame], source_datasets: dict[str, object] | None) -> None:
    render_notice_queue_page_module(
        st,
        datasets,
        source_datasets,
        api=sys.modules[__name__],
    )

def render_notices_source(
    source_config,
    mode_config,
    datasets: dict[str, pd.DataFrame],
    source_datasets: dict[str, object] | None,
    *,
    show_internal_tabs: bool = True,
) -> None:
    render_notice_queue_source_module(
        st,
        source_config,
        mode_config,
        datasets,
        source_datasets,
        show_internal_tabs=show_internal_tabs,
        api=sys.modules[__name__],
    )

def resolve_route_source_key_for_row(row: dict | pd.Series | None, source_key: str | None = None) -> str:
    source_alias_map = {
        "mss": "tipa",
        "tipa": "tipa",
        "nipa": "nipa",
        "iris": "iris",
    }

    candidate_values = [source_key]
    if row is not None:
        row_dict = row.to_dict() if isinstance(row, pd.Series) else dict(row)
        candidate_values.extend(
            [
                row_dict.get("source_key"),
                row_dict.get("_source_key"),
                row_dict.get("source"),
                row_dict.get("Source"),
                row_dict.get("source_site"),
            ]
        )

    for candidate in candidate_values:
        normalized = source_alias_map.get(clean(candidate).lower(), "")
        if normalized:
            return normalized
    return ""


def render_clickable_table(
    df: pd.DataFrame,
    preferred_columns: list[str],
    page_key: str,
    id_column: str,
    *,
    source_key_column: str | None = None,
    source_key_value: str | None = None,
) -> None:
    display_columns = [col for col in preferred_columns if col in df.columns]
    display_columns = [
        col for col in display_columns
        if df[col].fillna("").astype(str).str.strip().ne("").any()
    ]
    if not display_columns:
        display_columns = [col for col in df.columns if not col.startswith("_")]

    if df.empty:
        st.info("?쒖떆??곗씠?곌? ?놁뒿?덈떎.")
        return

    column_widths = {
        "怨듦퀬?쇱옄": "92px",
        "怨듦퀬踰덊샇": "132px",
        "?꾨Ц湲곌?": "126px",
        "?꾨Ц湲곌?紐?: "126px",
        "怨듦퀬紐?: "280px",
        "notice_title": "280px",
        "?대떦 怨쇱젣紐?: "280px",
        "project_name": "280px",
        "怨듦퀬?곹깭": "92px",
        "?묒닔湲곌컙": "156px",
        "異붿쿇?щ?": "84px",
        "異붿쿇??: "84px",
        "異붿쿇??諛??먯닔": "96px",
        "?먯닔": "84px",
        "?덉궛": "122px",
        "budget": "122px",
        "寃??щ?": "84px",
        "寃?좎뿬遺": "84px",
        "?곸꽭留곹겕": "76px",
        "detail_link": "76px",
    }
    compact_limits = {
        "怨듦퀬紐?: 56,
        "notice_title": 56,
        "?대떦 怨쇱젣紐?: 52,
        "project_name": 52,
        "?묒닔湲곌컙": 26,
        "?덉궛": 24,
        "budget": 24,
    }

    internal_link_columns = {"怨듦퀬紐?, "notice_title", "?대떦 怨쇱젣紐?, "?곌껐 怨쇱젣紐?, "project_name"}

    header_cells = [f"<th>{escape(column)}</th>" for column in display_columns]
    header_html = "".join(header_cells)

    body_rows = []
    for _, row in df.iterrows():
        identifier = clean(row.get(id_column))
        if not identifier:
            continue

        row_source_key = resolve_route_source_key_for_row(
            row,
            source_key=(row.get(source_key_column) if source_key_column else source_key_value),
        )
        cell_html = []
        for column in display_columns:
            value = compact_table_value(row.get(column), max_chars=compact_limits.get(column, 70))
            width_style = ""
            if column in column_widths:
                width_style = (
                    f" style=\"width:{column_widths[column]};max-width:{column_widths[column]};\""
                )
            full_value = escape(sanitize_display_text(column, row.get(column)))
            if column in {"?곸꽭留곹겕", "detail_link"}:
                raw_link = clean(row.get(column))
                if raw_link:
                    cell_html.append(
                        '<td class="list-link-cell"{style}><a class="list-link-out" href="{href}" title="{title}" target="_blank" rel="noopener noreferrer">?먮Ц</a></td>'.format(
                            style=width_style,
                            href=escape(raw_link, quote=True),
                            title=full_value,
                        )
                    )
                else:
                    cell_html.append(
                        '<td class="list-link-cell"{style}><span class="list-cell-empty">-</span></td>'.format(
                            style=width_style,
                        )
                    )
                continue
            if column in internal_link_columns:
                cell_html.append(
                    '<td class="list-title-cell"{style} title="{title}"><span class="list-cell-text">{value}</span></td>'.format(
                        style=width_style,
                        title=full_value,
                        value=escape(clean(value)),
                    )
                )
                continue
            cell_html.append(
                '<td{style} title="{title}"><span class="list-cell-text">{value}</span></td>'.format(
                    style=width_style,
                    title=full_value,
                    value=escape(clean(value)),
                )
            )
        body_rows.append(f"<tr>{''.join(cell_html)}</tr>")

    table_html = f"""
    <div class="list-table-wrap">
      <table class="list-table">
        <thead><tr>{header_html}</tr></thead>
        <tbody>{''.join(body_rows)}</tbody>
      </table>
    </div>
    """
    st.markdown(table_html, unsafe_allow_html=True)


def get_row_by_column_value(df: pd.DataFrame, column: str, value: str) -> dict | None:
    if df.empty or column not in df.columns or not clean(value):
        return None

    matched = df[df[column].fillna("").astype(str).str.strip().eq(clean(value))]
    if matched.empty:
        return None

    return matched.iloc[0].to_dict()


def first_non_empty(row: dict, *keys: str) -> str:
    for key in keys:
        value = clean(row.get(key))
        if value:
            return value
    return ""


def _normalize_display_title_key(value: object) -> str:
    text = clean(value).lower()
    text = re.sub(r"\.(pdf|hwpx|hwp|zip|docx?)$", "", text, flags=re.IGNORECASE)
    text = text.replace("_", " ")
    text = re.sub(r"[\[\]\(\)]+", " ", text)
    text = re.sub(r"\s+", "", text)
    return text


def _is_bad_display_project_title(value: object, *, notice_title: object = "", file_name: object = "") -> bool:
    text = clean(value)
    if not text:
        return True

    lowered = text.lower()
    normalized = _normalize_display_title_key(text)
    file_normalized = _normalize_display_title_key(file_name)
    notice_normalized = _normalize_display_title_key(notice_title)
    generic_titles = {"", "?ъ뾽紐?, "怨쇱젣紐?, "rfp", "rfp?쒕ぉ", "?ъ뾽紐꿹fp紐낃낵?쒖닔"}
    if normalized in generic_titles:
        return True
    if re.search(r"\.(pdf|hwpx|hwp|zip|docx?)$", lowered, flags=re.IGNORECASE):
        return True
    if file_normalized and normalized == file_normalized:
        return True
    if notice_normalized and normalized == notice_normalized:
        return True
    if "遺숈엫" in text and re.search(r"\.(pdf|hwpx|hwp|zip|docx?)", lowered, flags=re.IGNORECASE):
        return True
    return False


def choose_display_project_title(row: dict) -> str:
    notice_title = first_non_empty(row, "Notice Title", "notice_title", "怨듦퀬紐?)
    file_name = first_non_empty(row, "file_name", "File Name", "?뚯씪紐?)
    candidates = [
        "llm_project_name",
        "project_name",
        "Project",
        "?대떦 怨쇱젣紐?,
        "怨쇱젣紐?,
        "llm_rfp_title",
        "rfp_title",
        "RFP ?쒕ぉ",
    ]
    for key in candidates:
        value = clean(row.get(key))
        if not _is_bad_display_project_title(value, notice_title=notice_title, file_name=file_name):
            return value
    fallback = clean(re.sub(r"\.(pdf|hwpx|hwp|zip|docx?)$", "", file_name, flags=re.IGNORECASE))
    return notice_title or fallback or "-"


def format_dashboard_deadline_badge(period_text: object, fallback: object = "") -> str:
    period_end = extract_period_end(clean(period_text))
    if pd.notna(period_end):
        d_day = int((period_end.normalize() - pd.Timestamp.now().normalize()).days)
        if d_day > 0:
            return f"D-{d_day}"
        if d_day == 0:
            return "D-Day"
        return "留덇컧"
    return clean(fallback) or "-"


def resolve_local_file_path(row: dict) -> Path | None:
    if not row:
        return None

    for key in ["file_path", "?뚯씪寃쎈줈"]:
        candidate = clean(row.get(key))
        if not candidate:
            continue
        path = Path(candidate)
        if not path.is_absolute():
            path = (PROJECT_ROOT / candidate).resolve()
        if path.exists() and path.is_file():
            return path
    return None


def ensure_notice_analysis_fallback(row: dict, top_related: dict) -> dict:
    merged = dict(row or {})
    if not top_related:
        return merged

    fallback_map = {
        "??쒓낵?쒕챸": ["llm_project_name", "project_name", "rfp_title"],
        "??쒖텛泥쒕룄": ["llm_recommendation", "recommendation"],
        "??쒖젏??: ["llm_fit_score", "rfp_score"],
        "??쒖삁??: ["llm_total_budget_text", "total_budget_text", "llm_per_project_budget_text", "per_project_budget_text", "budget"],
        "??쒖텛泥쒖씠??: ["llm_reason", "reason"],
        "??쒗궎?뚮뱶": ["llm_keywords", "keywords"],
    }

    for target_key, source_keys in fallback_map.items():
        if clean(merged.get(target_key)):
            continue
        for source_key in source_keys:
            value = top_related.get(source_key)
            if isinstance(value, list):
                value = ", ".join([clean(x) for x in value if clean(x)])
            value = clean(value)
            if value:
                merged[target_key] = value
                break

    return merged


def find_related_opportunities_for_notice(row: dict, opportunity_df: pd.DataFrame) -> pd.DataFrame:
    if opportunity_df.empty:
        return pd.DataFrame()

    working = opportunity_df.copy()
    notice_id = clean(row.get("怨듦퀬ID"))
    if notice_id and "notice_id" in working.columns:
        notice_key = normalize_notice_id_for_match(notice_id)
        matched = working[
            working["notice_id"].apply(normalize_notice_id_for_match).eq(notice_key)
        ].copy()
        if not matched.empty:
            return matched

    notice_title = clean(row.get("怨듦퀬紐?))
    if notice_title and "怨듦퀬紐? in working.columns:
        matched = working[working["怨듦퀬紐?].fillna("").astype(str).str.strip().eq(notice_title)].copy()
        if not matched.empty:
            return matched
    if notice_title and "notice_title" in working.columns:
        matched = working[working["notice_title"].fillna("").astype(str).str.strip().eq(notice_title)].copy()
        if not matched.empty:
            return matched

    ancm_no = clean(row.get("怨듦퀬踰덊샇"))
    if ancm_no and "怨듦퀬踰덊샇" in working.columns:
        matched = working[working["怨듦퀬踰덊샇"].fillna("").astype(str).str.strip().eq(ancm_no)].copy()
        if not matched.empty:
            return matched
    if ancm_no and "ancm_no" in working.columns:
        matched = working[working["ancm_no"].fillna("").astype(str).str.strip().eq(ancm_no)].copy()
        if not matched.empty:
            return matched

    return pd.DataFrame()


def render_review_editor(
    notice_id: str,
    current_value: str,
    form_key: str,
    source_key: str = "iris",
    notice_title: str = "",
) -> None:
    if not get_bool_env("ENABLE_REVIEW_EDIT", default=True):
        st.info("怨듦컻 諛고룷?먯꽌??寃??щ? ?섏젙??鍮꾪솢?깊솕?섏뼱 ?덉뒿?덈떎.")
        return

    st.markdown("### 寃??щ? ?섏젙")
    normalized_value = clean(current_value)
    options = REVIEW_OPTIONS.copy()
    if normalized_value and normalized_value not in options:
        options.append(normalized_value)

    default_index = options.index(normalized_value) if normalized_value in options else 0

    with st.form(form_key):
        review_value = st.selectbox("寃??щ?", options=options, index=default_index)
        submitted = st.form_submit_button("???)

        if submitted:
            try:
                if is_user_scoped_operations_enabled():
                    upsert_user_review_status(
                        user_id=get_current_operation_scope_key(),
                        source_key=source_key,
                        notice_id=notice_id,
                        notice_title=notice_title,
                        review_status=review_value,
                    )
                elif clean(source_key) == "tipa":
                    update_mss_review_status(notice_id, review_value)
                elif clean(source_key) == "nipa":
                    update_nipa_review_status(notice_id, review_value)
                else:
                    update_notice_review_status(notice_id, review_value)
                st.success("寃??щ?瑜???ν뻽?듬땲??")
                st.rerun()
            except Exception as exc:
                st.error(f"???ㅽ뙣: {exc}")


def save_review_status(
    *,
    notice_id: str,
    review_status: str,
    source_key: str = "iris",
    notice_title: str = "",
) -> None:
    if is_user_scoped_operations_enabled():
        upsert_user_review_status(
            user_id=get_current_operation_scope_key(),
            source_key=source_key,
            notice_id=notice_id,
            notice_title=notice_title,
            review_status=review_status,
        )
    elif clean(source_key) == "tipa":
        update_mss_review_status(notice_id, review_status)
    elif clean(source_key) == "nipa":
        update_nipa_review_status(notice_id, review_status)
    else:
        update_notice_review_status(notice_id, review_status)




def render_notice_comments(row: dict, section_key: str) -> None:
    notice_id = clean(row.get("怨듦퀬ID") or row.get("notice_id"))
    notice_title = clean(row.get("怨듦퀬紐?) or row.get("notice_title"))
    source_key = resolve_notice_source_key(row)
    author_id = clean(get_current_user_label()) or clean(get_current_user_id()) or clean(get_env("DEFAULT_COMMENT_AUTHOR")) or "익명"
    save_feedback = ""

    st.markdown('<div class="detail-section-title">댓글</div>', unsafe_allow_html=True)
    if not notice_id:
        st.caption("공고ID가 없어 댓글을 연결할 수 없습니다.")
        return

    with st.form(f"{section_key}_comment_form", clear_on_submit=True):
        st.caption(f"작성자: {author_id}")
        comment = st.text_area("댓글", key=f"{section_key}_comment_text", height=120, placeholder="이 공고에 대한 메모나 검토 의견을 남겨주세요.")
        submitted = st.form_submit_button("댓글 저장")
        if submitted:
            try:
                append_notice_comment(
                    source_key=source_key,
                    notice_id=notice_id,
                    notice_title=notice_title,
                    author=author_id,
                    comment=comment,
                )
                save_feedback = "댓글을 저장했습니다."
            except Exception as exc:
                st.error(f"댓글 저장 실패: {exc}")

    try:
        comments_df = load_notice_comments()
    except Exception as exc:
        st.warning(f"댓글 이력을 불러오지 못했습니다: {exc}")
        comments_df = _empty_comment_dataframe()

    matched = filter_notice_comments(comments_df, source_key=source_key, notice_id=notice_id)
    if save_feedback:
        st.success(save_feedback)

    if matched.empty:
        st.caption("아직 등록된 댓글이 없습니다.")
        return

    st.caption(f"댓글 이력 {len(matched)}건")
    for _, comment_row in matched.iterrows():
        comment_id = clean(comment_row.get("comment_id"))
        created_at = clean(comment_row.get("created_at"))
        author = clean(comment_row.get("nickname")) or clean(comment_row.get("author")) or "익명"
        comment_text = clean(comment_row.get("content")) or clean(comment_row.get("comment"))
        delete_key = f"{section_key}_delete_comment_{comment_id}"
        with st.container(border=True):
            st.caption(" · ".join([value for value in [created_at, author] if value]))
            st.write(comment_text)
            if comment_id and can_delete_comment(comment_row, get_current_user_id()) and st.button("댓글 삭제", key=delete_key):
                try:
                    delete_notice_comment(comment_id, get_current_user_id())
                    st.success("댓글을 삭제했습니다.")
                    st.rerun()
                except Exception as exc:
                    st.error(f"댓글 삭제 실패: {exc}")


def render_notice_detail_from_row(row: dict, opportunity_df: pd.DataFrame) -> None:
    render_notice_detail_page_module(st, row, opportunity_df, api=sys.modules[__name__])

def render_pending_detail_from_row(row: dict) -> None:
    if not row:
        st.info("?쒖떆??묒닔?덉젙 怨듦퀬媛 ?놁뒿?덈떎.")
        return

    render_detail_header(
        title=clean(row.get("怨듦퀬紐?)),
        kicker="Pending Notice Detail",
        chips=[
            (clean(row.get("怨듦퀬?곹깭")), "accent"),
            (clean(row.get("?꾨Ц湲곌?")), "neutral"),
            (clean(row.get("怨듦퀬?쇱옄")), "neutral"),
            (f"寃?? {clean(row.get('寃??щ?') or '誘몄??)}", "neutral"),
        ],
    )

    top_left, top_right = st.columns([2, 1])
    with top_left:
        render_detail_card(
            "?묒닔?덉젙 怨듦퀬 ?뺣낫",
            [
                ("怨듦퀬紐?, row.get("怨듦퀬紐?)),
                ("?묒닔湲곌컙", row.get("?묒닔湲곌컙")),
                ("怨듦퀬?쇱옄", row.get("怨듦퀬?쇱옄")),
                ("?꾨Ц湲곌?", row.get("?꾨Ц湲곌?")),
                ("?뚭?遺泥?, row.get("?뚭?遺泥?)),
                ("怨듦퀬踰덊샇", row.get("怨듦퀬踰덊샇")),
            ],
        )
    with top_right:
        render_detail_card(
            "?앸퀎 ?뺣낫",
            [
                ("怨듦퀬ID", row.get("怨듦퀬ID")),
                ("?곹깭??, row.get("?곹깭??)),
                ("怨듦퀬?곹깭", row.get("怨듦퀬?곹깭")),
                ("?꾩옱 怨듦퀬?щ?", row.get("is_current")),
                ("寃??щ?", row.get("寃??щ?")),
            ],
        )

    action_left, action_right = st.columns([1, 2])
    with action_left:
        detail_link = resolve_external_detail_link(row)
        if detail_link:
            st.link_button("IRIS ?곸꽭 諛붾줈媛湲?, detail_link, use_container_width=True)
    with action_right:
        st.caption("?묒닔?덉젙 怨듦퀬??蹂꾨룄 master ?쒗듃 湲곗??쇰줈 議고쉶?⑸땲??")

    st.markdown('<div class="detail-section-title">怨듦퀬 硫붾え</div>', unsafe_allow_html=True)
    render_detail_card(
        "?댁쁺 ?뺣낫",
        [
            ("寃??щ?", row.get("寃??щ?")),
            ("怨듦퀬?곹깭", row.get("怨듦퀬?곹깭")),
            ("?묒닔湲곌컙", row.get("?묒닔湲곌컙")),
        ],
    )
    render_notice_comments(row, section_key=f"pending_{clean(row.get('怨듦퀬ID'))}")


def _score_value(value: object) -> int:
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return 0


def _badge_class(value: object, *, kind: str = "recommendation") -> str:
    text = clean(value).lower()
    if kind == "score":
        return "badge-blue"
    if kind == "deadline":
        return "badge-rose"
    if any(marker in text for marker in ["鍮꾩텛泥?, "誘몄텛泥?, "not recommend", "not recommended", "reject"]):
        return "badge-slate"
    if "異붿쿇" in text or "recommend" in text:
        return "badge-green"
    if "寃?? in text or "hold" in text or "蹂대쪟" in text:
        return "badge-amber"
    if "留덇컧" in text or "closed" in text:
        return "badge-rose"
    return "badge-slate"


def _pill_html(text: object, *, kind: str = "recommendation", base_class: str = "queue-badge") -> str:
    safe_text = clean(text)
    if not safe_text:
        return ""
    return f'<span class="{base_class} {_badge_class(safe_text, kind=kind)}">{escape(safe_text)}</span>'


def _queue_row_context(row: dict[str, object] | pd.Series) -> dict[str, str]:
    row_dict = row.to_dict() if isinstance(row, pd.Series) else dict(row or {})
    recommendation = first_non_empty(row_dict, "recommendation", "llm_recommendation", "Recommendation") or "寃??
    score = _score_value(first_non_empty(row_dict, "llm_fit_score", "rfp_score", "?먯닔", "Score"))
    period = first_non_empty(row_dict, "notice_period", "period", "Period", "?묒닔湲곌컙", "?붿껌湲곌컙")
    deadline = format_dashboard_deadline_badge(period, first_non_empty(row_dict, "status", "Status"))
    budget = extract_budget_summary(first_non_empty(row_dict, "budget", "Budget", "llm_total_budget_text", "total_budget_text")) or "-"
    agency = first_non_empty(row_dict, "agency", "Agency", "?꾨Ц湲곌?", "?꾨Ц湲곌?紐?) or "-"
    ministry = first_non_empty(row_dict, "ministry", "Ministry", "遺泥?, "二쇰Т遺泥?) or "-"
    project = choose_display_project_title(row_dict)
    notice = first_non_empty(row_dict, "notice_title", "Notice Title", "怨듦퀬紐?)
    reason = first_non_empty(row_dict, "llm_reason", "reason", "Reason", "llm_concept_and_development", "concept_and_development")
    risk = first_non_empty(row_dict, "llm_support_need", "support_need", "Support Need", "llm_eligibility", "eligibility", "Eligibility", "evidence")
    source_label = first_non_empty(row_dict, "Source", "source_site") or "-"
    status = first_non_empty(row_dict, "Status", "status", "rcve_status", "怨듦퀬?곹깭") or "-"
    review = first_non_empty(row_dict, "Review", "review_status", "寃?좎뿬遺") or "誘멸??
    registered_at = first_non_empty(row_dict, "Date", "ancm_de", "怨듦퀬?쇱옄", "registered_at") or "-"
    file_name = first_non_empty(row_dict, "file_name", "File Name", "rfp_title") or "-"
    archive_reason_label = derive_archive_reason_label_for_app(row_dict)
    return {
        "recommendation": recommendation,
        "score": str(score) if score else "-",
        "deadline": deadline or "-",
        "budget": budget,
        "agency": agency,
        "ministry": ministry,
        "project": project or "-",
        "notice": notice or "-",
        "reason": truncate_text(reason or "-", max_chars=220),
        "risk": truncate_text(risk or "-", max_chars=180),
        "period": period or "-",
        "source": source_label,
        "status": status,
        "review": review,
        "registered_at": registered_at,
        "file_name": file_name,
        "archive_reason_label": archive_reason_label,
    }


def build_queue_recommendation_options(values: pd.Series) -> list[str]:
    options = []
    for value in values.dropna().astype(str).tolist():
        normalized = clean(value)
        if not normalized or normalized == "-" or "寃?? in normalized:
            continue
        options.append(normalized)
    return sorted(set(options))


def build_queue_status_options(values: pd.Series) -> list[str]:
    options = []
    for value in values.dropna().astype(str).tolist():
        normalized = normalize_notice_status_label(value) or clean(value)
        if not normalized or normalized == "-":
            continue
        options.append(normalized)
    unique_options = sorted(set(options))
    if "留덇컧" not in unique_options:
        unique_options.append("留덇컧")
    return unique_options


def filter_queue_working_frame(
    working: pd.DataFrame,
    *,
    selected_recommendation: list[str],
    selected_status: list[str],
    archive: bool,
) -> pd.DataFrame:
    if working.empty:
        return working

    filtered = working.copy()
    if selected_recommendation:
        filtered = filtered[filtered["_queue_recommendation"].isin(selected_recommendation)]
    if archive:
        filtered = filtered[filtered["_queue_is_closed"]]
    elif not selected_status:
        filtered = filtered[~filtered["_queue_is_closed"]]
    if selected_status:
        filtered = filtered[filtered["_queue_status"].isin(selected_status)]
    return filtered


def _build_queue_filter_frame(rows: pd.DataFrame) -> pd.DataFrame:
    working = ensure_opportunity_row_ids(rows.copy())
    if working.empty:
        return working

    contexts: list[dict[str, str]] = []
    deadline_sorts: list[pd.Timestamp | pd.NaT] = []
    open_flags: list[bool] = []
    today = pd.Timestamp.now().normalize()

    for _, row in working.iterrows():
        ctx = _queue_row_context(row)
        contexts.append(ctx)
        deadline_value = extract_period_end(
            first_non_empty(row, "notice_period", "period", "?묒닔湲곌컙", "?붿껌湲곌컙")
        )
        deadline_sorts.append(deadline_value)
        status_text = clean(ctx["status"])
        is_open = False
        if status_text:
            is_open = "留덇컧" not in status_text and ("?묒닔" in status_text or "吏꾪뻾" in status_text or "?덉젙" in status_text)
        if not is_open and pd.notna(deadline_value):
            is_open = deadline_value >= today
        open_flags.append(bool(is_open))

    working["_queue_recommendation"] = [clean(ctx["recommendation"]) or "-" for ctx in contexts]
    working["_queue_score"] = [clean(ctx["score"]) or "-" for ctx in contexts]
    working["_queue_sort_score"] = to_numeric_column(
        series_from_candidates(working, ["llm_fit_score", "rfp_score", "?먯닔", "Score"])
    )
    working["_queue_deadline"] = [clean(ctx["deadline"]) or "-" for ctx in contexts]
    working["_queue_budget"] = [clean(ctx["budget"]) or "-" for ctx in contexts]
    working["_queue_agency"] = [clean(ctx["agency"]) or "-" for ctx in contexts]
    working["_queue_ministry"] = [clean(ctx["ministry"]) or "-" for ctx in contexts]
    working["_queue_notice"] = [clean(ctx["notice"]) or "-" for ctx in contexts]
    working["_queue_source"] = [clean(ctx["source"]) or "-" for ctx in contexts]
    working["_queue_status"] = [
        normalize_notice_status_label(ctx["status"]) or clean(ctx["status"]) or "-"
        for ctx in contexts
    ]
    working["_queue_period"] = [clean(ctx["period"]) or "-" for ctx in contexts]
    working["_queue_archive_reason"] = [clean(ctx["archive_reason_label"]) or "-" for ctx in contexts]
    working["_queue_deadline_sort"] = deadline_sorts
    working["_queue_is_open"] = open_flags
    working["_queue_is_closed"] = build_opportunity_archive_mask(working)
    working["_queue_project_sort"] = series_from_candidates(
        working,
        ["project_name", "llm_project_name", "????⑥눘?ｏ쭗?"],
    ).fillna("").astype(str).str.strip()
    return working


def _render_rfp_queue_list(rows: pd.DataFrame, *, page_key: str) -> None:
    if rows.empty:
        st.info("?쒖떆??RFP媛 ?놁뒿?덈떎.")
        return

    items: list[str] = []
    archive_mode = "archive" in clean(page_key).lower()
    for _, row in rows.iterrows():
        ctx = _queue_row_context(row)
        badges = "".join(
            [
                _pill_html(ctx["recommendation"]),
                _pill_html(ctx["score"], kind="score"),
                _pill_html(ctx["deadline"], kind="deadline"),
                _pill_html(ctx["archive_reason_label"], kind="archive") if archive_mode else "",
            ]
        )
        archive_reason_html = (
            f'<div class="queue-list-card-reason muted">蹂닿? ?ъ쑀: {escape(ctx["archive_reason_label"])}</div>'
            if archive_mode and clean(ctx["archive_reason_label"])
            else ""
        )
        items.append(
            (
                '<div class="queue-card queue-list-card">'
                f'<div class="queue-badge-row">{badges}</div>'
                f'<div class="queue-list-card-title">{escape(truncate_text(ctx["project"], max_chars=96))}</div>'
                f'<div class="queue-list-card-subtitle">{escape(truncate_text(ctx["notice"], max_chars=120))}</div>'
                '<div class="queue-list-card-meta">'
                f'<div class="queue-list-card-meta-item"><div class="queue-list-card-meta-label">?꾨Ц湲곌?</div><div class="queue-list-card-meta-value">{escape(ctx["agency"])}</div></div>'
                f'<div class="queue-list-card-meta-item"><div class="queue-list-card-meta-label">吏?먭툑</div><div class="queue-list-card-meta-value">{escape(ctx["budget"])}</div></div>'
                f'<div class="queue-list-card-meta-item"><div class="queue-list-card-meta-label">怨듦퀬 ?곹깭</div><div class="queue-list-card-meta-value">{escape(ctx["status"])}</div></div>'
                '</div>'
                f'<div class="queue-list-card-reason">{escape(ctx["reason"])}</div>'
                f'{archive_reason_html}'
                '</div>'
            )
        )

    st.markdown(f'<div class="queue-list-shell">{"".join(items)}</div>', unsafe_allow_html=True)


def render_opportunity_detail_from_row(row: dict) -> None:
    render_rfp_detail_page_module(st, row, api=sys.modules[__name__])

def render_summary_detail_from_row(row: dict, opportunity_df: pd.DataFrame) -> None:
    if not row:
        st.info("?쒖떆??붿빟 怨듦퀬媛 ?놁뒿?덈떎.")
        return

    render_detail_header(
        title=clean(row.get("怨듦퀬紐?)),
        kicker="Summary Detail",
        chips=[
            (clean(row.get("??쒖텛泥쒕룄")), "accent"),
            (clean(row.get("異붿쿇??諛??먯닔")), "neutral"),
            (clean(row.get("?꾨Ц湲곌?")), "neutral"),
            (clean(row.get("怨듦퀬?쇱옄")), "neutral"),
        ],
    )

    top_left, top_right = st.columns([2, 1])
    with top_left:
        render_detail_card(
            "???怨쇱젣 遺꾩꽍",
            [
                ("?대떦 怨쇱젣紐?, row.get("?대떦 怨쇱젣紐?)),
                ("異붿쿇??諛??먯닔", row.get("異붿쿇??諛??먯닔")),
                ("?덉궛", row.get("?덉궛")),
                ("怨쇱젣??, row.get("怨쇱젣??)),
                ("臾몄꽌??, row.get("臾몄꽌??)),
            ],
        )
    with top_right:
        render_detail_card(
            "怨듦퀬 ?앸퀎 ?뺣낫",
            [
                ("怨듦퀬ID", row.get("怨듦퀬ID")),
                ("怨듦퀬踰덊샇", row.get("怨듦퀬踰덊샇")),
                ("?꾨Ц湲곌?", row.get("?꾨Ц湲곌?")),
                ("?뚭?遺泥?, row.get("?뚭?遺泥?)),
                ("寃??щ?", row.get("寃??щ?")),
            ],
        )

    action_left, action_right = st.columns([1, 2])
    with action_left:
        detail_link = resolve_external_detail_link(row)
        if detail_link:
            st.link_button("IRIS ?곸꽭 諛붾줈媛湲?, detail_link, use_container_width=True)
    with action_right:
        st.caption("Summary????怨쇱젣 湲곗??쇰줈 怨듦퀬瑜??붿빟?댁꽌 蹂댁뿬以띾땲??")

    related = pd.DataFrame()
    if not opportunity_df.empty and "notice_id" in opportunity_df.columns:
        notice_key = normalize_notice_id_for_match(row.get("怨듦퀬ID"))
        related = opportunity_df[
            opportunity_df["notice_id"].apply(normalize_notice_id_for_match).eq(notice_key)
        ].copy()
        if not related.empty:
            related = related.sort_values(
                by=["rfp_score", "project_name"],
                ascending=[False, True],
                na_position="last",
            )
    top_related = related.iloc[0].to_dict() if not related.empty else {}

    st.markdown('<div class="detail-section-title">寃??곹깭</div>', unsafe_allow_html=True)
    left, right = st.columns(2)
    with left:
        render_detail_card(
            "怨듦퀬 ?뺣낫",
            [
                ("怨듦퀬?쇱옄", row.get("怨듦퀬?쇱옄")),
                ("怨듦퀬?곹깭", row.get("怨듦퀬?곹깭")),
                ("?묒닔湲곌컙", row.get("?묒닔湲곌컙")),
                ("is_current", row.get("is_current")),
            ],
        )
    with right:
        render_review_editor(
            notice_id=clean(row.get("怨듦퀬ID")),
            current_value=clean(row.get("寃??щ?")),
            form_key=f"summary_review_form_{clean(row.get('怨듦퀬ID'))}",
            notice_title=clean(row.get("怨듦퀬紐?)),
        )

    st.markdown('<div class="detail-section-title">???遺꾩꽍 ?붿빟</div>', unsafe_allow_html=True)
    render_detail_card(
        "怨쇱젣 遺꾩꽍",
        [
            ("異붿쿇 ?댁쑀", first_non_empty(top_related, "llm_reason", "reason", "??쒖텛泥쒖씠??)),
            (
                "媛쒕뀗 諛?媛쒕컻 ?댁슜",
                first_non_empty(
                    top_related,
                    "llm_concept_and_development",
                    "concept_and_development",
                    "媛쒕뀗 諛?媛쒕컻 ?댁슜",
                ),
            ),
            (
                "吏?먰븘?붿꽦(怨쇱젣 諛곌꼍)",
                first_non_empty(
                    top_related,
                    "llm_support_necessity",
                    "support_necessity",
                    "吏?먰븘?붿꽦(怨쇱젣 諛곌꼍)",
                    "llm_technical_background",
                    "technical_background",
                ),
            ),
            (
                "?쒖슜遺꾩빞",
                first_non_empty(
                    top_related,
                    "llm_application_field",
                    "application_field",
                    "?쒖슜遺꾩빞",
                ),
            ),
            (
                "吏?먭린媛?諛??덉궛쨌異붿쭊泥닿퀎",
                first_non_empty(
                    top_related,
                    "llm_support_plan",
                    "support_plan",
                    "吏?먭린媛?諛??덉궛쨌異붿쭊泥닿퀎",
                ),
            ),
            ("??쒓낵?쒕챸", first_non_empty(top_related, "llm_project_name", "project_name", "??쒓낵?쒕챸")),
            ("??쒖삁??, first_non_empty(top_related, "llm_total_budget_text", "total_budget_text", "budget", "??쒖삁??)),
            ("??쒗궎?뚮뱶", first_non_empty(top_related, "llm_keywords", "keywords", "??쒗궎?뚮뱶")),
        ],
    )

    render_notice_comments(row, section_key=f"summary_{clean(row.get('怨듦퀬ID'))}")


def render_notice_page(notice_df: pd.DataFrame, opportunity_df: pd.DataFrame) -> None:
    render_notice_page_with_scope(
        notice_df,
        opportunity_df,
        page_key="notice",
        title="吏꾪뻾 怨듦퀬",
        default_status_scope="?묒닔以?,
        current_only_default=True,
    )


def build_app_datasets(
    *,
    notice_master_df: pd.DataFrame,
    notice_current_df: pd.DataFrame,
    pending_df: pd.DataFrame,
    notice_archive_df: pd.DataFrame,
    opportunity_df: pd.DataFrame,
    opportunity_archive_df: pd.DataFrame,
    summary_df: pd.DataFrame,
    errors_df: pd.DataFrame,
) -> dict[str, pd.DataFrame]:
    notice_view_base_df = combine_notice_frames(notice_current_df, pending_df, notice_archive_df)
    opportunity_all_df = combine_opportunity_frames(opportunity_df, opportunity_archive_df)
    notice_view_df = merge_notice_with_analysis(notice_view_base_df, opportunity_all_df)
    return {
        "notice": notice_view_base_df,
        "notice_master": notice_master_df,
        "notice_current": notice_current_df,
        "notice_view": notice_view_df,
        "pending": pending_df,
        "notice_archive": notice_archive_df,
        "opportunity": opportunity_df,
        "opportunity_all": opportunity_all_df,
        "opportunity_archive": opportunity_archive_df,
        "summary": summary_df,
        "errors": errors_df,
    }


@st.cache_data(ttl=300, show_spinner=False)
def load_app_datasets(
    notice_master_sheet_name: str,
    notice_current_sheet_name: str,
    notice_pending_sheet_name: str,
    notice_archive_sheet_name: str,
    opportunity_sheet_name: str,
    opportunity_archive_sheet_name: str,
    summary_sheet_name: str,
    error_sheet_name: str,
) -> dict[str, pd.DataFrame]:
    notice_master_df = filter_notice_dataframe_by_source(
        enrich_notice_df(load_sheet_as_dataframe(notice_master_sheet_name)),
        "IRIS",
    )
    notice_current_df = filter_notice_dataframe_by_source(
        enrich_notice_df(load_optional_sheet_as_dataframe(notice_current_sheet_name)),
        "IRIS",
    )
    pending_df = filter_notice_dataframe_by_source(
        enrich_notice_df(load_optional_sheet_as_dataframe(notice_pending_sheet_name)),
        "IRIS",
    )
    notice_archive_df = filter_notice_dataframe_by_source(
        enrich_notice_df(load_optional_sheet_as_dataframe(notice_archive_sheet_name)),
        "IRIS",
    )
    opportunity_df = enrich_opportunity_df(load_optional_sheet_as_dataframe(opportunity_sheet_name))
    opportunity_df = enrich_opportunity_with_notice_meta(opportunity_df, notice_master_df)
    opportunity_archive_df = enrich_opportunity_df(load_optional_sheet_as_dataframe(opportunity_archive_sheet_name))
    opportunity_archive_df = enrich_opportunity_with_notice_meta(opportunity_archive_df, notice_master_df)
    summary_df = enrich_summary_df(load_optional_sheet_as_dataframe(summary_sheet_name))
    summary_df = enrich_summary_with_notice_meta(summary_df, notice_master_df)
    errors_df = enrich_error_df(load_optional_sheet_as_dataframe(error_sheet_name))
    return build_app_datasets(
        notice_master_df=notice_master_df,
        notice_current_df=notice_current_df,
        pending_df=pending_df,
        notice_archive_df=notice_archive_df,
        opportunity_df=opportunity_df,
        opportunity_archive_df=opportunity_archive_df,
        summary_df=summary_df,
        errors_df=errors_df,
    )


@st.cache_data(ttl=1800, show_spinner=False)
def build_source_datasets() -> dict[str, object]:
    mss_current_df, mss_current_origin = load_mss_notice_df()
    mss_past_df, mss_past_origin = load_mss_past_df()
    nipa_current_df, nipa_current_origin = load_nipa_notice_df()
    nipa_past_df, nipa_past_origin = load_nipa_past_df()
    return {
        "mss_current": mss_current_df,
        "mss_current_origin": mss_current_origin,
        "mss_past": mss_past_df,
        "mss_past_origin": mss_past_origin,
        "mss_opportunity": load_mss_opportunity_df(),
        "mss_opportunity_archive": load_mss_opportunity_archive_df(),
        "nipa_current": nipa_current_df,
        "nipa_current_origin": nipa_current_origin,
        "nipa_past": nipa_past_df,
        "nipa_past_origin": nipa_past_origin,
        "nipa_opportunity": load_nipa_opportunity_df(),
        "nipa_opportunity_archive": load_nipa_opportunity_archive_df(),
    }


def combine_notice_frames(*frames: pd.DataFrame) -> pd.DataFrame:
    available_frames = [frame.copy() for frame in frames if frame is not None and not frame.empty]
    if not available_frames:
        return pd.DataFrame()

    combined = pd.concat(available_frames, ignore_index=True)
    if "怨듦퀬ID" in combined.columns:
        combined = combined.drop_duplicates(subset=["怨듦퀬ID"], keep="first")
    elif "notice_id" in combined.columns:
        combined = combined.drop_duplicates(subset=["notice_id"], keep="first")
    return combined


def combine_opportunity_frames(*frames: pd.DataFrame) -> pd.DataFrame:
    available_frames = [frame.copy() for frame in frames if frame is not None and not frame.empty]
    if not available_frames:
        return pd.DataFrame()

    combined = pd.concat(available_frames, ignore_index=True)
    for key_columns in [
        ["document_id"],
        ["_row_id"],
        ["notice_id", "project_name"],
        ["notice_id", "rfp_title"],
    ]:
        if all(column in combined.columns for column in key_columns):
            non_empty_mask = combined[key_columns].fillna("").astype(str).apply(
                lambda col: col.str.strip()
            )
            valid_rows = non_empty_mask.ne("").all(axis=1)
            if valid_rows.any():
                deduped = combined[valid_rows].drop_duplicates(subset=key_columns, keep="first")
                remainder = combined[~valid_rows]
                return pd.concat([deduped, remainder], ignore_index=True)
    return combined


def filter_notice_dataframe_by_source(df: pd.DataFrame, source_site: str) -> pd.DataFrame:
    if df is None:
        return pd.DataFrame()
    if df.empty or "source_site" not in df.columns:
        return df.copy()
    normalized_source = clean(source_site).upper()
    return df[
        df["source_site"].fillna("").astype(str).str.strip().str.upper().eq(normalized_source)
    ].copy()


def render_notice_page_with_scope(
    source_df: pd.DataFrame,
    opportunity_df: pd.DataFrame,
    *,
    page_key: str,
    title: str,
    default_status_scope: str,
    current_only_default: bool,
    archive: bool = False,
) -> None:
    subtitle = "?섏쭛??怨듦퀬瑜??곹깭? 湲곌? 湲곗??쇰줈 ?뺣━??遊낅땲??"
    if archive:
        subtitle = "醫낅즺?섏뿀嫄곕굹 蹂닿? ??곸쑝濡?遺꾨쪟??怨듦퀬瑜?紐⑥븘 遊낅땲??"
    elif default_status_scope == "?덉젙":
        subtitle = "?덉젙 怨듦퀬? ?묒닔 ?덉젙 嫄댁쓣 癒쇱? ?뺤씤?⑸땲??"
    current_view, selected_notice_id = get_route_state(page_key)
    if current_view == "detail":
        selected_row = get_row_by_column_value(source_df, "怨듦퀬ID", selected_notice_id)
        action_col, info_col = st.columns([1, 5])
        with action_col:
            if st.button("RFP Dashboard濡?, key=f"{page_key}_back_to_dashboard", use_container_width=True):
                navigate_to_route("dashboard", "dashboard")
        with info_col:
            st.markdown('<div class="page-note">RFP 異붿쿇 ?붾㈃?먯꽌 ?곌껐??怨듦퀬 ?곸꽭瑜??뺤씤?섎뒗 ?붾㈃?낅땲??</div>', unsafe_allow_html=True)
        render_notice_detail_from_row(selected_row, opportunity_df)
        return

    render_page_header(title, subtitle, eyebrow="Notice")

    filtered = source_df.copy()
    filtered = filter_archived_notice_rows(filtered) if archive else filter_current_notice_rows(filtered)
    filtered["?ъ뾽鍮?] = series_from_candidates(filtered, ["?ъ뾽鍮?, "??쒖삁??]).apply(extract_budget_summary)
    search_text, current_only, status_scope = render_notice_filter_sidebar(
        page_key,
        current_only_default=current_only_default,
        status_default=default_status_scope,
        show_current_only=not archive,
        show_status_scope=not archive,
    )
    if current_only and "is_current" in filtered.columns:
        filtered = filtered[filtered["is_current"].fillna("").eq("Y")]
    if not archive:
        filtered = filter_notice_status_scope(filtered, status_scope)
    filtered = apply_multiselect_filter(filtered, "?꾨Ц湲곌?", "?꾨Ц湲곌?", f"{page_key}_agency")
    filtered = apply_multiselect_filter(filtered, "?뚭?遺泥?, "?뚭?遺泥?, f"{page_key}_ministry")
    filtered = apply_multiselect_filter(filtered, "寃??щ?", "寃??щ?", f"{page_key}_review")

    filtered = filtered[
        build_contains_mask(
            filtered,
            ["怨듦퀬紐?, "怨듦퀬踰덊샇", "?꾨Ц湲곌?", "?뚭?遺泥?, "怨듦퀬ID", "??쒓낵?쒕챸"],
            search_text,
        )
    ]

    render_metrics(
        [
            ("怨듦퀬 ??, str(len(filtered))),
            ("?꾩옱 怨듦퀬", str(int((filtered["is_current"] == "Y").sum()) if "is_current" in filtered.columns else 0)),
            ("?꾨Ц湲곌? ??, str(filtered["?꾨Ц湲곌?"].nunique() if "?꾨Ц湲곌?" in filtered.columns else 0)),
            ("寃??꾨즺", str(int(filtered["寃??щ?"].fillna("").ne("").sum()) if "寃??щ?" in filtered.columns else 0)),
        ]
    )

    render_section_label("Notice List")
    st.markdown(
        f'<div class="page-note">怨듦퀬紐??먮뒗 怨쇱젣紐낆쓣 ?대┃?섎㈃ ?곸꽭 怨듦퀬? ?곌껐 RFP瑜??④퍡 ?뺤씤??덉뒿?덈떎. ?꾩옱 {len(filtered)}嫄?/div>',
        unsafe_allow_html=True,
    )
    render_clickable_table(
        filtered,
        NOTICE_PREFERRED_COLUMNS,
        page_key=page_key,
        id_column="怨듦퀬ID",
    )


def render_opportunity_page_aligned(
    df: pd.DataFrame,
    *,
    page_key: str | None = None,
    title: str | None = None,
    archive: bool = False,
    all_df: pd.DataFrame | None = None,
) -> None:
    page_key = page_key or ("opportunity_archive" if archive else "opportunity")
    title = title or ("RFP Archive" if archive else "RFP Queue")
    subtitle = "?ъ뾽怨듦퀬 ??吏??媛?ν븳 RFP瑜?異붿쿇?⑸땲??"
    if archive:
        subtitle = "蹂닿? ??곸쑝濡?遺꾨쪟??RFP 遺꾩꽍 寃곌낵瑜?媛蹂띻쾶 ?먯깋??덉뒿?덈떎."
    render_page_header(title, subtitle, eyebrow="RFP")

    source_df = ensure_opportunity_row_ids(df)
    working_source_df = ensure_opportunity_row_ids(all_df) if all_df is not None and not all_df.empty else source_df
    if archive:
        working_source_df = filter_archived_opportunity_rows(working_source_df)
    if working_source_df.empty:
        st.info("?쒖떆??RFP媛 ?놁뒿?덈떎.")
        return

    working = _build_queue_filter_frame(working_source_df)
    recommendation_options = build_queue_recommendation_options(working["_queue_recommendation"])
    status_options = build_queue_status_options(working["_queue_status"])

    filter_cols = st.columns(2)
    with filter_cols[0]:
        selected_recommendation = st.multiselect(
            "異붿쿇 ?곹깭",
            options=recommendation_options,
            default=[],
            key=f"{page_key}_filter_recommendation_aligned",
            placeholder="?꾩껜",
        )
    with filter_cols[1]:
        selected_status = st.multiselect(
            "怨듦퀬 ?곹깭",
            options=status_options,
            default=[],
            key=f"{page_key}_filter_status_aligned",
            placeholder="?꾩껜",
        )

    filtered = filter_queue_working_frame(
        working,
        selected_recommendation=selected_recommendation,
        selected_status=selected_status,
        archive=archive,
    )
    if filtered.empty:
        st.info("寃??議곌굔??留욌뒗 RFP媛 ?놁뒿?덈떎.")
        return

    filtered = filtered.sort_values(
        by=["_queue_sort_score", "_queue_deadline_sort", "_queue_project_sort"],
        ascending=[False, True, True],
        na_position="last",
    )

    render_metrics(
        [
            ("RFP Count", str(len(filtered))),
            ("Recommended", str(int((filtered["recommendation"] == "異붿쿇").sum()) if "recommendation" in filtered.columns else 0)),
            ("Avg Score", safe_mean(filtered["rfp_score"]) if "rfp_score" in filtered.columns and len(filtered) > 0 else "-"),
            ("Notice Count", str(filtered["notice_id"].nunique() if "notice_id" in filtered.columns else 0)),
        ]
    )

    current_view, selected_document_id = get_route_state(page_key)
    if current_view == "detail":
        selected_row = get_row_by_column_value(source_df, "_row_id", selected_document_id)
        if selected_row is None and all_df is not None and not all_df.empty:
            selected_row = get_row_by_column_value(
                ensure_opportunity_row_ids(all_df),
                "_row_id",
                selected_document_id,
            )
        action_col, info_col = st.columns([1, 5])
        with action_col:
            if st.button("紐⑸줉?쇰줈", key=f"{page_key}_back_to_table_aligned", use_container_width=True):
                switch_to_table(page_key)
        with info_col:
            st.markdown('<div class="page-note">釉뚮씪?곗? ?ㅻ줈媛湲곕줈??댁쟾 ?붾㈃?쇰줈 ?뚯븘媛???덉뒿?덈떎.</div>', unsafe_allow_html=True)
        render_opportunity_detail_from_row(selected_row)
        return

    render_section_label("RFP Analysis List")
    st.markdown(
        f'<div class="page-note">怨듦퀬紐낆씠??怨쇱젣紐낆쓣 ?꾨Ⅴ硫??곸꽭 怨듦퀬? RFP 遺꾩꽍 ?섏씠吏濡??대룞?⑸땲?? ?꾩옱 {len(filtered)}嫄?/div>',
        unsafe_allow_html=True,
    )
    render_clickable_table(
        filtered,
        OPPORTUNITY_PREFERRED_COLUMNS,
        page_key=page_key,
        id_column="_row_id",
        source_key_column="source_key",
    )




















def render_opportunity_page_aligned(
    df: pd.DataFrame,
    *,
    page_key: str | None = None,
    title: str | None = None,
    archive: bool = False,
    all_df: pd.DataFrame | None = None,
) -> None:
    page_key = page_key or ("opportunity_archive" if archive else "opportunity")
    title = title or ("RFP Archive" if archive else "RFP Queue")
    subtitle = "?ъ뾽怨듦퀬 ??吏??媛?ν븳 RFP瑜?異붿쿇?⑸땲??"
    if archive:
        subtitle = "蹂닿? ??곸쑝濡?遺꾨쪟??RFP 遺꾩꽍 寃곌낵瑜?媛蹂띻쾶 ?먯깋??덉뒿?덈떎."
    render_page_header(title, subtitle, eyebrow="RFP")

    source_df = ensure_opportunity_row_ids(df)
    filtered = filter_archived_opportunity_rows(source_df) if archive else filter_current_opportunity_rows(source_df)
    filtered = filter_rankable_opportunity_rows(filtered)
    if filtered.empty:
        st.info("?쒖떆??RFP媛 ?놁뒿?덈떎.")
        return

    working = filtered.copy()
    working["_queue_recommendation"] = series_from_candidates(working, ["異붿쿇?щ?", "recommendation"]).fillna("").astype(str).str.strip()
    working["_queue_status"] = series_from_candidates(working, ["怨듦퀬?곹깭", "status", "rcve_status"]).fillna("").astype(str).apply(normalize_notice_status_label)
    working["_queue_deadline_sort"] = series_from_candidates(working, ["?묒닔湲곌컙", "period"]).apply(extract_period_end)
    working["_queue_project_sort"] = series_from_candidates(working, ["?대떦 怨쇱젣紐?, "project_name", "llm_project_name"]).fillna("").astype(str).str.strip()

    recommendation_options = sorted(
        [value for value in working["_queue_recommendation"].unique().tolist() if clean(value)]
    )
    status_options = sorted(
        [value for value in working["_queue_status"].unique().tolist() if clean(value)]
    )

    st.markdown('<div class="queue-shell-note">異붿쿇 ?곹깭? 怨듦퀬 ?곹깭留?鍮좊Ⅴ寃?醫곹엳怨? 寃곌낵 ?됱쓣 ?뚮윭 ?곸꽭 怨듦퀬? RFP ?댁슜??諛붾줈 ?뺤씤??덇쾶 援ъ꽦?덉뒿?덈떎.</div>', unsafe_allow_html=True)
    st.markdown('<div class="queue-filter-label">?붽굔 / ?꾪꽣</div>', unsafe_allow_html=True)
    st.markdown(
        '<div class="queue-filter-help">異붿쿇 ?곹깭? 怨듦퀬 ?곹깭留?鍮좊Ⅴ寃?醫곹??遺꾩꽍??RFP瑜??뺤씤?⑸땲??</div>',
        unsafe_allow_html=True,
    )
    filter_cols = st.columns(2)
    with filter_cols[0]:
        selected_recommendation = st.multiselect(
            "異붿쿇 ?곹깭",
            options=recommendation_options,
            default=[],
            key=f"{page_key}_filter_recommendation_aligned",
            placeholder="?꾩껜",
        )
    with filter_cols[1]:
        selected_status = st.multiselect(
            "怨듦퀬 ?곹깭",
            options=status_options,
            default=[],
            key=f"{page_key}_filter_status_aligned",
            placeholder="?꾩껜",
        )

    filtered = working.copy()
    if selected_recommendation:
        filtered = filtered[filtered["_queue_recommendation"].isin(selected_recommendation)]
    if selected_status:
        filtered = filtered[filtered["_queue_status"].isin(selected_status)]

    filtered = filtered.sort_values(
        by=["_queue_sort_score", "_queue_deadline_sort", "_queue_project_sort"],
        ascending=[False, True, True],
        na_position="last",
    )

    render_metrics(
        [
            ("RFP 遺꾩꽍 嫄댁닔", str(len(filtered))),
            ("異붿쿇 嫄댁닔", str(int((filtered["recommendation"] == "異붿쿇").sum()) if "recommendation" in filtered.columns else 0)),
            ("?됯퇏 ?먯닔", safe_mean(filtered["rfp_score"]) if "rfp_score" in filtered.columns and len(filtered) > 0 else "-"),
            ("怨듦퀬 ??, str(filtered["notice_id"].nunique() if "notice_id" in filtered.columns else 0)),
        ]
    )

    current_view, selected_document_id = get_route_state(page_key)
    if current_view == "detail":
        selected_row = get_row_by_column_value(source_df, "_row_id", selected_document_id)
        if selected_row is None and all_df is not None and not all_df.empty:
            selected_row = get_row_by_column_value(
                ensure_opportunity_row_ids(all_df),
                "_row_id",
                selected_document_id,
            )
        action_col, info_col = st.columns([1, 5])
        with action_col:
            if st.button("?뚯씠釉붾줈 ?뚯븘媛湲?, key=f"{page_key}_back_to_table_aligned", use_container_width=True):
                switch_to_table(page_key)
        with info_col:
            st.markdown('<div class="page-note">釉뚮씪?곗? ?ㅻ줈媛湲곕줈??붾㈃?쇰줈 ?뚯븘媛???덉뒿?덈떎.</div>', unsafe_allow_html=True)
        render_opportunity_detail_from_row(selected_row)
        return

    render_section_label("RFP Analysis List")
    st.markdown(
        f'<div class="page-note">怨듦퀬紐낆씠??怨쇱젣紐낆쓣 ?대┃?섎㈃ ?곸꽭 怨듦퀬? RFP 遺꾩꽍 ?섏씠吏濡??대룞?⑸땲?? ?꾩옱 {len(filtered)}嫄?/div>',
        unsafe_allow_html=True,
    )
    render_clickable_table(
        filtered,
        OPPORTUNITY_PREFERRED_COLUMNS,
        page_key=page_key,
        id_column="_row_id",
    )




def prepare_notice_collection_rows(
    source_df: pd.DataFrame,
    *,
    page_key: str,
    search_columns: list[str],
    status_default: str = "?꾩껜",
    current_only_default: bool = False,
    current_column: str = "is_current",
    apply_status_scope: bool = True,
    extra_multiselects: list[tuple[str, str, str]] | None = None,
) -> pd.DataFrame:
    filtered = source_df.copy()
    search_text, current_only, status_scope = render_notice_filter_sidebar(
        page_key,
        current_only_default=current_only_default,
        status_default=status_default,
    )
    if current_only and current_column in filtered.columns:
        filtered = filtered[filtered[current_column].fillna("").eq("Y")]
    if apply_status_scope:
        filtered = filter_notice_status_scope(filtered, status_scope)
    filtered = apply_multiselect_filter(filtered, "?꾨Ц湲곌?", "?꾨Ц湲곌?", f"{page_key}_agency")
    filtered = apply_multiselect_filter(filtered, "?뚭?遺泥?, "?뚭?遺泥?, f"{page_key}_ministry")
    filtered = apply_multiselect_filter(filtered, "寃??щ?", "寃??щ?", f"{page_key}_review")
    for column, label, key_suffix in extra_multiselects or []:
        filtered = apply_multiselect_filter(filtered, column, label, f"{page_key}_{key_suffix}")
    return filtered[
        build_contains_mask(
            filtered,
            search_columns,
            search_text,
        )
    ]


def render_pending_page(df: pd.DataFrame) -> None:
    render_page_header("Pending Notice", "?덉젙 怨듦퀬? ?묒닔 ?덉젙 嫄댁쓣 癒쇱? ?먭??⑸땲??", eyebrow="Pending")
    page_key = "pending"

    source_df = df.copy()
    filtered = prepare_notice_collection_rows(
        source_df,
        page_key=page_key,
        search_columns=["怨듦퀬紐?, "怨듦퀬踰덊샇", "?꾨Ц湲곌?", "?뚭?遺泥?, "怨듦퀬ID"],
        status_default="?덉젙",
        current_only_default=True,
        extra_multiselects=[("怨듦퀬?곹깭", "怨듦퀬?곹깭", "status")],
    )

    render_metrics(
        [
            ("?묒닔?덉젙 怨듦퀬 ??, str(len(filtered))),
            ("?꾩옱 ?쒖떆 怨듦퀬", str(int((filtered["is_current"] == "Y").sum()) if "is_current" in filtered.columns else 0)),
            ("?꾨Ц湲곌? ??, str(filtered["?꾨Ц湲곌?"].nunique() if "?꾨Ц湲곌?" in filtered.columns else 0)),
            ("寃??꾨즺", str(int(filtered["寃??щ?"].fillna("").ne("").sum()) if "寃??щ?" in filtered.columns else 0)),
        ]
    )

    current_view, selected_notice_id = get_route_state(page_key)

    if current_view == "detail":
        selected_row = get_row_by_column_value(source_df, "怨듦퀬ID", selected_notice_id)
        action_col, info_col = st.columns([1, 5])
        with action_col:
            if st.button("?뚯씠釉붾줈 ?뚯븘媛湲?, key="pending_back_to_table", use_container_width=True):
                switch_to_table(page_key)
        with info_col:
            st.markdown('<div class="page-note">釉뚮씪?곗? ?ㅻ줈媛湲곕줈??붾㈃?쇰줈 ?뚯븘媛???덉뒿?덈떎.</div>', unsafe_allow_html=True)
        render_pending_detail_from_row(selected_row)
        return

    render_section_label("Pending List")
    st.markdown(
        f'<div class="page-note">怨듦퀬紐??먮뒗 怨쇱젣紐낆쓣 ?대┃?섎㈃ ?곸꽭 ?섏씠吏濡??대룞?⑸땲?? ?꾩옱 {len(filtered)}嫄?/div>',
        unsafe_allow_html=True,
    )
    render_clickable_table(
        filtered,
        PENDING_PREFERRED_COLUMNS,
        page_key=page_key,
        id_column="怨듦퀬ID",
    )


def render_summary_page(df: pd.DataFrame, opportunity_df: pd.DataFrame) -> None:
    render_page_header("Summary", "怨듦퀬蹂????怨쇱젣? 異붿쿇 ?붿빟??쒕늿??遊낅땲??", eyebrow="Summary")
    page_key = "summary"

    source_df = df.copy()
    filtered = filter_current_summary_rows(source_df)
    filtered = prepare_notice_collection_rows(
        filtered,
        page_key=page_key,
        search_columns=["怨듦퀬紐?, "怨듦퀬踰덊샇", "?대떦 怨쇱젣紐?, "?덉궛", "怨듦퀬ID"],
        status_default="?꾩껜",
        current_only_default=True,
        extra_multiselects=[("??쒖텛泥쒕룄", "??쒖텛泥쒕룄", "recommendation")],
    )

    if "??쒖젏?? in filtered.columns and len(filtered) > 0:
        min_score = int(filtered["??쒖젏??].min())
        max_score = int(filtered["??쒖젏??].max())
        if min_score < max_score:
            score_range = st.sidebar.slider(
                "??쒖젏??踰붿쐞",
                min_value=min_score,
                max_value=max_score,
                value=(min_score, max_score),
                key="summary_score_range",
            )
            filtered = filtered[
                (filtered["??쒖젏??] >= score_range[0]) &
                (filtered["??쒖젏??] <= score_range[1])
            ]
        else:
            st.sidebar.caption(f"??쒖젏??怨좎젙媛? {min_score}")

    render_metrics(
        [
            ("?붿빟 怨듦퀬 ??, str(len(filtered))),
            ("異붿쿇 怨듦퀬", str(int((filtered["??쒖텛泥쒕룄"] == "異붿쿇").sum()) if "??쒖텛泥쒕룄" in filtered.columns else 0)),
            ("?됯퇏 ??쒖젏??, safe_mean(filtered["??쒖젏??]) if "??쒖젏?? in filtered.columns and len(filtered) > 0 else "-"),
            ("?됯퇏 怨쇱젣??, safe_mean(filtered["怨쇱젣??]) if "怨쇱젣?? in filtered.columns and len(filtered) > 0 else "-"),
        ]
    )

    current_view, selected_notice_id = get_route_state(page_key)

    if current_view == "detail":
        selected_row = get_row_by_column_value(source_df, "怨듦퀬ID", selected_notice_id)
        action_col, info_col = st.columns([1, 5])
        with action_col:
            if st.button("紐⑸줉?쇰줈", key="summary_back_to_table", use_container_width=True):
                switch_to_table(page_key)
        with info_col:
            st.markdown('<div class="page-note">釉뚮씪?곗? ?ㅻ줈媛湲곕줈??붿빟 由ъ뒪??붾㈃?쇰줈 ?뚯븘媛???덉뒿?덈떎.</div>', unsafe_allow_html=True)
        render_summary_detail_from_row(selected_row, opportunity_df)
        return

    render_section_label("Summary List")
    st.markdown(
        f'<div class="page-note">怨듦퀬紐??먮뒗 怨쇱젣紐낆쓣 ?대┃?섎㈃ ???遺꾩꽍 ?붿빟怨??곌껐??RFP ?곸꽭瑜??④퍡 ?뺤씤??덉뒿?덈떎. ?꾩옱 {len(filtered)}嫄?/div>',
        unsafe_allow_html=True,
    )
    render_clickable_table(
        filtered,
        SUMMARY_PREFERRED_COLUMNS,
        page_key=page_key,
        id_column="怨듦퀬ID",
    )


def render_summary_page(df: pd.DataFrame, opportunity_df: pd.DataFrame) -> None:
    del df

    working = ensure_opportunity_row_ids(filter_rankable_opportunity_rows(filter_current_opportunity_rows(opportunity_df.copy())))
    if working.empty:
        st.info("??뽯뻻??브쑴苑???怨몄뵠 ??곷뮸??덈뼄.")
        return

    selected_row_id = clean(get_query_param("id"))
    if not selected_row_id or selected_row_id not in working["_row_id"].fillna("").astype(str).tolist():
        working = working.sort_values(by=["rfp_score", "project_name"], ascending=[False, True], na_position="last")
        selected_row_id = clean(working.iloc[0].get("_row_id"))

    selected_row = get_row_by_column_value(working, "_row_id", selected_row_id)
    render_opportunity_detail_from_row(selected_row)


def render_errors_page(df: pd.DataFrame) -> None:
    st.subheader("Errors")
    page_key = "errors"

    filtered = enrich_error_df(df)
    if filtered.empty:
        st.info("?꾩옱 ?곸옱??ㅻ쪟 ?됱씠 ?놁뒿?덈떎.")
        return

    search_text = render_sidebar_search()
    filtered = apply_multiselect_filter(filtered, "異쒖쿂?ъ씠??, "source_site", "errors_source")
    if search_text:
        filtered = filtered[
            build_contains_mask(
                filtered,
                [
                    "source_site",
                    "notice_id",
                    "notice_title",
                    "project_name",
                    "rfp_title",
                    "file_name",
                    "validation_errors",
                    "llm_error",
                    "parse_error",
                ],
                search_text,
            )
        ]

    render_metrics(
        [
            ("?ㅻ쪟 ??, str(len(filtered))),
            ("怨듦퀬 ??, str(filtered["notice_id"].nunique() if "notice_id" in filtered.columns else 0)),
            ("異쒖쿂 ??, str(filtered["source_site"].nunique() if "source_site" in filtered.columns else 0)),
        ]
    )

    st.caption(f"寃利??뚯떛/LLM ?ㅻ쪟 ?됱엯?덈떎. ?꾩옱 {len(filtered)}嫄?)
    visible_columns = [column for column in ERROR_PREFERRED_COLUMNS if column in filtered.columns]
    st.dataframe(filtered[visible_columns] if visible_columns else filtered, use_container_width=True, hide_index=True)


def render_source_notice_page(
    df: pd.DataFrame,
    data_origin: str,
    *,
    prefix: str,
    title: str,
    source_label: str = "以묒냼湲곗뾽踰ㅼ쿂遺",
    view_columns: list[str] | None = None,
    archive: bool = False,
) -> None:
    st.markdown(f"### {title}")
    st.caption(f"{source_label} ?곌퀎 怨듦퀬 紐⑸줉?낅땲?? ?곗씠??뚯뒪: {data_origin}")

    if df.empty:
        st.info(f"{source_label} 怨듦퀬 ?곗씠?곕? ?꾩쭅 遺덈윭?ㅼ? 紐삵뻽?듬땲??")
        return

    current_view, selected_notice_id = get_route_state(prefix)
    if current_view == "detail":
        selected_row = get_row_by_column_value(df, "怨듦퀬ID", selected_notice_id)
        action_col, info_col = st.columns([1, 5])
        with action_col:
            if st.button("紐⑸줉?쇰줈 ?뚯븘媛湲?, key=f"{prefix}_back_to_table", use_container_width=True):
                switch_to_table(prefix)
        with info_col:
            st.caption("釉뚮씪?곗? ?ㅻ줈媛湲곕? ?뚮윭??紐⑸줉?쇰줈 ?뚯븘媛???덉뒿?덈떎.")
        render_notice_detail_from_row(selected_row, pd.DataFrame())
        return

    filtered = df.copy()
    filtered = filter_archived_notice_rows(filtered) if archive else filter_current_notice_rows(filtered)
    default_status = "?꾩껜" if archive else "?덉젙" if "scheduled" in prefix else "?묒닔以?
    search_text, current_only, status_scope = render_notice_filter_sidebar(
        prefix,
        current_only_default=False,
        status_default=default_status,
        show_current_only=not archive,
        show_status_scope=not archive,
    )
    if current_only and "is_current" in filtered.columns:
        filtered = filtered[filtered["is_current"].fillna("").eq("Y")]
    if not archive:
        filtered = filter_notice_status_scope(filtered, status_scope)
    filtered = apply_multiselect_filter(filtered, "?꾨Ц湲곌?", "?꾨Ц湲곌?", f"{prefix}_agency")
    filtered = apply_multiselect_filter(filtered, "?뚭?遺泥?, "?뚭?遺泥?, f"{prefix}_ministry")
    filtered = apply_multiselect_filter(filtered, "寃??щ?", "寃??щ?", f"{prefix}_review")

    if search_text:
        filtered = filtered[build_contains_mask(filtered, ["怨듦퀬紐?, "怨듦퀬踰덊샇", "?꾨Ц湲곌?", "?대떦遺??, "?뚭?遺泥?, "?ъ뾽紐?], search_text)]

    open_count = int(filtered["?곹깭"].fillna("").astype(str).str.strip().eq("?묒닔以?).sum()) if "?곹깭" in filtered.columns else 0
    metric_cols = st.columns(3)
    metric_cols[0].metric("怨듦퀬 ??, str(len(filtered)))
    metric_cols[1].metric("?묒닔以?, str(open_count))
    metric_cols[2].metric("?대떦遺???, str(filtered["?대떦遺??].nunique() if "?대떦遺?? in filtered.columns else 0))

    st.caption(f"怨듦퀬紐??먮뒗 怨쇱젣紐낆쓣 ?대┃?섎㈃ ?곸꽭 ?섏씠吏濡??대룞?⑸땲?? ?꾩옱 {len(filtered)}嫄?)
    render_clickable_table(
        filtered,
        view_columns or MSS_VIEW_COLUMNS,
        page_key=prefix,
        id_column="怨듦퀬ID",
    )


def normalize_favorite_notice_df(df: pd.DataFrame, *, source_key: str, source_label: str) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame()

    working = df.copy()
    working["留ㅼ껜"] = source_label
    working["_source_key"] = source_key
    working["怨듦퀬ID"] = series_from_candidates(working, ["怨듦퀬ID", "notice_id"])
    working["怨듦퀬紐?] = series_from_candidates(working, ["怨듦퀬紐?, "notice_title", "title"])
    working["怨듦퀬踰덊샇"] = series_from_candidates(working, ["怨듦퀬踰덊샇", "notice_no", "ancm_no"])
    working["怨듦퀬?쇱옄"] = series_from_candidates(working, ["怨듦퀬?쇱옄", "?깅줉??, "registered_at", "ancm_de"])
    working["?묒닔湲곌컙"] = series_from_candidates(working, ["?묒닔湲곌컙", "?좎껌湲곌컙", "period"])
    working["?꾨Ц湲곌?"] = series_from_candidates(working, ["?꾨Ц湲곌?", "?꾨Ц湲곌?紐?, "agency"])
    working["?대떦遺??] = series_from_candidates(working, ["?대떦遺??, "department", "agency"])
    working["?뚭?遺泥?] = series_from_candidates(working, ["?뚭?遺泥?, "ministry"])
    working["怨듦퀬?곹깭"] = series_from_candidates(working, ["怨듦퀬?곹깭", "?곹깭", "status", "rcve_status"])
    working["寃??щ?"] = series_from_candidates(working, ["寃??щ?", "寃?좎뿬遺", "review_status"])
    working["?곸꽭留곹겕"] = series_from_candidates(working, ["?곸꽭留곹겕", "detail_link"])
    working["?곸꽭留곹겕"] = working.apply(
        lambda row: resolve_external_detail_link(row, source_key=source_key),
        axis=1,
    )
    working["_favorite_id"] = working.apply(
        lambda row: f"{source_key}::{clean(row.get('怨듦퀬ID'))}",
        axis=1,
    )
    if "??쒖젏?? not in working.columns:
        working["??쒖젏??] = 0
    working["_sort_date"] = parse_date_column(working["怨듦퀬?쇱옄"])
    return working


def build_favorite_notice_df(notice_view_df: pd.DataFrame, source_datasets: dict[str, object]) -> pd.DataFrame:
    frames = []
    iris_df = normalize_favorite_notice_df(
        notice_view_df,
        source_key="iris",
        source_label="IRIS",
    )
    if not iris_df.empty:
        frames.append(iris_df)

    mss_current_df = source_datasets["mss_current"]
    mss_past_df = source_datasets["mss_past"]
    mss_df = pd.concat([mss_current_df, mss_past_df], ignore_index=True) if not mss_current_df.empty or not mss_past_df.empty else pd.DataFrame()
    if not mss_df.empty:
        mss_df = mss_df.drop_duplicates(subset=["怨듦퀬ID"], keep="first")
        frames.append(normalize_favorite_notice_df(mss_df, source_key="tipa", source_label="以묒냼湲곗뾽踰ㅼ쿂遺"))

    nipa_current_df = source_datasets["nipa_current"]
    nipa_past_df = source_datasets["nipa_past"]
    nipa_df = pd.concat([nipa_current_df, nipa_past_df], ignore_index=True) if not nipa_current_df.empty or not nipa_past_df.empty else pd.DataFrame()
    if not nipa_df.empty:
        nipa_df = nipa_df.drop_duplicates(subset=["怨듦퀬ID"], keep="first")
        frames.append(normalize_favorite_notice_df(nipa_df, source_key="nipa", source_label="NIPA"))

    if not frames:
        return pd.DataFrame()

    combined = pd.concat(frames, ignore_index=True)
    combined = combined[
        combined["寃??щ?"].fillna("").astype(str).str.strip().eq(FAVORITE_REVIEW_STATUS)
    ]
    if combined.empty:
        return combined
    return combined.sort_values(
        by=["_sort_date", "留ㅼ껜", "怨듦퀬紐?],
        ascending=[False, True, True],
        na_position="last",
    )






def render_other_crawlers_source_page() -> None:
    st.subheader("Other Crawlers")
    st.info("?ㅻⅨ ?щ·??뚯뒪??ш린??뺤옣??덉뒿?덈떎.")


VIEWER_V2_ROUTE_MAP: dict[str, tuple[str, str]] = {
    "rfp_queue": ("iris", "rfp_queue"),
    "notice_queue": ("iris", "notice_queue"),
    "summary": ("iris", "summary"),
    "notice_archive": ("iris", "notice_archive"),
    "opportunity_archive": ("iris", "notice_archive"),
    "favorites": ("favorites", "favorites"),
}


def load_viewer_runtime(app_mode: str = "viewer") -> tuple[AppModeConfig, dict[str, pd.DataFrame], dict[str, object]]:
    load_dotenv()

    mode_config = build_app_mode_config(
        app_mode,
        nipa_view_columns=tuple(NIPA_VIEW_COLUMNS),
    )

    st.set_page_config(
        page_title=mode_config.page_title,
        layout="wide",
    )
    inject_page_styles()
    inject_opportunity_detail_alignment_styles()
    inject_viewer_layout_styles()
    require_login(mode_config)

    sheet_names = {
        "notice_master": resolve_canonical_notice_master_sheet(get_env),
        "notice_current": resolve_notice_current_view_sheet(get_env),
        "pending": resolve_notice_pending_view_sheet(get_env),
        "notice_archive": resolve_notice_archive_view_sheet(get_env),
        "opportunity": resolve_iris_opportunity_current_sheet(get_env),
        "opportunity_archive": resolve_iris_opportunity_archive_sheet(get_env),
        "summary": get_env("SUMMARY_SHEET", "SUMMARY"),
        "errors": get_env("ERROR_SHEET", "OPPORTUNITY_ERRORS"),
    }

    datasets = load_app_datasets(
        sheet_names["notice_master"],
        sheet_names["notice_current"],
        sheet_names["pending"],
        sheet_names["notice_archive"],
        sheet_names["opportunity"],
        sheet_names["opportunity_archive"],
        sheet_names["summary"],
        sheet_names["errors"],
    )

    source_datasets = build_source_datasets()
    if is_user_scoped_operations_enabled():
        datasets, source_datasets = apply_user_review_statuses(
            datasets,
            source_datasets,
            get_current_operation_scope_key(),
        )
    return mode_config, datasets, source_datasets


def main_viewer_v2(app_mode: str = "viewer") -> None:
    try:
        mode_config, datasets, source_datasets = load_viewer_runtime(app_mode)
    except Exception as exc:
        st.error(f"?쒗듃 濡쒕뵫 ?ㅽ뙣: {exc}")
        st.stop()

    render_workspace_header(mode_config)

    current_page = normalize_route_page_key(get_query_param("page")) or "rfp_queue"
    if current_page == "opportunity_archive":
        current_page = "notice_archive"
    if current_page not in VIEWER_V2_ROUTE_MAP:
        current_page = "rfp_queue"

    selected_page = render_page_tabs(
        current_page,
        [
            ("rfp_queue", "RFP Queue"),
            ("notice_queue", "Notice Queue"),
            ("summary", "Summary"),
            ("notice_archive", "Archive"),
            ("favorites", "愿?ш났怨?),
        ],
        key="viewer_v2_primary_tabs",
    )
    if selected_page != current_page:
        target_source, target_page = VIEWER_V2_ROUTE_MAP[selected_page]
        navigate_to_route(target_source, target_page)

    if current_page == "notice_queue":
        render_notice_queue_page(datasets, source_datasets)
        return
    if current_page == "notice_archive":
        render_notice_page_with_scope(
            datasets["notice_view"],
            datasets["opportunity_all"],
            page_key="notice_archive",
            title="Archive",
            default_status_scope="?꾩껜",
            current_only_default=False,
            archive=True,
        )
        return
    if current_page == "summary":
        render_summary_page(
            datasets["summary"],
            datasets["opportunity_all"],
        )
        return
    if current_page == "favorites":
        render_favorite_notice_page(
            datasets["notice_view"],
            datasets["opportunity_all"],
            source_datasets,
        )
        return

    render_opportunity_page(
        datasets["opportunity"],
        page_key="rfp_queue",
        title="RFP Queue",
        archive=False,
    )


def main(app_mode: str = "viewer"):
    _run_public_workspace_main(app_mode)
    return
    load_dotenv()

    mode_config = build_app_mode_config(
        app_mode,
        nipa_view_columns=tuple(NIPA_VIEW_COLUMNS),
    )

    st.set_page_config(
        page_title=mode_config.page_title,
        layout="wide",
    )
    inject_page_styles()
    inject_opportunity_detail_alignment_styles()
    inject_viewer_layout_styles()
    require_login(mode_config)
    render_workspace_header(mode_config)

    sheet_names = {
        "notice_master": resolve_canonical_notice_master_sheet(get_env),
        "notice_current": resolve_notice_current_view_sheet(get_env),
        "pending": resolve_notice_pending_view_sheet(get_env),
        "notice_archive": resolve_notice_archive_view_sheet(get_env),
        "opportunity": resolve_iris_opportunity_current_sheet(get_env),
        "opportunity_archive": resolve_iris_opportunity_archive_sheet(get_env),
        "summary": get_env("SUMMARY_SHEET", "SUMMARY"),
        "errors": get_env("ERROR_SHEET", "OPPORTUNITY_ERRORS"),
    }

    try:
        datasets = load_app_datasets(
            sheet_names["notice_master"],
            sheet_names["notice_current"],
            sheet_names["pending"],
            sheet_names["notice_archive"],
            sheet_names["opportunity"],
            sheet_names["opportunity_archive"],
            sheet_names["summary"],
            sheet_names["errors"],
        )
    except Exception as exc:
        st.error(f"?쒗듃 濡쒕뵫 ?ㅽ뙣: {exc}")
        st.stop()

    source_config_map = get_source_config_map(mode_config)
    default_route = route_core.normalize_route(
        {
            "source": mode_config.default_source,
            "page": get_default_page_for_source(mode_config, mode_config.default_source),
            "view": "list",
            "source_key": mode_config.default_source,
        }
    )
    current_route = initialize_route_state(default_route)
    normalized_route = _normalize_workspace_shell_route(current_route)
    if not route_core.route_equals(current_route, normalized_route):
        route_core.set_current_route(normalized_route)
        replace_query_params(with_auth_params(route_core.serialize_route(normalized_route)))
        current_route = normalized_route
    current_source = clean(current_route.get("source")) or mode_config.default_source
    if current_source not in source_config_map:
        current_source = mode_config.default_source
    current_page = normalize_route_page_key(current_route.get("page")) or get_default_page_for_source(mode_config, current_source)
    current_group = find_nav_group_for_route(mode_config, current_source, current_page)

    selected_group_key = render_nav_tabs(
        current_group.key,
        [(group.key, group.label) for group in mode_config.nav_groups],
        key=f"{mode_config.mode}_primary_nav",
        label="",
    )
    selected_group = next((group for group in mode_config.nav_groups if group.key == selected_group_key), mode_config.nav_groups[0])
    if selected_group.key != current_group.key:
        target_item = selected_group.items[0]
        navigate_to_route(target_item.source_key, target_item.page_key)

    current_item = next(
        (
            item
            for item in selected_group.items
            if item.source_key == current_source and item.page_key == current_page
        ),
        None,
    )
    selected_item = current_item or selected_group.items[0]
    if current_item is not None and len(selected_group.items) > 1:
        selected_item_key = render_nav_tabs(
            current_item.key,
            [(item.key, item.label) for item in selected_group.items],
            key=f"{mode_config.mode}_secondary_nav_{selected_group.key}",
            label="?몃? ?섏씠吏",
        )
        selected_item = next((item for item in selected_group.items if item.key == selected_item_key), selected_group.items[0])
        if selected_item.key != current_item.key:
            navigate_to_route(selected_item.source_key, selected_item.page_key)

    selected_source_key = selected_item.source_key
    selected_source_config = source_config_map.get(selected_source_key)

    source_datasets = None
    if selected_source_config and selected_source_config.requires_source_datasets:
        source_datasets = build_source_datasets()
    if is_user_scoped_operations_enabled():
        datasets, source_datasets = apply_user_review_statuses(
            datasets,
            source_datasets,
            get_current_operation_scope_key(),
        )

    render_selected_source(
        selected_source_key,
        source_config=selected_source_config,
        mode_config=mode_config,
        datasets=datasets,
        source_datasets=source_datasets,
        show_internal_tabs=False,
    )


# BEGIN ADMIN ALIGNMENT OVERRIDES
def inject_opportunity_detail_alignment_styles() -> None:
    st.markdown(
        """
        <style>
        .notice-detail-panel,
        .notice-detail-sidebar-card {
          background: var(--surface);
          border: 1px solid var(--border);
          border-radius: 28px;
          overflow: hidden;
          box-shadow: var(--shadow);
          margin-bottom: 1rem;
        }
        .notice-detail-panel-header {
          display: flex;
          align-items: center;
          gap: 0.7rem;
          padding: 1rem 1.45rem;
          border-bottom: 1px solid var(--border);
        }
        .notice-detail-panel-header.blue {
          background: rgba(60, 99, 240, 0.09);
          color: var(--blue);
        }
        .notice-detail-panel-header.green {
          background: rgba(105, 187, 144, 0.14);
          color: #327a57;
        }
        .notice-detail-panel-header.amber {
          background: rgba(239, 173, 96, 0.16);
          color: #ad6c27;
        }
        .notice-detail-panel-title {
          margin: 0;
          font-size: 1.12rem;
          line-height: 1.2;
          font-weight: 900;
          letter-spacing: -0.03em;
        }
        .notice-detail-panel-body {
          padding: 0.1rem 1.45rem 0.35rem 1.45rem;
        }
        .notice-detail-panel-copy {
          padding: 1.35rem 1.45rem 1.5rem 1.45rem;
        }
        .notice-detail-data-row {
          display: flex;
          align-items: flex-start;
          gap: 1.4rem;
          padding: 0.95rem 0;
          border-bottom: 1px solid var(--border);
        }
        .notice-detail-data-row:last-child {
          border-bottom: none;
        }
        .notice-detail-data-label {
          width: 110px;
          flex-shrink: 0;
          min-height: 1.5rem;
          display: flex;
          align-items: center;
          color: var(--text-subtle);
          font-size: 0.84rem;
          font-weight: 900;
          letter-spacing: 0.01em;
        }
        .notice-detail-data-content {
          flex: 1;
          color: var(--text-strong);
          font-size: 0.98rem;
          line-height: 1.72;
        }
        .notice-detail-value,
        .notice-detail-multiline {
          color: var(--text-strong);
          white-space: pre-wrap;
        }
        .notice-detail-multiline {
          line-height: 1.82;
        }
        .notice-detail-tag {
          display: inline-flex;
          align-items: center;
          padding: 0.35rem 0.72rem;
          margin: 0 0.35rem 0.35rem 0;
          border-radius: 999px;
          background: var(--slate-soft);
          border: 1px solid var(--border);
          color: var(--text-body);
          font-size: 0.82rem;
          font-weight: 800;
        }
        .notice-detail-inline-badge {
          display: inline-flex;
          align-items: center;
          justify-content: center;
          min-height: 1.9rem;
          padding: 0.25rem 0.58rem;
          border-radius: 10px;
          font-size: 0.82rem;
          font-weight: 900;
        }
        .notice-detail-inline-badge.violet {
          background: rgba(132, 96, 247, 0.12);
          color: #7154e4;
        }
        .notice-detail-inline-badge.green {
          background: rgba(105, 187, 144, 0.13);
          color: #2e855c;
        }
        .notice-detail-inline-badge.rose {
          background: var(--rose);
          color: #ffffff;
        }
        .notice-detail-deadline-wrap {
          display: flex;
          align-items: center;
          flex-wrap: wrap;
          gap: 0.6rem;
        }
        .notice-detail-period-text {
          color: var(--text-strong);
          font-size: 0.96rem;
          line-height: 1.6;
        }
        .notice-detail-empty,
        .notice-detail-empty-block {
          color: var(--text-subtle);
        }
        .notice-detail-empty-block {
          padding: 1.05rem 0;
        }
        .notice-detail-paragraph {
          margin: 0 0 0.8rem 0;
          color: var(--text-body);
          font-size: 1rem;
          line-height: 1.84;
          white-space: pre-wrap;
        }
        .notice-detail-paragraph:last-child {
          margin-bottom: 0;
        }
        .notice-detail-step {
          display: flex;
          align-items: flex-start;
          gap: 0.9rem;
          padding: 1rem 0 0.2rem 0;
        }
        .notice-detail-step + .notice-detail-step {
          border-top: 1px solid var(--border);
          margin-top: 0.95rem;
          padding-top: 1.1rem;
        }
        .notice-detail-step-index {
          width: 1.7rem;
          height: 1.7rem;
          display: inline-flex;
          align-items: center;
          justify-content: center;
          flex-shrink: 0;
          border-radius: 8px;
          background: rgba(60, 99, 240, 0.12);
          color: var(--blue);
          font-size: 0.8rem;
          font-weight: 900;
        }
        .notice-detail-step-title {
          color: var(--blue);
          font-size: 0.92rem;
          font-weight: 900;
          letter-spacing: 0.01em;
          margin-bottom: 0.45rem;
        }
        .notice-detail-step-body {
          color: var(--text-body);
          font-size: 0.96rem;
          line-height: 1.8;
          white-space: pre-wrap;
        }
        </style>
        """,
        unsafe_allow_html=True,
    )


def parse_detail_tag_items(value: object, *, limit: int = 8) -> list[str]:
    if isinstance(value, (list, tuple, set)):
        raw_items = [clean(item) for item in value]
    else:
        text = clean(value)
        if not text:
            return []
        raw_items = []
        if text.startswith("[") and text.endswith("]"):
            try:
                parsed = ast.literal_eval(text)
            except (SyntaxError, ValueError):
                parsed = None
            if isinstance(parsed, (list, tuple, set)):
                raw_items.extend(clean(item) for item in parsed)
        if not raw_items:
            normalized = text
            for separator in ["|", "\n", ";"]:
                normalized = normalized.replace(separator, ",")
            raw_items.extend(clean(item) for item in normalized.split(","))

    deduped: list[str] = []
    for item in raw_items:
        if item and item not in deduped:
            deduped.append(item)
    return deduped[:limit]

def _notice_detail_scalar_text(label: str, value: object) -> str:
    raw_text = sanitize_display_text(label, value)
    if label in {"?ъ뾽 洹쒕え", "吏?먭툑", "珥앹삁??, "怨쇱젣蹂??덉궛"}:
        budget_summary = extract_budget_summary(raw_text)
        if budget_summary:
            raw_text = budget_summary
    return raw_text or "-"

def _notice_detail_deadline_parts(value: object) -> tuple[str, str]:
    period_text = clean(value)
    if not period_text:
        return "", "-"
    period_end = extract_period_end(period_text)
    if pd.isna(period_end):
        return "", period_text
    d_day = int((period_end.normalize() - pd.Timestamp.now().normalize()).days)
    if d_day > 0:
        return f"D-{d_day}", period_text
    if d_day == 0:
        return "D-Day", period_text
    return "留덇컧", period_text

def _notice_detail_value_html(label: str, value: object, *, kind: str = "text") -> str:
    if kind == "chips":
        chips = parse_detail_tag_items(value)
        if not chips:
            return '<span class="notice-detail-empty">-</span>'
        return "".join(
            f'<span class="notice-detail-tag">{escape(chip)}</span>' for chip in chips
        )

    if kind == "deadline":
        deadline_badge, period_text = _notice_detail_deadline_parts(value)
        badge_html = (
            f'<span class="notice-detail-inline-badge rose">{escape(deadline_badge)}</span>'
            if deadline_badge
            else ""
        )
        return (
            '<div class="notice-detail-deadline-wrap">'
            f'{badge_html}'
            f'<span class="notice-detail-period-text">{escape(period_text)}</span>'
            '</div>'
        )

    display_text = _notice_detail_scalar_text(label, value)
    if kind == "accent":
        return f'<span class="notice-detail-inline-badge violet">{escape(display_text)}</span>'
    if kind == "success":
        return f'<span class="notice-detail-inline-badge green">{escape(display_text)}</span>'
    if kind == "multiline":
        return f'<div class="notice-detail-multiline">{escape(display_text)}</div>'
    return f'<div class="notice-detail-value">{escape(display_text)}</div>'

def render_notice_detail_rows_panel(
    title: str,
    rows: list[dict[str, object]],
    *,
    tone: str = "blue",
) -> None:
    row_html: list[str] = []
    for item in rows:
        label = clean(item.get("label"))
        if not label:
            continue
        kind = clean(item.get("kind")) or "text"
        value = item.get("value")
        display_text = _notice_detail_scalar_text(label, value)
        if kind not in {"chips", "deadline"} and display_text == "-":
            continue
        value_html = _notice_detail_value_html(label, value, kind=kind)
        row_html.append(
            (
                '<div class="notice-detail-data-row">'
                f'<div class="notice-detail-data-label">{escape(label)}</div>'
                f'<div class="notice-detail-data-content">{value_html}</div>'
                '</div>'
            )
        )

    if not row_html:
        row_html.append(
            '<div class="notice-detail-empty-block">?쒖떆??뺣낫媛 ?놁뒿?덈떎.</div>'
        )

    st.markdown(
        (
            '<div class="notice-detail-panel">'
            f'<div class="notice-detail-panel-header {escape(tone)}">'
            f'<h2 class="notice-detail-panel-title">{escape(title)}</h2>'
            '</div>'
            f'<div class="notice-detail-panel-body">{"".join(row_html)}</div>'
            '</div>'
        ),
        unsafe_allow_html=True,
    )

def render_notice_detail_text_panel(title: str, value: object, *, tone: str = "blue") -> None:
    body = sanitize_display_text(title, value)
    if not clean(body):
        body = "?쒖떆??붿빟 ?뺣낫媛 ?놁뒿?덈떎."

    parts = [clean(chunk) for chunk in re.split(r"\n{2,}", body) if clean(chunk)]
    if not parts:
        parts = [body]
    body_html = "".join(
        f'<p class="notice-detail-paragraph">{escape(part)}</p>' for part in parts
    )

    st.markdown(
        (
            '<div class="notice-detail-panel">'
            f'<div class="notice-detail-panel-header {escape(tone)}">'
            f'<h2 class="notice-detail-panel-title">{escape(title)}</h2>'
            '</div>'
            f'<div class="notice-detail-panel-copy">{body_html}</div>'
            '</div>'
        ),
        unsafe_allow_html=True,
    )

def render_notice_detail_steps_panel(
    title: str,
    steps: list[dict[str, object]],
    *,
    tone: str = "blue",
) -> None:
    visible_steps = [
        item
        for item in steps
        if clean(item.get("title")) and clean(sanitize_display_text(clean(item.get("title")), item.get("body")))
    ]
    if not visible_steps:
        return

    step_html: list[str] = []
    for index, item in enumerate(visible_steps, start=1):
        step_title = clean(item.get("title"))
        step_body = sanitize_display_text(step_title, item.get("body")) or "-"
        step_html.append(
            (
                '<div class="notice-detail-step">'
                f'<div class="notice-detail-step-index">{index}</div>'
                '<div class="notice-detail-step-main">'
                f'<div class="notice-detail-step-title">{escape(step_title)}</div>'
                f'<div class="notice-detail-step-body">{escape(step_body)}</div>'
                '</div>'
                '</div>'
            )
        )

    st.markdown(
        (
            '<div class="notice-detail-panel">'
            f'<div class="notice-detail-panel-header {escape(tone)}">'
            f'<h2 class="notice-detail-panel-title">{escape(title)}</h2>'
            '</div>'
            f'<div class="notice-detail-panel-copy">{"".join(step_html)}</div>'
            '</div>'
        ),
        unsafe_allow_html=True,
    )

def render_notice_detail_sidebar_card(
    *,
    source_label: str,
    keyword_text: object,
    total_budget: object,
    per_project_budget: object,
    period_value: object,
    agency: object,
    ministry: object,
    recommendation: object,
    score: object,
    detail_link: str,
    detail_button_label: str,
    related_count: int,
) -> None:
    keyword_tags = parse_detail_tag_items(keyword_text, limit=4)
    deadline_badge, period_text = _notice_detail_deadline_parts(period_value)
    button_html = (
        f'<a class="notice-detail-action-link" href="{escape(detail_link, quote=True)}">'
        f'{escape(detail_button_label)}'
        '</a>'
        if detail_link
        else ""
    )
    keyword_html = (
        "".join(f'<span class="notice-detail-tag">{escape(tag)}</span>' for tag in keyword_tags)
        if keyword_tags
        else '<span class="notice-detail-empty">-</span>'
    )
    deadline_html = (
        f'<span class="notice-detail-inline-badge rose">{escape(deadline_badge)}</span>'
        if deadline_badge
        else ""
    )
    sidebar_html = (
        '<div class="notice-detail-sidebar-card">'
        f'<div class="notice-detail-sidebar-kicker">{escape(source_label)}</div>'
        '<div class="notice-detail-sidebar-title">怨듦퀬 ?쒕늿??蹂닿린</div>'
        '<div class="notice-detail-sidebar-meta">'
        '<div class="notice-detail-sidebar-label">?듭떖 ?ㅼ썙??/div>'
        f'<div class="notice-detail-sidebar-tags">{keyword_html}</div>'
        '</div>'
        '<div class="notice-detail-sidebar-grid">'
        '<div class="notice-detail-sidebar-item">'
        '<div class="notice-detail-sidebar-label">?ъ뾽 洹쒕え</div>'
        f'<div class="notice-detail-sidebar-value">{escape(_notice_detail_scalar_text("?ъ뾽 洹쒕え", total_budget))}</div>'
        '</div>'
        '<div class="notice-detail-sidebar-item">'
        '<div class="notice-detail-sidebar-label">吏?먭툑</div>'
        f'<div class="notice-detail-sidebar-value">{escape(_notice_detail_scalar_text("吏?먭툑", per_project_budget))}</div>'
        '</div>'
        '<div class="notice-detail-sidebar-item">'
        '<div class="notice-detail-sidebar-label">?꾨Ц湲곌?</div>'
        f'<div class="notice-detail-sidebar-value">{escape(_notice_detail_scalar_text("?꾨Ц湲곌?", agency))}</div>'
        '</div>'
        '<div class="notice-detail-sidebar-item">'
        '<div class="notice-detail-sidebar-label">?뚭?遺泥?/div>'
        f'<div class="notice-detail-sidebar-value">{escape(_notice_detail_scalar_text("?뚭?遺泥?, ministry))}</div>'
        '</div>'
        '</div>'
        '<div class="notice-detail-sidebar-period">'
        '<div class="notice-detail-sidebar-label">?좎껌 湲곌컙</div>'
        '<div class="notice-detail-deadline-wrap">'
        f'{deadline_html}'
        f'<span class="notice-detail-period-text">{escape(period_text)}</span>'
        '</div>'
        '</div>'
        '<div class="notice-detail-sidebar-grid compact">'
        '<div class="notice-detail-sidebar-item">'
        '<div class="notice-detail-sidebar-label">異붿쿇 ?곹깭</div>'
        f'<div class="notice-detail-sidebar-value">{escape(_notice_detail_scalar_text("異붿쿇 ?곹깭", recommendation))}</div>'
        '</div>'
        '<div class="notice-detail-sidebar-item">'
        '<div class="notice-detail-sidebar-label">?곹빀 ?먯닔</div>'
        f'<div class="notice-detail-sidebar-value">{escape(_notice_detail_scalar_text("?곹빀 ?먯닔", score))}</div>'
        '</div>'
        '<div class="notice-detail-sidebar-item">'
        '<div class="notice-detail-sidebar-label">?곌껐 RFP</div>'
        f'<div class="notice-detail-sidebar-value">{related_count}嫄?/div>'
        '</div>'
        '</div>'
        f'{button_html}'
        '</div>'
    )
    st.markdown(sidebar_html, unsafe_allow_html=True)

def _split_sentences_for_display(value: object) -> list[str]:
    text = clean(value)
    if not text:
        return []

    normalized = re.sub(r"\r\n?", "\n", text)
    normalized = re.sub(r"\n{3,}", "\n\n", normalized)
    parts = re.split(r"\n{2,}|(?<=[.!??ㅼ슂??)\s+(?=[A-Z媛-??-9?△뼚?뗢뿈])", normalized)
    sentences: list[str] = []
    seen: set[str] = set()
    for part in parts:
        sentence = clean(part)
        if not sentence:
            continue
        key = re.sub(r"\s+", " ", sentence).lower()
        if key in seen:
            continue
        seen.add(key)
        sentences.append(sentence)
    return sentences

def _join_display_blocks(*values: object, max_items: int = 4) -> str:
    sentences: list[str] = []
    seen: set[str] = set()
    for value in values:
        for sentence in _split_sentences_for_display(value):
            key = re.sub(r"\s+", " ", sentence).lower()
            if key in seen:
                continue
            seen.add(key)
            sentences.append(sentence)
            if len(sentences) >= max_items:
                return "\n\n".join(sentences)
    return "\n\n".join(sentences)

def _build_benefit_text(
    *,
    total_budget_text: object,
    per_project_budget_text: object,
    eligibility_text: object,
    period_text: object,
    support_plan_text: object,
    support_need_text: object = "",
) -> str:
    benefit_parts: list[str] = []

    total_budget = extract_budget_summary(clean(total_budget_text))
    per_project_budget = extract_budget_summary(clean(per_project_budget_text))
    eligibility = clean(eligibility_text)
    period = clean(period_text)
    support_plan = clean(support_plan_text)
    support_need = clean(support_need_text)

    if total_budget:
        benefit_parts.append(f"?ъ뾽鍮?洹쒕え??{total_budget}?낅땲??")
    if per_project_budget:
        benefit_parts.append(f"怨쇱젣蹂?吏??議곌굔? {per_project_budget} 湲곗??쇰줈 ?뺣━?⑸땲??")
    if eligibility:
        benefit_parts.append(f"吏??媛??湲곌?? {eligibility}?낅땲??")
    if period:
        benefit_parts.append(f"?ъ뾽 湲곌컙 諛??묒닔 ?쇱젙? {period} 湲곗??쇰줈 ?댁쁺?⑸땲??")
    if support_plan:
        benefit_parts.append(support_plan)
    elif support_need:
        benefit_parts.append(support_need)

    return _join_display_blocks(*benefit_parts, max_items=5)

def build_analysis_story_bundle(
    row: dict,
    *,
    notice_row: dict | None = None,
    period_text: object = "",
) -> dict[str, object]:
    base_row = row or {}
    notice_row = notice_row or {}

    summary_text = first_non_empty(base_row, "llm_reason", "reason", "summary", "analysis_summary")
    background_text = first_non_empty(
        base_row,
        "llm_support_necessity",
        "support_necessity",
        "llm_technical_background",
        "technical_background",
        "project_overview",
        "project_summary",
        "?ъ뾽媛쒖슂",
        "怨쇱젣媛쒖슂",
    )
    objective_text = first_non_empty(
        base_row,
        "llm_concept_and_development",
        "concept_and_development",
        "llm_application_field",
        "application_field",
        "?쒖슜遺꾩빞",
        "怨쇱젣 紐⑺몴",
    )
    detail_text = first_non_empty(
        base_row,
        "llm_development_content",
        "development_content",
        "?곸꽭?댁슜",
        "吏?먮궡??,
        "?곸꽭 ?댁뿭",
    )
    support_need_text = first_non_empty(base_row, "llm_support_need", "support_need")
    support_plan_text = first_non_empty(base_row, "llm_support_plan", "support_plan")
    eligibility_text = first_non_empty(base_row, "llm_eligibility", "eligibility", "吏?먮??)
    total_budget_text = first_non_empty(base_row, "llm_total_budget_text", "total_budget_text", "budget")
    per_project_budget_text = first_non_empty(base_row, "llm_per_project_budget_text", "per_project_budget_text")
    period_value = clean(period_text) or first_non_empty(
        base_row,
        "rfp_period",
        "project_period",
        "llm_project_period",
        "notice_period",
        "period",
    ) or first_non_empty(notice_row, "?묒닔湲곌컙", "?좎껌湲곌컙", "period")

    benefit_text = _build_benefit_text(
        total_budget_text=total_budget_text,
        per_project_budget_text=per_project_budget_text,
        eligibility_text=eligibility_text,
        period_text=period_value,
        support_plan_text=support_plan_text,
        support_need_text=support_need_text,
    )

    summary_text = build_project_analysis_text(notice_row, base_row)

    overview_steps = [
        {"title": "?ъ뾽 媛쒖슂 諛?諛곌꼍", "body": _join_display_blocks(background_text, support_need_text, max_items=3)},
        {"title": "怨쇱젣 紐⑺몴", "body": objective_text},
        {"title": "怨쇱젣 ?댁슜", "body": _join_display_blocks(detail_text, support_plan_text, max_items=3)},
        {"title": "吏??댁슜 諛??쒗깮", "body": benefit_text},
    ]

    return {
        "summary_text": summary_text or "-",
        "background_text": background_text or "-",
        "objective_text": objective_text or "-",
        "detail_text": detail_text or "-",
        "support_need_text": support_need_text or "-",
        "support_plan_text": support_plan_text or "-",
        "eligibility_text": eligibility_text or "-",
        "total_budget_text": total_budget_text or "-",
        "per_project_budget_text": per_project_budget_text or "-",
        "period_text": period_value or "-",
        "overview_steps": overview_steps,
    }

def render_notice_detail_from_row(row: dict, opportunity_df: pd.DataFrame) -> None:
    render_notice_detail_page_module(st, row, opportunity_df, api=sys.modules[__name__])

def render_summary_detail_from_row(row: dict, opportunity_df: pd.DataFrame) -> None:
    if not row:
        st.info("?쒖떆??붿빟 怨듦퀬媛 ?놁뒿?덈떎.")
        return

    render_detail_header(
        title=clean(row.get("怨듦퀬紐?)),
        kicker="Summary Detail",
        chips=[
            (clean(row.get("??쒖텛泥쒕룄")), "accent"),
            (clean(row.get("異붿쿇??諛??먯닔")), "neutral"),
            (clean(row.get("?꾨Ц湲곌?")), "neutral"),
            (clean(row.get("怨듦퀬?쇱옄")), "neutral"),
        ],
    )

    top_left, top_right = st.columns([2, 1])
    with top_left:
        render_detail_card(
            "???怨쇱젣 遺꾩꽍",
            [
                ("?대떦 怨쇱젣紐?, row.get("?대떦 怨쇱젣紐?)),
                ("異붿쿇??諛??먯닔", row.get("異붿쿇??諛??먯닔")),
                ("?덉궛", row.get("?덉궛")),
                ("怨쇱젣??, row.get("怨쇱젣??)),
                ("臾몄꽌??, row.get("臾몄꽌??)),
            ],
        )
    with top_right:
        render_detail_card(
            "怨듦퀬 ?앸퀎 ?뺣낫",
            [
                ("怨듦퀬ID", row.get("怨듦퀬ID")),
                ("怨듦퀬踰덊샇", row.get("怨듦퀬踰덊샇")),
                ("?꾨Ц湲곌?", row.get("?꾨Ц湲곌?")),
                ("?뚭?遺泥?, row.get("?뚭?遺泥?)),
                ("寃??щ?", row.get("寃??щ?")),
            ],
        )

    action_left, action_right = st.columns([1, 2])
    with action_left:
        detail_link = resolve_external_detail_link(row)
        if detail_link:
            st.link_button("IRIS ?곸꽭 諛붾줈媛湲?, detail_link, use_container_width=True)
    with action_right:
        st.caption("Summary????怨쇱젣 湲곗??쇰줈 怨듦퀬瑜??붿빟?댁꽌 蹂댁뿬以띾땲??")

    related = pd.DataFrame()
    if not opportunity_df.empty and "notice_id" in opportunity_df.columns:
        notice_key = normalize_notice_id_for_match(row.get("怨듦퀬ID"))
        related = opportunity_df[
            opportunity_df["notice_id"].apply(normalize_notice_id_for_match).eq(notice_key)
        ].copy()
        if not related.empty:
            related = related.sort_values(
                by=["rfp_score", "project_name"],
                ascending=[False, True],
                na_position="last",
            )
    top_related = related.iloc[0].to_dict() if not related.empty else {}

    st.markdown('<div class="detail-section-title">寃??곹깭</div>', unsafe_allow_html=True)
    left, right = st.columns(2)
    with left:
        render_detail_card(
            "怨듦퀬 ?뺣낫",
            [
                ("怨듦퀬?쇱옄", row.get("怨듦퀬?쇱옄")),
                ("怨듦퀬?곹깭", row.get("怨듦퀬?곹깭")),
                ("?묒닔湲곌컙", row.get("?묒닔湲곌컙")),
                ("is_current", row.get("is_current")),
            ],
        )
    with right:
        render_review_editor(
            notice_id=clean(row.get("怨듦퀬ID")),
            current_value=clean(row.get("寃??щ?")),
            form_key=f"summary_review_form_{clean(row.get('怨듦퀬ID'))}",
        )

    st.markdown('<div class="detail-section-title">???遺꾩꽍 ?붿빟</div>', unsafe_allow_html=True)
    render_detail_card(
        "怨쇱젣 遺꾩꽍",
        [
            ("異붿쿇 ?댁쑀", first_non_empty(top_related, "llm_reason", "reason", "??쒖텛泥쒖씠??)),
            (
                "媛쒕뀗 諛?媛쒕컻 ?댁슜",
                first_non_empty(
                    top_related,
                    "llm_concept_and_development",
                    "concept_and_development",
                    "媛쒕뀗 諛?媛쒕컻 ?댁슜",
                ),
            ),
            (
                "吏?먰븘?붿꽦(怨쇱젣 諛곌꼍)",
                first_non_empty(
                    top_related,
                    "llm_support_necessity",
                    "support_necessity",
                    "吏?먰븘?붿꽦(怨쇱젣 諛곌꼍)",
                    "llm_technical_background",
                    "technical_background",
                ),
            ),
            (
                "?쒖슜遺꾩빞",
                first_non_empty(
                    top_related,
                    "llm_application_field",
                    "application_field",
                    "?쒖슜遺꾩빞",
                ),
            ),
            (
                "吏?먭린媛?諛??덉궛쨌異붿쭊泥닿퀎",
                first_non_empty(
                    top_related,
                    "llm_support_plan",
                    "support_plan",
                    "吏?먭린媛?諛??덉궛쨌異붿쭊泥닿퀎",
                ),
            ),
            ("??쒓낵?쒕챸", first_non_empty(top_related, "llm_project_name", "project_name", "??쒓낵?쒕챸")),
            ("??쒖삁??, first_non_empty(top_related, "llm_total_budget_text", "total_budget_text", "budget", "??쒖삁??)),
            ("??쒗궎?뚮뱶", first_non_empty(top_related, "llm_keywords", "keywords", "??쒗궎?뚮뱶")),
            ("??쒓??ъ쁺??, first_non_empty(top_related, "target_market", "??쒓??ъ쁺??)),
        ],
    )

    render_notice_comments(row, section_key=f"summary_{clean(row.get('怨듦퀬ID'))}")

def render_notice_page_with_scope(
    source_df: pd.DataFrame,
    opportunity_df: pd.DataFrame,
    *,
    page_key: str,
    title: str,
    default_status_scope: str,
    current_only_default: bool,
    archive: bool = False,
    already_scoped: bool = False,
) -> None:
    current_view, selected_notice_id = get_route_state(page_key)

    if current_view == "detail":
        selected_row = get_row_by_column_value(source_df, "怨듦퀬ID", selected_notice_id)
        action_col, info_col = st.columns([1, 5])
        with action_col:
            if st.button("RFP Dashboard濡?, key=f"{page_key}_back_to_dashboard", use_container_width=True):
                navigate_to_route("dashboard", "dashboard")
        with info_col:
            st.markdown('<div class="page-note">RFP 異붿쿇 ?붾㈃?먯꽌 ?곌껐??怨듦퀬 ?곸꽭瑜??뺤씤?섎뒗 ?붾㈃?낅땲??</div>', unsafe_allow_html=True)
        render_notice_detail_from_row(selected_row, opportunity_df)
        return

    subtitle = "?섏쭛??怨듦퀬瑜??곹깭? 湲곌? 湲곗??쇰줈 ?뺣━??遊낅땲??"
    if archive:
        subtitle = "醫낅즺?섏뿀嫄곕굹 蹂닿? ??곸쑝濡?遺꾨쪟??怨듦퀬瑜?紐⑥븘 遊낅땲??"
    elif default_status_scope == "?덉젙":
        subtitle = "?덉젙 怨듦퀬? ?묒닔 ?덉젙 嫄댁쓣 癒쇱? ?뺤씤?⑸땲??"
    render_page_header(title, subtitle, eyebrow="Notice")

    filtered = source_df.copy()
    if not already_scoped:
        filtered = filter_archived_notice_rows(filtered) if archive else filter_current_notice_rows(filtered)
    filtered["?ъ뾽鍮?] = series_from_candidates(filtered, ["?ъ뾽鍮?, "??쒖삁??]).apply(extract_budget_summary)
    search_text, current_only, status_scope = render_notice_filter_sidebar(
        page_key,
        current_only_default=current_only_default,
        status_default=default_status_scope,
        show_current_only=not archive,
        show_status_scope=not archive,
    )
    if current_only and "is_current" in filtered.columns:
        filtered = filtered[filtered["is_current"].fillna("").eq("Y")]
    if not archive:
        filtered = filter_notice_status_scope(filtered, status_scope)
    filtered = apply_multiselect_filter(filtered, "?꾨Ц湲곌?", "?꾨Ц湲곌?", f"{page_key}_agency")
    filtered = apply_multiselect_filter(filtered, "?뚭?遺泥?, "?뚭?遺泥?, f"{page_key}_ministry")
    filtered = apply_multiselect_filter(filtered, "寃??щ?", "寃??щ?", f"{page_key}_review")

    filtered = filtered[
        build_contains_mask(
            filtered,
            ["怨듦퀬紐?, "怨듦퀬踰덊샇", "?꾨Ц湲곌?", "?뚭?遺泥?, "怨듦퀬ID", "??쒓낵?쒕챸"],
            search_text,
        )
    ]

    render_metrics(
        [
            ("怨듦퀬 ??, str(len(filtered))),
            ("?꾩옱 怨듦퀬", str(int((filtered["is_current"] == "Y").sum()) if "is_current" in filtered.columns else 0)),
            ("?꾨Ц湲곌? ??, str(filtered["?꾨Ц湲곌?"].nunique() if "?꾨Ц湲곌?" in filtered.columns else 0)),
            ("寃??꾨즺", str(int(filtered["寃??щ?"].fillna("").ne("").sum()) if "寃??щ?" in filtered.columns else 0)),
        ]
    )

    render_section_label("Notice List")
    st.markdown(
        f'<div class="page-note">怨듦퀬紐??먮뒗 怨쇱젣紐낆쓣 ?대┃?섎㈃ ?곸꽭 ?섏씠吏濡??대룞?⑸땲?? ?꾩옱 {len(filtered)}嫄?/div>',
        unsafe_allow_html=True,
    )
    render_clickable_table(
        filtered,
        NOTICE_PREFERRED_COLUMNS,
        page_key=page_key,
        id_column="怨듦퀬ID",
    )

def render_opportunity_detail_from_row(row: dict) -> None:
    render_rfp_detail_page_module(st, row, api=sys.modules[__name__])

def render_summary_page(df: pd.DataFrame, opportunity_df: pd.DataFrame) -> None:
    del df

    working = ensure_opportunity_row_ids(filter_rankable_opportunity_rows(filter_current_opportunity_rows(opportunity_df.copy())))
    if working.empty:
        st.info("?쒖떆??遺꾩꽍 ??곸씠 ?놁뒿?덈떎.")
        return

    selected_row_id = clean(get_query_param("id"))
    if not selected_row_id or selected_row_id not in working["_row_id"].fillna("").astype(str).tolist():
        working = working.sort_values(by=["rfp_score", "project_name"], ascending=[False, True], na_position="last")
        selected_row_id = clean(working.iloc[0].get("_row_id"))

    selected_row = get_row_by_column_value(working, "_row_id", selected_row_id)
    render_opportunity_detail_from_row(selected_row)

# END ADMIN ALIGNMENT OVERRIDES

# BEGIN VIEWER LAYOUT OVERRIDES

def inject_viewer_layout_styles() -> None:
    st.markdown(
        """
        <style>
        [data-testid="stSidebar"],
        section[data-testid="stSidebar"],
        [data-testid="collapsedControl"] {
          display: none !important;
        }
        .notice-queue-list {
          display: flex;
          flex-direction: column;
          gap: 0.65rem;
          margin-top: 88px;
        }
        .notice-queue-row {
          display: grid;
          grid-template-columns: 96px minmax(0, 1fr) 240px;
          align-items: center;
          gap: 1rem;
          padding: 1rem 1.1rem;
          border-radius: 18px;
          border: 1px solid var(--border);
          background: var(--surface);
          text-decoration: none !important;
          box-shadow: var(--shadow);
        }
        .notice-queue-row:hover {
          border-color: var(--border-strong);
          transform: translateY(-1px);
        }
        .notice-queue-date {
          color: var(--text-muted);
          font-size: 0.9rem;
          font-weight: 800;
        }
        .notice-queue-title {
          color: var(--text-strong);
          font-size: 1rem;
          font-weight: 850;
          line-height: 1.45;
        }
        .notice-queue-period {
          color: var(--text-muted);
          font-size: 0.92rem;
          font-weight: 700;
          text-align: right;
        }
        @media (max-width: 980px) {
          .notice-queue-row {
            grid-template-columns: 1fr;
          }
          .notice-queue-period {
            text-align: left;
          }
        }
        </style>
        """,
        unsafe_allow_html=True,
    )


def apply_multiselect_filter(df: pd.DataFrame, column: str, label: str, key: str) -> pd.DataFrame:
    del column, label, key
    return df


def render_sidebar_search(key: str = "sidebar_search") -> str:
    del key
    return ""


def render_notice_filter_sidebar(
    key_prefix: str,
    *,
    current_only_default: bool = True,
    status_default: str = "??",
    show_current_only: bool = True,
    show_status_scope: bool = True,
) -> tuple[str, bool, str]:
    del key_prefix, show_current_only, show_status_scope
    return "", current_only_default, status_default


def build_crawled_notice_collection(
    datasets: dict[str, pd.DataFrame],
    source_datasets: dict[str, object] | None,
) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []

    def append_frame(df: pd.DataFrame, *, source_key: str, source_label: str, scope: str) -> None:
        if df is None or df.empty:
            return
        normalized = normalize_favorite_notice_df(df, source_key=source_key, source_label=source_label)
        if normalized.empty:
            return
        normalized["_notice_scope"] = scope
        normalized["_collection_id"] = normalized.apply(
            lambda row: f"{source_key}::{scope}::{clean(first_non_empty(row, '怨듦퀬ID', 'notice_id'))}",
            axis=1,
        )
        frames.append(normalized)

    append_frame(datasets.get("notice_current", pd.DataFrame()), source_key="iris", source_label="IRIS", scope="current")
    append_frame(datasets.get("pending", pd.DataFrame()), source_key="iris", source_label="IRIS", scope="scheduled")
    append_frame(datasets.get("notice_archive", pd.DataFrame()), source_key="iris", source_label="IRIS", scope="archive")

    source_datasets = source_datasets or {}
    append_frame(source_datasets.get("mss_current", pd.DataFrame()), source_key="tipa", source_label="MSS", scope="current")
    append_frame(source_datasets.get("mss_past", pd.DataFrame()), source_key="tipa", source_label="MSS", scope="archive")
    append_frame(source_datasets.get("nipa_current", pd.DataFrame()), source_key="nipa", source_label="NIPA", scope="current")
    append_frame(source_datasets.get("nipa_past", pd.DataFrame()), source_key="nipa", source_label="NIPA", scope="archive")

    if not frames:
        return pd.DataFrame()

    combined = pd.concat(frames, ignore_index=True)
    if "source_key" not in combined.columns:
        combined["source_key"] = series_from_candidates(combined, ["source_key", "_source_key"])
    else:
        fallback_source = series_from_candidates(combined, ["source_key", "_source_key"])
        combined["source_key"] = combined["source_key"].where(
            combined["source_key"].fillna("").astype(str).str.strip().ne(""),
            fallback_source,
        )
    combined = combined.drop_duplicates(subset=["_collection_id"], keep="first")
    return combined.sort_values(
        by=["_sort_date", "留ㅼ껜", "怨듦퀬紐?],
        ascending=[False, True, True],
        na_position="last",
    )




def filter_notice_queue_rows(rows: pd.DataFrame, *, search_text: str) -> pd.DataFrame:
    if rows.empty or not clean(search_text):
        return rows.copy()
    return rows[
        build_contains_mask(
            rows,
            ["留ㅼ껜", "怨듦퀬紐?, "怨듦퀬踰덊샇", "?꾨Ц湲곌?", "?대떦遺??, "?뚭?遺泥?, "怨듦퀬ID", "怨듦퀬?곹깭", "?묒닔湲곌컙"],
            search_text,
        )
    ].copy()


def clear_widget_value(widget_key: str) -> None:
    st.session_state[widget_key] = ""


def normalize_notice_queue_filter(value: str) -> str:
    normalized = clean(value).lower()
    alias_map = {
        "all": "all",
        "iris": "iris",
        "mss": "mss",
        "tipa": "mss",
        "nipa": "nipa",
        "archive": "archive",
        "closed": "archive",
    }
    return alias_map.get(normalized, "all")


def get_notice_queue_filter_state_key(page_key: str) -> str:
    return f"{page_key}_selected_queue_filter"


def build_notice_queue_filter_href(*, page_key: str, filter_value: str) -> str:
    params = get_query_params_dict()
    params["page"] = normalize_route_page_key(page_key)
    params["view"] = "table"
    params["queue_filter_select"] = normalize_notice_queue_filter(filter_value)
    params = with_auth_params(params)
    return f"?{urlencode(params)}"


def consume_notice_queue_filter_query_action(*, page_key: str, state_key: str) -> None:
    selected_filter = get_query_param("queue_filter_select")
    if not clean(selected_filter):
        st.session_state.setdefault(state_key, "all")
        return

    st.session_state[state_key] = normalize_notice_queue_filter(selected_filter)
    params = get_query_params_dict()
    params["page"] = normalize_route_page_key(page_key)
    params["view"] = "table"
    params.pop("queue_filter_select", None)
    replace_query_params(params)
    st.rerun()


def reset_notice_queue_controls(search_key: str, filter_state_key: str) -> None:
    st.session_state[search_key] = ""
    st.session_state[filter_state_key] = "all"


def apply_notice_queue_kpi_filter(rows: pd.DataFrame, selected_filter: str) -> pd.DataFrame:
    if rows.empty:
        return rows.copy()

    normalized_filter = normalize_notice_queue_filter(selected_filter)
    if normalized_filter == "iris":
        mask = rows["source_key"].eq("iris") & rows["_notice_scope"].isin(["current", "scheduled"])
    elif normalized_filter == "mss":
        mask = rows["source_key"].eq("tipa") & rows["_notice_scope"].eq("current")
    elif normalized_filter == "nipa":
        mask = rows["source_key"].eq("nipa") & rows["_notice_scope"].eq("current")
    elif normalized_filter == "archive":
        mask = rows["_notice_scope"].eq("archive")
    else:
        return rows.copy()
    return rows[mask].copy()


def build_notice_queue_metric_items(rows: pd.DataFrame) -> list[tuple[str, str, str]]:
    iris_rows = rows[rows["source_key"].eq("iris") & rows["_notice_scope"].isin(["current", "scheduled"])]
    mss_rows = rows[rows["source_key"].eq("tipa") & rows["_notice_scope"].eq("current")]
    nipa_rows = rows[rows["source_key"].eq("nipa") & rows["_notice_scope"].eq("current")]
    archive_rows = rows[rows["_notice_scope"].eq("archive")]
    return [
        ("?꾩껜 怨듦퀬", str(len(rows)), "all"),
        ("IRIS", str(len(iris_rows)), "iris"),
        ("MSS", str(len(mss_rows)), "mss"),
        ("NIPA", str(len(nipa_rows)), "nipa"),
        ("留덇컧쨌蹂닿?", str(len(archive_rows)), "archive"),
    ]


def render_notice_queue_kpi_cards(
    items: list[tuple[str, str, str]],
    *,
    page_key: str,
    selected_filter: str,
) -> None:
    if not items:
        return

    active_filter = normalize_notice_queue_filter(selected_filter)
    cards: list[str] = []
    for label, value, filter_value in items:
        normalized_filter = normalize_notice_queue_filter(filter_value)
        active_class = " is-active" if normalized_filter == active_filter else ""
        href = build_notice_queue_filter_href(page_key=page_key, filter_value=normalized_filter)
        cards.append(
            (
                f'<div class="notice-kpi-card{active_class}">'
                f'<div class="notice-kpi-label">{escape(clean(label))}</div>'
                f'<div class="notice-kpi-value">{escape(clean(value))}</div>'
                "</div>"
            )
        )
    st.markdown(f'<div class="notice-kpi-grid">{"".join(cards)}</div>', unsafe_allow_html=True)






def render_opportunity_page(
    df: pd.DataFrame,
    *,
    page_key: str | None = None,
    title: str | None = None,
    archive: bool = False,
    all_df: pd.DataFrame | None = None,
) -> None:
    page_key = page_key or ("opportunity_archive" if archive else "opportunity")
    title = title or ("Opportunity Archive" if archive else "RFP Queue")
    source_df = ensure_opportunity_row_ids(df)
    all_source_df = ensure_opportunity_row_ids(all_df) if all_df is not None and not all_df.empty else source_df
    default_route = route_core.build_rfp_queue_route()
    current_route = route_core.get_current_route(default_route)

    if clean(current_route.get("page")) == page_key and clean(current_route.get("view")) == "detail":
        selected_document_id = clean(current_route.get("item_id"))
        selected_row = get_row_by_column_value(source_df, "_row_id", selected_document_id)
        if selected_row is None and not all_source_df.empty:
            selected_row = get_row_by_column_value(all_source_df, "_row_id", selected_document_id)
        back_col, info_col = st.columns([1.8, 4.2])
        with back_col:
            if st.button(f"??{title}濡??뚯븘媛湲?, key=f"{page_key}_back_to_table_ui", use_container_width=False, type="secondary"):
                go_back_route(route_core.build_rfp_queue_route())
                st.rerun()
        with info_col:
            st.markdown(f'<div class="page-note">{escape(title)} / RFP Detail</div>', unsafe_allow_html=True)
        render_opportunity_detail_from_row(selected_row)
        return

    render_page_header(
        title,
        "遺꾩꽍 ?꾨즺??Opportunity瑜?異붿쿇?쒖쑝濡?寃?좏븯??硫붿씤 Intelligence Workspace?낅땲??" if not archive else "蹂닿??Opportunity 臾띠쓬??寃??대젰 湲곗??쇰줈 ?ㅼ떆 ?먯깋??덉뒿?덈떎.",
        eyebrow="Opportunity",
    )
    st.markdown(
        '<div class="queue-shell-note">異붿쿇 ?곹깭? 怨듦퀬 ?곹깭留?鍮좊Ⅴ寃?醫곹엳怨? ?곸쐞 Opportunity瑜?移대뱶 罹먮윭?濡??섍꺼蹂대㈃??諛붾줈 ?곸꽭 寃?좊줈 ?댁뼱吏???덇쾶 援ъ꽦?덉뒿?덈떎.</div>',
        unsafe_allow_html=True,
    )

    base_rows = filter_archived_opportunity_rows(all_source_df) if archive else filter_current_opportunity_rows(source_df)
    base_rows = filter_rankable_opportunity_rows(base_rows)
    working = _build_queue_filter_frame(base_rows)
    option_rows = filter_archived_opportunity_rows(all_source_df) if archive else all_source_df
    option_working = _build_queue_filter_frame(option_rows)
    if working.empty and option_working.empty:
        st.info("?쒖떆??RFP媛 ?놁뒿?덈떎.")
        return

    recommendation_options = build_queue_recommendation_options(working["_queue_recommendation"]) if not working.empty else []
    status_options = build_queue_status_options(option_working["_queue_status"]) if not option_working.empty else ["留덇컧"]
    application_field_series = series_from_candidates(
        option_working if not option_working.empty else working,
        ["llm_application_field", "application_field", "?쒖슜遺꾩빞"],
    ).fillna("").astype(str).str.strip()
    application_field_options = sorted({value for value in application_field_series.tolist() if clean(value) and value != "-"})
    archive_reason_options = sorted(
        [
            value
            for value in working["_queue_archive_reason"].dropna().astype(str).unique().tolist()
            if clean(value) and value != "-"
        ]
    ) if not working.empty else []
    route_filters = dict(current_route.get("filters") or {})
    recommendation_key = f"{page_key}_filter_recommendation"
    status_key = f"{page_key}_filter_status"
    deadline_key = f"{page_key}_filter_deadline"
    field_key = f"{page_key}_filter_field"
    review_key = f"{page_key}_filter_review"
    sort_key = f"{page_key}_sort"
    archive_reason_key = f"{page_key}_filter_archive_reason"
    st.session_state.setdefault(recommendation_key, route_filters.get("recommendation", []))
    st.session_state.setdefault(status_key, route_filters.get("status", []))
    st.session_state.setdefault(deadline_key, route_filters.get("deadline", []))
    st.session_state.setdefault(field_key, route_filters.get("field", []))
    st.session_state.setdefault(review_key, route_filters.get("review", []))
    st.session_state.setdefault(sort_key, clean(route_filters.get("sort")) or "異붿쿇??)
    if archive:
        st.session_state.setdefault(archive_reason_key, route_filters.get("archive_reason", []))

    _inject_opportunity_workspace_styles()
    filter_cols = st.columns(5 if archive else 4)
    with filter_cols[0]:
        selected_recommendation = st.multiselect(
            "異붿쿇 ?곹깭",
            options=recommendation_options,
            key=f"{page_key}_filter_recommendation",
            placeholder="?꾩껜",
        )
    with filter_cols[1]:
        selected_status = st.multiselect(
            "怨듦퀬 ?곹깭",
            options=status_options,
            key=f"{page_key}_filter_status",
            placeholder="?꾩껜",
        )
    with filter_cols[2]:
        selected_deadline = st.multiselect(
            "D-day",
            options=["吏꾪뻾以?, "7??대궡", "30??대궡", "?덉젙", "留덇컧"],
            key=deadline_key,
            placeholder="?꾩껜",
        )
    with filter_cols[3]:
        selected_field = st.multiselect(
            "?곌뎄遺꾩빞",
            options=application_field_options,
            key=field_key,
            placeholder="?꾩껜",
        )

    selected_archive_reason: list[str] = []
    if archive:
        with filter_cols[4]:
            selected_archive_reason = st.multiselect(
                "蹂닿? ?ъ쑀",
                options=archive_reason_options,
                key=archive_reason_key,
                placeholder="?꾩껜",
            )
    sort_col, spacer_col = st.columns([1.2, 4.8])
    with sort_col:
        sort_option = st.selectbox(
            "?뺣젹",
            options=["異붿쿇??, "留덇컧?꾨컯??, "怨쇱젣紐낆닚"],
            key=sort_key,
        )
    with spacer_col:
        st.markdown("", unsafe_allow_html=True)

    include_closed = archive or ("留덇컧" in selected_status)
    filter_source = (
        filter_archived_opportunity_rows(all_source_df)
        if archive
        else (all_source_df if include_closed else filter_current_opportunity_rows(source_df))
    )
    filter_source = filter_rankable_opportunity_rows(filter_source)
    filtered = filter_queue_working_frame(
        _build_queue_filter_frame(filter_source),
        selected_recommendation=selected_recommendation,
        selected_status=selected_status,
        archive=archive,
    )
    if selected_archive_reason:
        filtered = filtered[filtered["_queue_archive_reason"].isin(selected_archive_reason)]
    if selected_field:
        field_source = series_from_candidates(filtered, ["llm_application_field", "application_field", "?쒖슜遺꾩빞"]).fillna("").astype(str)
        filtered = filtered[field_source.apply(lambda value: any(option in value for option in selected_field))]
    selected_review = st.session_state.get(review_key, [])
    if selected_review:
        review_source = series_from_candidates(filtered, ["Review", "review_status", "寃??щ?", "寃?좎뿬遺"]).fillna("").astype(str).str.strip()
        filtered = filtered[review_source.isin(selected_review)].copy()
    if selected_deadline:
        today = pd.Timestamp.now().normalize()

        def _deadline_bucket_match(row: pd.Series) -> bool:
            buckets: set[str] = set()
            deadline = row.get("_queue_deadline_sort")
            status_text = clean(row.get("_queue_status"))
            if "留덇컧" in status_text or bool(row.get("_queue_is_closed")):
                buckets.add("留덇컧")
            elif "?덉젙" in status_text:
                buckets.add("?덉젙")
            else:
                buckets.add("吏꾪뻾以?)
            if pd.notna(deadline):
                days_left = int((deadline.normalize() - today).days)
                if days_left <= 7:
                    buckets.add("7??대궡")
                if days_left <= 30:
                    buckets.add("30??대궡")
            return any(option in buckets for option in selected_deadline)

        filtered = filtered[filtered.apply(_deadline_bucket_match, axis=1)]

    if filtered.empty:
        st.info("寃??議곌굔??留욌뒗 RFP媛 ?놁뒿?덈떎.")
        return

    if sort_option == "留덇컧?꾨컯??:
        filtered = filtered.sort_values(
            by=["_queue_deadline_sort", "_queue_sort_score", "_queue_project_sort"],
            ascending=[True, False, True],
            na_position="last",
        )
    elif sort_option == "怨쇱젣紐낆닚":
        filtered = filtered.sort_values(
            by=["_queue_project_sort", "_queue_sort_score"],
            ascending=[True, False],
            na_position="last",
        )
    else:
        filtered = filtered.sort_values(
            by=["_queue_sort_score", "_queue_deadline_sort", "_queue_project_sort"],
            ascending=[False, True, True],
            na_position="last",
        )

    selected_document_id = clean(current_route.get("item_id")) if clean(current_route.get("view")) == "summary" else ""
    current_filters = {
        "recommendation": selected_recommendation,
        "status": selected_status,
        "deadline": selected_deadline,
        "field": selected_field,
        "review": selected_review,
        "sort": sort_option,
        "archive_reason": selected_archive_reason,
    }

    def _select_rfp_preview(row: pd.Series) -> None:
        row_id = clean(first_non_empty(row, "_row_id", "Row ID"))
        route = route_core.build_rfp_queue_route(
            filters=current_filters,
            page_no=1,
            page_size=20,
            view="summary",
            item_id=row_id,
            source_key=resolve_route_source_key_for_row(row) or "iris",
        )
        route["page"] = page_key
        route["source"] = "iris"
        route_core.set_current_route(route)
        replace_query_params(with_auth_params(route_core.serialize_route(route)))

    display_col, summary_col = st.columns([5.4, 2.15], gap="large")
    with display_col:
        st.markdown('<div class="queue-results-label">異붿쿇 寃곌낵</div>', unsafe_allow_html=True)
        _render_recommended_opportunity_cards(
            filtered.head(30),
            page_key=page_key,
            carousel_key=f"{page_key}_carousel",
            visible_count=4,
            show_rank=not archive,
            selected_item_id=selected_document_id,
            on_select=_select_rfp_preview,
        )
    with summary_col:
        selected_row = get_row_by_column_value(filtered, "_row_id", selected_document_id)
        if selected_row is None and not all_source_df.empty and selected_document_id:
            selected_row = get_row_by_column_value(all_source_df, "_row_id", selected_document_id)
        _render_rfp_preview_panel(
            selected_row,
            panel_key=f"{page_key}_preview",
            empty_title="RFP瑜??좏깮?섎㈃ 誘몃━蹂닿린媛 ?대┰?덈떎.",
            empty_copy="移대뱶 ?좏깮? ?곗륫 Preview Panel留?媛깆떊?섍퀬, ?곸꽭 ?붾㈃? 踰꾪듉??뚮???뚮쭔 ?꾪솚?⑸땲??",
            close_callback=lambda: (
                route_core.set_current_route(
                    route_core.build_rfp_queue_route(
                        filters=current_filters,
                        page_no=1,
                        page_size=20,
                        view="list",
                        item_id="",
                    )
                ),
                replace_query_params(with_auth_params(route_core.serialize_route(route_core.get_current_route()))),
                st.rerun(),
            ),
        )

    route_snapshot = route_core.build_rfp_queue_route(
        filters=current_filters,
        page_no=1,
        page_size=20,
        view="summary" if selected_document_id else "list",
        item_id=selected_document_id,
        source_key=clean(current_route.get("source_key")) or "iris",
    )
    route_snapshot["page"] = page_key
    route_snapshot["source"] = "iris"
    route_core.set_current_route(route_snapshot)
    replace_query_params(with_auth_params(route_core.serialize_route(route_snapshot)))

# END VIEWER LAYOUT OVERRIDES


def render_notice_queue_ui_styles() -> None:
    st.markdown(
        """
        <style>
        .notice-queue-footer {
          display: flex;
          flex-direction: column;
          align-items: center;
          justify-content: center;
          gap: 0.75rem;
          margin-top: 1.15rem;
          padding: 0.2rem 0 0.4rem;
        }
        .notice-queue-footer-meta {
          color: #6b7280;
          font-size: 0.82rem;
          font-weight: 700;
          line-height: 2.15rem;
          white-space: nowrap;
        }
        .notice-queue-page-slot {
          display: flex;
          align-items: center;
          justify-content: center;
          width: 100%;
        }
        .notice-queue-pagination-wrap {
          display: flex;
          flex-direction: column;
          align-items: center;
          gap: 0.8rem;
        }
        .notice-queue-pagination {
          display: inline-flex;
          align-items: center;
          justify-content: center;
          gap: 0.7rem;
        }
        .notice-queue-page-nav {
          display: inline-flex;
          align-items: center;
          justify-content: center;
          color: #4b5563 !important;
          font-size: 1rem;
          font-weight: 800;
          text-decoration: none !important;
        }
        .notice-queue-page-nav.is-disabled {
          color: #cbd5e1 !important;
          pointer-events: none;
        }
        .notice-queue-page-link {
          display: inline-flex;
          align-items: center;
          justify-content: center;
          min-width: 38px;
          height: 38px;
          padding: 0 0.62rem;
          border: 1px solid transparent;
          border-radius: 9px;
          background: transparent;
          color: #4b5563 !important;
          font-size: 1rem;
          font-weight: 800;
          line-height: 1;
          text-decoration: none !important;
        }
        .notice-queue-page-link.is-active {
          border-color: #2563eb;
          background: #2563eb;
          color: #ffffff !important;
        }
        .notice-queue-page-ellipsis {
          display: inline-flex;
          align-items: center;
          justify-content: center;
          min-width: 34px;
          height: 38px;
          color: #4b5563;
          font-size: 1.05rem;
          font-weight: 900;
        }
        .notice-queue-page-jump {
          display: inline-flex;
          align-items: center;
          justify-content: center;
          gap: 0.55rem;
        }
        .notice-queue-page-jump input {
          width: 64px;
          height: 38px;
          border: 1px solid #6b7280;
          border-radius: 8px;
          color: #4b5563;
          font-size: 0.98rem;
          font-weight: 800;
          text-align: center;
          background: #ffffff;
        }
        .notice-queue-page-jump-total {
          color: #4b5563;
          font-size: 0.98rem;
          font-weight: 800;
        }
        .notice-queue-page-jump button {
          height: 38px;
          padding: 0 1rem;
          border: 1px solid #3b82f6;
          border-radius: 8px;
          background: #eff6ff;
          color: #2563eb;
          font-size: 0.96rem;
          font-weight: 900;
          cursor: pointer;
        }
        .notice-kpi-grid {
          display: grid;
          grid-template-columns: repeat(5, minmax(0, 1fr));
          gap: 1rem;
          margin: 1.35rem 0 1rem;
        }
        .notice-kpi-card {
          display: block;
          padding: 1.15rem 1.35rem;
          border-radius: 24px;
          border: 1px solid #e2e8f0;
          background: #ffffff;
          text-decoration: none !important;
          box-shadow: 0 16px 36px rgba(148, 163, 184, 0.10);
          cursor: pointer;
          transition: border-color 140ms ease, background-color 140ms ease, transform 140ms ease, box-shadow 140ms ease;
        }
        .notice-kpi-card:hover {
          background: #f8fafc;
          border-color: rgba(148, 163, 184, 0.95);
          transform: translateY(-1px);
          box-shadow: 0 20px 42px rgba(148, 163, 184, 0.14);
        }
        .notice-kpi-card.is-active {
          border-color: #2563eb;
          background: #eff6ff;
        }
        .notice-kpi-label {
          color: var(--text-muted);
          font-size: 0.88rem;
          font-weight: 800;
          line-height: 1.4;
        }
        .notice-kpi-value {
          margin-top: 0.6rem;
          color: var(--text-strong);
          font-size: 2.2rem;
          font-weight: 900;
          line-height: 1;
        }
        .notice-kpi-card.is-active .notice-kpi-label,
        .notice-kpi-card.is-active .notice-kpi-value {
          color: #1d4ed8;
        }
        @media (max-width: 960px) {
          .notice-kpi-grid {
            grid-template-columns: repeat(2, minmax(0, 1fr));
          }
        }
        @media (max-width: 640px) {
          .notice-kpi-grid {
            grid-template-columns: 1fr;
          }
        }
        </style>
        """,
        unsafe_allow_html=True,
    )


def _build_public_workspace_route_href(source_key: str, page_key: str) -> str:
    current_route = route_core.get_current_route()
    next_source = clean(source_key)
    next_page = normalize_route_page_key(page_key)
    if not next_source:
        next_source = "dashboard"
    if not next_page:
        next_page = "dashboard"

    if next_page == "dashboard":
        next_route = route_core.build_dashboard_route(
            view="list",
            filters=dict(current_route.get("filters") or {}),
        )
    elif next_page == "notice_queue":
        next_route = route_core.build_notice_queue_route(
            filters=dict(current_route.get("filters") or {}),
            page_no=1,
            page_size=20,
            view="list",
            item_id="",
            source_key=clean(current_route.get("source_key")) or "iris",
        )
    elif next_page == "favorites":
        next_route = route_core.build_favorites_route(
            filters=dict(current_route.get("filters") or {}),
            page_no=1,
            page_size=20,
            view="list",
            item_id="",
            source_key="favorites",
        )
    else:
        next_route = route_core.build_rfp_queue_route(
            filters=dict(current_route.get("filters") or {}),
            page_no=1,
            page_size=20,
            view="list",
            item_id="",
            source_key=clean(current_route.get("source_key")) or "iris",
        )
    params = with_auth_params(route_core.serialize_route(next_route))
    return f"?{urlencode(params)}"


def _inject_public_workspace_shell_styles() -> None:
    st.markdown(
        """
        <style>
        .app-shell {
          min-height: 96px;
          display: grid;
          grid-template-columns: minmax(260px, 320px) minmax(460px, 1fr) auto;
          align-items: center;
          gap: 1.45rem;
          margin: -1.05rem -1rem 0.7rem;
          padding: 1.15rem 1.45rem 1rem;
          background: rgba(255, 255, 255, 0.98);
          border: 1px solid #dbe4f0;
          border-top: none;
          border-left: none;
          border-right: none;
          border-radius: 0 0 24px 24px;
          box-shadow: 0 20px 42px rgba(15, 23, 42, 0.07);
        }
        .app-brand {
          display: flex;
          align-items: center;
          gap: 0.92rem;
          color: #0f172a;
          font-size: 1.1rem;
          font-weight: 800;
          white-space: nowrap;
        }
        .app-brand-mark {
          display: inline-flex;
          align-items: center;
          justify-content: center;
          width: 46px;
          height: 46px;
          color: #ffffff;
          background: linear-gradient(180deg, #3b82f6 0%, #2563eb 100%);
          font-size: 1.12rem;
          font-weight: 800;
          border-radius: 14px;
          box-shadow: 0 14px 28px rgba(37, 99, 235, 0.2);
        }
        .app-brand-copy {
          display: flex;
          align-items: baseline;
          gap: 0.54rem;
        }
        .app-brand-title {
          color: #0f172a;
          font-size: 1.14rem;
          font-weight: 850;
        }
        .app-brand-subtitle {
          color: #475569;
          font-size: 0.84rem;
          font-weight: 650;
        }
        .app-nav {
          display: flex;
          align-items: center;
          gap: 1.6rem;
          min-width: 0;
        }
        .app-nav-item {
          display: inline-flex;
          align-items: center;
          min-height: 54px;
          color: #475569;
          border-bottom: 2px solid transparent;
          font-size: 1.02rem;
          font-weight: 700;
          padding-top: 0.34rem;
          padding-bottom: 0.28rem;
          text-decoration: none !important;
          white-space: nowrap;
        }
        .app-nav-item:hover {
          color: #1d4ed8;
        }
        .app-nav-item-active {
          color: #2563eb;
          border-bottom-color: #2563eb;
        }
        .app-actions {
          display: flex;
          align-items: center;
          justify-content: flex-end;
          gap: 0.8rem;
          min-width: 0;
        }
        .app-icon-button,
        .app-user-menu {
          min-height: 46px;
          display: inline-flex;
          align-items: center;
          justify-content: center;
          padding: 0 1.05rem;
          color: #334155;
          background: #f8fafc;
          border: 1px solid #dbe3ef;
          border-radius: 14px;
          font-size: 0.92rem;
          font-weight: 700;
          white-space: nowrap;
          box-shadow: 0 10px 22px rgba(15, 23, 42, 0.05);
        }
        .app-user-menu {
          align-items: flex-start;
          flex-direction: column;
          gap: 0.1rem;
          min-width: 92px;
          background: #ffffff;
        }
        .app-user-name {
          color: #0f172a;
          font-size: 0.92rem;
          font-weight: 850;
          line-height: 1.1;
        }
        .app-user-role {
          color: #64748b;
          font-size: 0.77rem;
          font-weight: 700;
          line-height: 1.1;
        }
        @media (max-width: 1200px) {
          .app-shell {
            grid-template-columns: 1fr;
            gap: 0.6rem;
            margin-left: -0.7rem;
            margin-right: -0.7rem;
            padding-bottom: 0.75rem;
          }
          .app-actions {
            justify-content: flex-start;
            flex-wrap: wrap;
          }
        }
        @media (max-width: 780px) {
          .app-nav {
            overflow-x: auto;
          }
        }
        </style>
        """,
        unsafe_allow_html=True,
    )


def render_public_workspace_navigation(mode_config: AppModeConfig, current_source: str, current_page: str) -> None:
    _inject_public_workspace_shell_styles()
    nav_items = [
        ("Dashboard", "dashboard", "dashboard"),
        ("RFP Queue", "iris", "rfp_queue"),
        ("Notice Queue", "notices", "notice_queue"),
        ("Favorites", "favorites", "favorites"),
    ]
    nav_links: list[str] = []
    for label, source_key, page_key in nav_items:
        active_class = " app-nav-item-active" if current_source == source_key and current_page == page_key else ""
        href = _build_public_workspace_route_href(source_key, page_key)
        nav_links.append(
            f'<a class="app-nav-item{active_class}" href="{escape(href, quote=True)}" target="_self">{escape(label)}</a>'
        )

    user_label = escape(get_current_user_label() or get_current_user_id() or "User")
    st.markdown(
        (
            '<div class="app-shell">'
            '<div class="app-brand">'
            '<span class="app-brand-mark">X</span>'
            '<span class="app-brand-copy">'
            '<span class="app-brand-title">R&amp;D Opportunity</span>'
            '<span class="app-brand-subtitle">Public Viewer</span>'
            '</span>'
            '</div>'
            f'<nav class="app-nav">{"".join(nav_links)}</nav>'
            '<div class="app-actions">'
            f'<div class="app-user-menu"><span class="app-user-name">{user_label}</span><span class="app-user-role">Researcher</span></div>'
            '</div>'
            '</div>'
        ),
        unsafe_allow_html=True,
    )


def _inject_compact_public_dashboard_styles() -> None:
    st.markdown(
        """
        <style>
        .main .block-container {
          max-width: min(1920px, calc(100vw - 0.2rem));
          padding-left: 0.28rem;
          padding-right: 0.28rem;
          padding-top: 0.18rem;
          padding-bottom: 1rem;
        }
        .app-shell {
          gap: 1.15rem;
          margin-bottom: 0.56rem;
          grid-template-columns: minmax(240px, 300px) minmax(0, 1fr) auto;
        }
        .dashboard-shell {
          padding: 0;
          gap: 0.85rem;
        }
        .dashboard-toolbar-meta {
          min-height: 2.8rem;
          display: flex;
          align-items: center;
          justify-content: flex-end;
          color: #64748b;
          font-size: 0.8rem;
          font-weight: 700;
          white-space: nowrap;
        }
        .st-key-public_dashboard_compact_search_text div[data-baseweb="input"] {
          min-height: 2.8rem;
          border-radius: 14px;
          border-color: #dbe3ef;
          box-shadow: 0 8px 24px rgba(15, 23, 42, 0.04);
        }
        .st-key-public_dashboard_compact_search_text input {
          font-size: 0.9rem;
        }
        .st-key-public_dashboard_compact_search_reset button {
          min-height: 2.8rem;
          border-radius: 14px;
          border: 1px solid #dbe3ef;
          background: #ffffff;
          color: #1e3a8a;
          font-weight: 800;
          box-shadow: 0 8px 24px rgba(15, 23, 42, 0.04);
        }
        .dashboard-section,
        .queue-table-card,
        .summary-panel {
          border-radius: 14px;
          padding: 0.95rem 1rem;
        }
        .dashboard-kpi-grid {
          grid-template-columns: repeat(3, minmax(0, 1fr));
          gap: 0.8rem;
          margin: 0.2rem 0 0.9rem;
        }
        .rfp-card {
          min-height: 208px;
          padding: 0.9rem;
        }
        .rfp-card-title {
          font-size: 0.92rem;
        }
        .rfp-card-notice,
        .rfp-card-analysis,
        .rfp-card-meta,
        .notice-row-meta,
        .notice-row-summary {
          font-size: 0.78rem;
        }
        .notice-row-head,
        .notice-row-body {
          grid-template-columns: minmax(0, 0.95fr) minmax(0, 1.05fr) minmax(0, 3fr) minmax(0, 1.55fr) minmax(0, 1fr) minmax(0, 0.9fr) minmax(0, 1.15fr) minmax(0, 2.2fr) minmax(0, 0.85fr);
        }
        @media (max-width: 980px) {
          .dashboard-kpi-grid {
            grid-template-columns: 1fr;
          }
        }
        </style>
        """,
        unsafe_allow_html=True,
    )


def _filter_public_dashboard_frames(
    opportunity_rows: pd.DataFrame,
    notice_rows: pd.DataFrame,
    query: str,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    search_text = clean(query)
    if not search_text:
        return opportunity_rows, notice_rows

    filtered_opportunities = opportunity_rows
    if not filtered_opportunities.empty:
        filtered_opportunities = filtered_opportunities[
            build_contains_mask(
                filtered_opportunities,
                ["Project", "Notice Title", "Agency", "Ministry", "Keywords", "Reason", "Budget"],
                search_text,
            )
        ].copy()

    filtered_notices = notice_rows
    if not filtered_notices.empty:
        filtered_notices = filtered_notices[
            build_contains_mask(
                filtered_notices,
                ["怨듦퀬紐?, "notice_title", "agency", "?꾨Ц湲곌?", "?뚭?遺泥?, "_queue_analysis", "_queue_project_name", "budget"],
                search_text,
            )
        ].copy()

    return filtered_opportunities, filtered_notices


def _render_public_dashboard_kpi_cards_impl(recommended_rows: pd.DataFrame, notice_rows: pd.DataFrame) -> None:
    recommended_count = len(recommended_rows) if recommended_rows is not None and not recommended_rows.empty else 0
    favorite_count = 0
    if notice_rows is not None and not notice_rows.empty:
        favorite_series = notice_rows.apply(_dashboard_review_value, axis=1)
        favorite_count = int(favorite_series.eq(FAVORITE_REVIEW_STATUS).sum())
    urgent_count = _count_dashboard_urgent_notices(notice_rows)

    cards = [
        ("recommended_rfp", "異붿쿇 RFP", str(recommended_count), "異붿쿇 RFP Queue濡??대룞", "RFP"),
        ("urgent_notice", "留덇컧 ?꾨컯", str(urgent_count), "30??대궡 怨듦퀬 蹂닿린", "D-30"),
        ("favorite_notice", "愿?ш났怨?, str(favorite_count), "利먭꺼李얘린 紐⑥븘蹂닿린", "SAVE"),
    ]
    cols = st.columns(3, gap="medium")
    for column, (card_key, label, value, copy, icon) in zip(cols, cards):
        safe_key = _css_safe_key(f"dashboard_kpi_{card_key}")
        st.markdown(
            f"""
            <style>
            .st-key-{safe_key} button {{
              min-height: 88px !important;
              width: 100% !important;
              padding: 0.85rem 0.95rem !important;
              border-radius: 14px !important;
              border: 1px solid #e2e8f0 !important;
              background: #ffffff !important;
              color: #15233b !important;
              box-shadow: 0 8px 20px rgba(15, 23, 42, 0.04) !important;
              text-align: left !important;
              white-space: pre-line !important;
              line-height: 1.3 !important;
              font-size: 0.82rem !important;
              font-weight: 700 !important;
            }}
            .st-key-{safe_key} button:hover {{
              border-color: #93c5fd !important;
              background: #f8fbff !important;
              color: #1d4ed8 !important;
              box-shadow: 0 14px 28px rgba(37, 99, 235, 0.10) !important;
            }}
            </style>
            """,
            unsafe_allow_html=True,
        )
        with column:
            if st.button(
                f"{label}  {icon}\n{value}\n{copy}\n諛붾줈媛湲?>",
                key=f"dashboard_kpi_{card_key}",
                use_container_width=True,
                type="secondary",
            ):
                _navigate_from_dashboard_kpi(card_key)


def _render_public_dashboard_workspace_impl(
    datasets: dict[str, pd.DataFrame],
    source_datasets: dict[str, object] | None,
) -> None:
    _inject_compact_public_dashboard_styles()
    opportunity_index = build_dashboard_opportunity_index(datasets, source_datasets)
    if not opportunity_index.empty:
        opportunity_index = opportunity_index.sort_values(
            by=["Score", "_sort_date", "Project"],
            ascending=[False, False, True],
            na_position="last",
        )
    recommended_rows = (
        opportunity_index[build_positive_recommendation_mask(opportunity_index)].copy()
        if not opportunity_index.empty
        else pd.DataFrame()
    )
    notice_rows = _build_dashboard_notice_inbox_rows(datasets, source_datasets)
    updated_at = pd.Timestamp.now(tz="Asia/Seoul").strftime("%Y-%m-%d %H:%M")

    search_cols = st.columns([8.3, 1.35, 2.2], gap="small")
    with search_cols[0]:
        dashboard_search = clean(
            st.text_input(
                "Dashboard Search",
                key="public_dashboard_compact_search_text",
                placeholder="怨듦퀬紐? 怨쇱젣紐? ?ㅼ썙?? 湲곌? 寃??,
                label_visibility="collapsed",
            )
        )
    with search_cols[1]:
        if st.button("珥덇린??, key="public_dashboard_compact_search_reset", use_container_width=True):
            st.session_state["public_dashboard_compact_search_text"] = ""
            st.rerun()
    with search_cols[2]:
        st.markdown(
            f'<div class="dashboard-toolbar-meta">?낅뜲?댄듃 {escape(updated_at)}</div>',
            unsafe_allow_html=True,
        )

    filtered_opportunity_rows, filtered_notice_rows = _filter_public_dashboard_frames(
        opportunity_index,
        notice_rows,
        dashboard_search,
    )
    recommended_filtered = (
        filtered_opportunity_rows[build_positive_recommendation_mask(filtered_opportunity_rows)].copy()
        if not filtered_opportunity_rows.empty
        else pd.DataFrame()
    )
    preview_rows = filtered_notice_rows.head(10).copy() if not filtered_notice_rows.empty else pd.DataFrame()

    if dashboard_search:
        st.caption(f"寃??寃곌낵: 異붿쿇 RFP {len(recommended_filtered.head(5))}嫄? 理쒓렐 怨듦퀬 {len(preview_rows)}嫄?)

    _render_public_dashboard_kpi_cards_impl(
        recommended_rows.head(len(recommended_rows)),
        build_dashboard_notice_index(datasets, source_datasets, archived=False),
    )

    top_left, top_right = st.columns([6, 1.8], gap="medium")
    with top_left:
        st.markdown(
            '<div class="oppty-section-header"><div><div class="oppty-section-title">異붿쿇 RFP Top 5</div><div class="oppty-section-subtitle">?듭떖 ?뺣낫留?鍮좊Ⅴ寃??묎퀬 ?곸꽭 寃?좉? ?꾩슂??怨듦퀬留??좊퀎?⑸땲??</div></div></div>',
            unsafe_allow_html=True,
        )
    with top_right:
        st.markdown('<div style="height:1rem"></div>', unsafe_allow_html=True)
        if st.button("?꾩껜 RFP Queue 蹂닿린", key="public_dashboard_to_rfp_queue", use_container_width=True):
            navigate_to_route_state(route_core.build_rfp_queue_route(), push=True)
    _render_dashboard_top_rfp_cards(recommended_filtered, selected_item_id="", on_select=None, visible_count=5)

    notice_left, notice_right = st.columns([6, 2.0], gap="medium")
    with notice_left:
        st.markdown(
            '<div class="oppty-section-header"><div><div class="oppty-section-title">理쒓렐 怨듦퀬 (Notice Inbox)</div><div class="oppty-section-subtitle">理쒖떊 怨듦퀬 10嫄대쭔 compact inbox濡?蹂댁뿬以띾땲??</div></div></div>',
            unsafe_allow_html=True,
        )
    with notice_right:
        st.markdown('<div style="height:1rem"></div>', unsafe_allow_html=True)
        if st.button("?꾩껜 Notice Queue 蹂닿린", key="public_dashboard_to_notice_queue", use_container_width=True):
            navigate_to_route_state(route_core.build_notice_queue_route(), push=True)
    _render_dashboard_recent_notice_inbox(preview_rows, limit=10)


def _run_public_workspace_main(app_mode: str = "viewer"):
    load_dotenv()

    mode_config = build_app_mode_config(
        app_mode,
        nipa_view_columns=tuple(NIPA_VIEW_COLUMNS),
    )

    st.set_page_config(
        page_title=mode_config.page_title,
        layout="wide",
    )
    inject_page_styles()
    inject_opportunity_detail_alignment_styles()
    inject_viewer_layout_styles()
    require_login(mode_config)
    consume_favorite_toggle_query_action()

    sheet_names = {
        "notice_master": resolve_canonical_notice_master_sheet(get_env),
        "notice_current": resolve_notice_current_view_sheet(get_env),
        "pending": resolve_notice_pending_view_sheet(get_env),
        "notice_archive": resolve_notice_archive_view_sheet(get_env),
        "opportunity": resolve_iris_opportunity_current_sheet(get_env),
        "opportunity_archive": resolve_iris_opportunity_archive_sheet(get_env),
        "summary": get_env("SUMMARY_SHEET", "SUMMARY"),
        "errors": get_env("ERROR_SHEET", "OPPORTUNITY_ERRORS"),
    }

    try:
        datasets = load_app_datasets(
            sheet_names["notice_master"],
            sheet_names["notice_current"],
            sheet_names["pending"],
            sheet_names["notice_archive"],
            sheet_names["opportunity"],
            sheet_names["opportunity_archive"],
            sheet_names["summary"],
            sheet_names["errors"],
        )
    except Exception as exc:
        st.error(f"?쒗듃 濡쒕뵫 ?ㅽ뙣: {exc}")
        st.stop()

    default_route = route_core.normalize_route(
        {
            "source": mode_config.default_source,
            "page": get_default_page_for_source(mode_config, mode_config.default_source),
            "view": "list",
            "source_key": mode_config.default_source,
        }
    )
    current_route = initialize_route_state(default_route)
    normalized_route = _normalize_workspace_shell_route(current_route)
    if not route_core.route_equals(current_route, normalized_route):
        route_core.set_current_route(normalized_route)
        replace_query_params(with_auth_params(route_core.serialize_route(normalized_route)))
        current_route = normalized_route

    current_source = clean(current_route.get("source")) or mode_config.default_source
    current_page = normalize_route_page_key(current_route.get("page")) or get_default_page_for_source(mode_config, mode_config.default_source)
    if current_page == "notice_queue":
        current_source = "notices"
    elif current_page == "favorites":
        current_source = "favorites"
    elif current_page == "dashboard":
        current_source = "dashboard"
    else:
        current_source = "iris"

    render_public_workspace_navigation(mode_config, current_source, current_page)

    source_config_map = get_source_config_map(mode_config)
    selected_source_config = source_config_map.get(current_source)
    source_datasets = None
    if current_source in {"dashboard", "notices", "favorites"}:
        source_datasets = build_source_datasets()
    elif selected_source_config and selected_source_config.requires_source_datasets:
        source_datasets = build_source_datasets()

    if is_user_scoped_operations_enabled():
        datasets, source_datasets = apply_user_review_statuses(
            datasets,
            source_datasets,
            get_current_operation_scope_key(),
        )

    render_selected_source(
        current_source,
        source_config=selected_source_config,
        mode_config=mode_config,
        datasets=datasets,
        source_datasets=source_datasets,
        show_internal_tabs=False,
    )
