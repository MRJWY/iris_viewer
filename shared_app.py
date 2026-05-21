import json
import hashlib
# Legacy local shared app copy for the public viewer bundle.
# Active runtime entrypoints in `iris_viewer_repo/` now delegate to the
# repository-root `app.py`, which is the source of truth for Dashboard,
# queue, and detail behavior. Keep this file only for compatibility/fallback
# while the remaining local duplicates are phased out.
import hmac
import math
import os
import re
import time
import base64
import uuid
from html import escape
from pathlib import Path
from urllib.parse import urlencode

import gspread
import pandas as pd
import streamlit as st
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
PROJECT_ROOT = BASE_DIR
SCOPE = ["https://www.googleapis.com/auth/spreadsheets"]
DEFAULT_PAGE_SIZE = 300
REVIEW_OPTIONS = ["", "검토전", "관심공고", "보류", "완료", "검토완료"]
FAVORITE_REVIEW_STATUS = "관심공고"
ARCHIVE_REVIEW_STATUS_VALUES = {"완료", "검토완료"}


class _PageApiProxy:
    def __getattr__(self, name: str):
        try:
            return globals()[name]
        except KeyError as exc:
            raise AttributeError(name) from exc


PAGE_API = _PageApiProxy()


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
    "source",
    "notice_id",
    "notice_title",
    "user_id",
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

NOTICE_ID_CANDIDATES = ["공고ID", "notice_id"]
NOTICE_TITLE_CANDIDATES = ["공고명", "notice_title", "title"]
DETAIL_LINK_CANDIDATES = ["상세링크", "detail_link"]
REVIEW_STATUS_CANDIDATES = ["검토 여부", "검토여부", "review_status"]
STATUS_KEY_CANDIDATES = ["상태키", "status_key"]

SOURCE_KEY_ALIAS_MAP = {
    "iris": "iris",
    "notices": "iris",
    "tipa": "tipa",
    "mss": "tipa",
    "nipa": "nipa",
    "favorites": "favorites",
}

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


def safe_int(value: object, default: int = 0) -> int:
    try:
        return int(clean(value))
    except (TypeError, ValueError):
        return default


def normalize_row_dict(row: dict[str, object] | pd.Series | None) -> dict[str, object]:
    if row is None:
        return {}
    if isinstance(row, pd.Series):
        return row.to_dict()
    return dict(row)


def row_has_data(row: dict[str, object] | pd.Series | None) -> bool:
    return bool(normalize_row_dict(row))

def normalize_opportunity_source_key(source_key: object) -> str:
    return SOURCE_KEY_ALIAS_MAP.get(clean(source_key).lower(), clean(source_key).lower())


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


def _source_tagged_opportunity_frame(df: pd.DataFrame, *, source_key: str) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame()
    tagged = df.copy()
    if "source_key" not in tagged.columns or tagged["source_key"].fillna("").astype(str).str.strip().eq("").all():
        tagged["source_key"] = source_key
    return tagged


def build_rfp_queue_frames(
    datasets: dict[str, pd.DataFrame],
    source_datasets: dict[str, object] | None,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    current_frames = [_source_tagged_opportunity_frame(datasets["opportunity"], source_key="iris")]
    all_frames = [
        _source_tagged_opportunity_frame(datasets["opportunity"], source_key="iris"),
        _source_tagged_opportunity_frame(datasets["opportunity_archive"], source_key="iris"),
    ]
    if source_datasets:
        current_frames.extend(
            [
                _source_tagged_opportunity_frame(source_datasets["mss_opportunity"], source_key="tipa"),
                _source_tagged_opportunity_frame(source_datasets["nipa_opportunity"], source_key="nipa"),
            ]
        )
        all_frames.extend(
            [
                _source_tagged_opportunity_frame(source_datasets["mss_opportunity"], source_key="tipa"),
                _source_tagged_opportunity_frame(source_datasets["mss_opportunity_archive"], source_key="tipa"),
                _source_tagged_opportunity_frame(source_datasets["nipa_opportunity"], source_key="nipa"),
                _source_tagged_opportunity_frame(source_datasets["nipa_opportunity_archive"], source_key="nipa"),
            ]
        )
    current = pd.concat([frame for frame in current_frames if not frame.empty], ignore_index=True) if any(not frame.empty for frame in current_frames) else pd.DataFrame()
    all_rows = pd.concat([frame for frame in all_frames if not frame.empty], ignore_index=True) if any(not frame.empty for frame in all_frames) else current
    return current, all_rows


def render_iris_page(
    page_key: str,
    datasets: dict[str, pd.DataFrame],
    source_datasets: dict[str, object] | None = None,
) -> None:
    if page_key in {"opportunity", "rfp_queue"}:
        if source_datasets is None:
            source_datasets = build_source_datasets()
        rfp_current_df, rfp_all_df = build_rfp_queue_frames(datasets, source_datasets)
        render_opportunity_page(
            rfp_current_df,
            page_key="rfp_queue",
            all_df=rfp_all_df,
        )
    elif page_key == "summary":
        render_summary_page(datasets["summary"], datasets["opportunity_all"])
    elif page_key in {"notice", "notice_queue"}:
        render_notice_page_with_scope(
            datasets["notice_current"],
            datasets["opportunity_all"],
            page_key="notice_queue",
            title="진행 공고",
            default_status_scope="접수중",
            current_only_default=True,
            already_scoped=True,
        )
    elif page_key == "notice_scheduled":
        render_notice_page_with_scope(
            datasets["pending"],
            datasets["opportunity_all"],
            page_key="notice_scheduled",
            title="예정 공고",
            default_status_scope="예정",
            current_only_default=True,
            already_scoped=True,
        )
    elif page_key == "notice_archive":
        render_notice_page_with_scope(
            datasets["notice_archive"],
            datasets["opportunity_all"],
            page_key="notice_archive",
            title="Archive",
            default_status_scope="전체",
            current_only_default=False,
            archive=True,
            already_scoped=True,
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
        append_source(tipa_df, source_key="tipa", source_label="중소기업벤처부")

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
    source_labels = {"iris": "IRIS", "tipa": "중소기업벤처부", "nipa": "NIPA"}

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
        index=["검토 완료", "미검토"],
    )


def build_dashboard_opportunity_index(
    datasets: dict[str, pd.DataFrame],
    source_datasets: dict[str, object] | None,
) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []

    def append_source(df: pd.DataFrame, *, source_key: str, source_label: str) -> None:
        if df is None or df.empty:
            return
        working = ensure_opportunity_row_ids(filter_current_opportunity_rows(df))
        if working.empty:
            return

        normalized = working.copy()
        normalized["Row ID"] = series_from_candidates(working, ["_row_id"])
        normalized["source_key"] = (
            series_from_candidates(working, ["source_key", "_source_key"])
            .replace("", pd.NA)
            .fillna(source_key)
        )
        normalized["Source"] = (
            series_from_candidates(working, ["Source", "source_label", "source_site"])
            .replace("", pd.NA)
            .fillna(source_label)
        )
        normalized["Notice ID"] = series_from_candidates(working, ["notice_id", "공고ID"])
        normalized["Notice Title"] = series_from_candidates(working, ["notice_title", "공고명"])
        normalized["Project"] = series_from_candidates(working, ["llm_project_name", "project_name", "해당 과제명", "llm_rfp_title", "rfp_title"])
        normalized["Recommendation"] = series_from_candidates(working, ["recommendation", "추천여부", "llm_recommendation"])
        normalized["Score"] = to_numeric_column(series_from_candidates(working, ["rfp_score", "점수", "llm_fit_score"]))
        normalized["Candidate"] = series_from_candidates(working, ["candidate_flag", "확장후보여부", "llm_score_candidate_flag"])
        normalized["Candidate Reason"] = series_from_candidates(working, ["candidate_reason", "확장후보사유", "llm_score_candidate_reason"])
        normalized["Budget"] = series_from_candidates(working, ["budget", "예산", "llm_total_budget_text", "total_budget_text"])
        normalized["Reason"] = series_from_candidates(working, ["llm_reason", "reason", "관심사유"])
        normalized["Keywords"] = series_from_candidates(working, ["llm_keywords", "keywords", "키워드"])
        normalized["LLM Enriched"] = series_from_candidates(working, ["llm_enriched"])
        normalized["LLM Mode"] = series_from_candidates(working, ["llm_mode"])
        normalized["LLM Error"] = series_from_candidates(working, ["llm_error"])
        normalized["Document Role"] = series_from_candidates(working, ["llm_document_role", "document_role", "document_type"])
        normalized["Validation Errors"] = series_from_candidates(working, ["validation_errors", "검증오류"])
        normalized["Date"] = series_from_candidates(working, ["ancm_de", "공고일자", "registered_at"])
        normalized["Period"] = series_from_candidates(working, ["notice_period", "period", "접수기간", "요청기간"])
        normalized["RFP Period"] = series_from_candidates(working, ["rfp_period", "RFP 기간"])
        normalized["Agency"] = series_from_candidates(working, ["agency", "전문기관", "전문기관명"])
        normalized["Ministry"] = series_from_candidates(working, ["ministry", "소관부처"])
        normalized["Application Field"] = series_from_candidates(working, ["llm_application_field", "application_field"])
        normalized["Eligibility"] = series_from_candidates(working, ["llm_eligibility", "eligibility"])
        normalized["Support Need"] = series_from_candidates(working, ["llm_support_need", "support_need"])
        normalized["Status"] = series_from_candidates(working, ["rcve_status", "status", "notice_status"])
        normalized["Review"] = series_from_candidates(working, ["review_status", "검토여부"])
        normalized["_sort_date"] = parse_date_column(normalized["Date"])
        frames.append(normalized)

    append_source(datasets["opportunity"], source_key="iris", source_label="IRIS")
    if source_datasets:
        append_source(source_datasets["mss_opportunity"], source_key="tipa", source_label="중소기업벤처부")
        append_source(source_datasets["nipa_opportunity"], source_key="nipa", source_label="NIPA")

    if not frames:
        return pd.DataFrame(columns=["Row ID", "source_key", "Source", "Notice ID", "Notice Title", "Project", "Recommendation", "Score", "Budget", "Reason", "Keywords", "Date", "Period", "Agency", "Ministry", "Status", "Review", "_sort_date"])

    combined = pd.concat(frames, ignore_index=True)
    combined = _build_queue_filter_frame(combined)
    return _sort_opportunity_rows_for_display(combined)


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

    recent = filter_comments_by_scope(comments_df)
    recent = recent.head(limit).copy()
    if recent.empty:
        return pd.DataFrame(columns=["작성시각", "작성자", "댓글"])
    recent["작성자"] = series_from_candidates(recent, ["nickname", "user_id", "author"])
    recent["댓글"] = series_from_candidates(recent, ["content", "comment"]).apply(
        lambda value: compact_table_value(value, max_chars=42)
    )
    return recent.rename(columns={"created_at": "작성시각"})[["작성시각", "작성자", "댓글"]]


def render_dashboard_chart_block(title: str, chart_df: pd.DataFrame, *, chart_type: str = "bar") -> None:
    st.markdown(f"### {title}")
    if chart_df.empty:
        st.info("표시할 데이터가 없습니다.")
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
        st.info("표시할 데이터가 없습니다.")
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
    empty_message: str = "표시할 데이터가 없습니다.",
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
        ("IRIS 진행", "iris", "notice"),
        ("IRIS Opportunity", "iris", "opportunity"),
        ("중소기업벤처부 진행", "tipa", "tipa_current"),
        ("NIPA 진행", "nipa", "nipa_current"),
    ]
    secondary_links = [("관심 공고", "favorites", "favorites")]
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
            <div class="grant-search-brand">정부 과제 추천</div>
            <div class="grant-search-divider"></div>
            <div class="grant-search-nav">
              <span class="active">과제 검색</span>
              <span>맞춤 추천</span>
            </div>
          </div>
          <div class="grant-search-auth">
            <span>로그인</span>
            <span>회원가입</span>
          </div>
        </div>
        <div class="grant-search-hero">
          <div class="grant-search-title">원하는 정부 과제를 검색하고 필터를 적용해보세요</div>
          <div class="grant-search-subtitle">{source_count}개 부처 · {agency_count:,}개 수행기관 · {notice_count:,}개 공고 사이트 기반 실시간 업데이트</div>
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
                placeholder="예) 과제명, 사업 분야 키워드, 기술/연구 세부 키워드 입력을 통해 필요한 과제를 찾아보세요.",
                label_visibility="collapsed",
                height=210,
            )
        with button_col:
            st.markdown('<div class="grant-search-button-wrap">', unsafe_allow_html=True)
            submitted = st.form_submit_button("⌕\n검색하기", use_container_width=True)
            st.markdown("</div>", unsafe_allow_html=True)
    st.markdown("</div>", unsafe_allow_html=True)

    if submitted:
        st.session_state["sidebar_search"] = clean(search_text)
        navigate_to_route("iris", "notice_queue")

    st.markdown(
        """
        <div class="grant-filter-head">
          <div class="grant-filter-title">요건 / 필터</div>
          <div class="grant-chip">↻ 초기화</div>
        </div>
        <div class="grant-filter-grid">
          <div class="grant-filter-cell">
            <div class="grant-filter-label">기관 유형</div>
            <div class="grant-chip-row">
              <span class="grant-chip">대기업</span>
              <span class="grant-chip">중견기업</span>
              <span class="grant-chip">중소기업/스타트업</span>
              <span class="grant-chip">대학 연구실</span>
              <span class="grant-chip">공공/민간 연구기관</span>
              <span class="grant-chip">의료기관</span>
            </div>
          </div>
          <div class="grant-filter-cell">
            <div class="grant-filter-label">내 매출액</div>
            <div class="grant-filter-input"><div class="grant-filter-placeholder">매출액 입력</div><div class="grant-filter-unit">억원</div></div>
          </div>
          <div class="grant-filter-cell">
            <div class="grant-filter-label">내 사업연수</div>
            <div class="grant-filter-input"><div class="grant-filter-placeholder">사업 연수 입력</div><div class="grant-filter-unit">년</div></div>
          </div>
          <div class="grant-filter-cell">
            <div class="grant-filter-label">기관 소재지</div>
            <div class="grant-chip-row">
              <span class="grant-chip active">전국</span>
              <span class="grant-chip">서울</span>
              <span class="grant-chip">경기</span>
              <span class="grant-chip">인천</span>
              <span class="grant-chip">부산</span>
              <span class="grant-chip">대구</span>
            </div>
          </div>
          <div class="grant-filter-cell">
            <div class="grant-filter-label">부설연구소/연구전담부서 유무</div>
            <div class="grant-chip-row">
              <span class="grant-chip">예</span>
              <span class="grant-chip">아니오</span>
            </div>
          </div>
          <div class="grant-filter-cell">
            <div class="grant-filter-label">과제 유형</div>
            <div class="grant-chip-row">
              <span class="grant-chip active">전체</span>
              <span class="grant-chip">연구개발</span>
              <span class="grant-chip">사업화</span>
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
        st.error(f"{source_config.label} 데이터를 불러오지 못했습니다.")
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
        st.error(f"{source_config.label} 페이지 구성을 찾지 못했습니다: {current_page_key}")
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
    recommended_count = int(build_positive_recommendation_mask(opportunity_index).sum()) if not opportunity_index.empty else 0
    high_score_count = int(opportunity_index["Score"].fillna(0).ge(80).sum()) if not opportunity_index.empty else 0

    render_page_header(
        "제안관리",
        "제안 단계 데이터는 아직 분리 연동 전입니다. 현재는 추천기회와 관심 공고 중심으로 후보를 관리합니다.",
        eyebrow="Proposal",
    )
    render_metrics(
        [
            ("추천 후보", str(recommended_count)),
            ("고득점 후보", str(high_score_count)),
            ("작성중", "-"),
            ("제출 완료", "-"),
        ]
    )
    st.info("제안 단계별 상태, 제출 이력, 결과 대기 현황은 후속 연동이 필요합니다.")
    action_cols = st.columns(3)
    with action_cols[0]:
        if st.button("추천기회로 이동", key="proposal_go_opportunity", use_container_width=True):
            navigate_to_route("iris", "rfp_queue")
    with action_cols[1]:
        if st.button("관심 공고 보기", key="proposal_go_favorites", use_container_width=True):
            navigate_to_route("favorites", "favorites")
    with action_cols[2]:
        if st.button("대시보드로 이동", key="proposal_go_dashboard", use_container_width=True):
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
        "운영관리",
        (
            f"{get_current_operation_scope_label()} 기준의 관심 공고, 댓글, 오류, 검토 커버리지를 확인합니다."
            if is_user_scoped_operations_enabled()
            else "관심 공고, 댓글, 오류, 검토 커버리지를 기준으로 운영 상태를 확인합니다."
        ),
        eyebrow="Operations",
    )
    render_metrics(
        [
            ("현재 공고", str(total_current_notices)),
            ("미검토", str(total_review_needed)),
            ("관심 공고", str(len(favorites_df))),
            ("커버리지", review_coverage),
        ]
    )

    left_col, right_col = st.columns([1.2, 1.0])
    with left_col:
        render_dashboard_table_block(
            "관심 공고",
            favorites_df[["매체", "공고명", "공고일자"]].head(10)
            if not favorites_df.empty and {"매체", "공고명", "공고일자"}.issubset(favorites_df.columns)
            else pd.DataFrame(),
        )
    with right_col:
        render_dashboard_table_block("최근 댓글", recent_comments_df)


def render_signup_request_public_page() -> None:
    render_page_header(
        "가입 요청",
        "Viewer 사용 요청을 남기면 private admin app에서 바로 검토할 수 있도록 접수됩니다.",
        eyebrow="Support",
    )
    st.caption("접수된 요청 검토와 승인/반려 처리는 별도 private admin app에서 진행됩니다.")

    default_email = clean(get_env("APP_USER_EMAIL"))
    default_name = clean(get_env("APP_USER_NAME") or get_env("DEFAULT_COMMENT_AUTHOR"))
    default_org = clean(get_env("APP_USER_ORGANIZATION"))

    with st.form("signup_request_public_form"):
        name = st.text_input("이름", value=default_name)
        email = st.text_input("이메일", value=default_email)
        organization = st.text_input("소속 / 회사", value=default_org)
        account_type = st.selectbox("계정 유형", ["company", "lab", "institution", "student", "team"], index=0)
        request_note = st.text_area("요청 메모", height=140, placeholder="사용 목적이나 필요한 데이터 범위를 적어주세요.")
        submitted = st.form_submit_button("가입 요청 보내기", type="primary", use_container_width=True)

    normalized_email = clean(email).lower()
    existing_requests = get_signup_requests_for_email(normalized_email) if normalized_email else pd.DataFrame()
    latest_request = existing_requests.iloc[0].to_dict() if not existing_requests.empty else {}
    latest_status = clean(latest_request.get("status")).upper()

    if submitted:
        if not normalized_email:
            st.error("이메일은 비워둘 수 없습니다.")
            return
        if latest_status in {"PENDING", "HOLD"}:
            st.warning("같은 이메일로 진행 중인 가입 요청이 이미 있습니다. private admin app 검토 후 다시 확인해주세요.")
            return
        if latest_status == "APPROVED":
            st.success("이미 승인된 요청이 있습니다. 운영팀 안내 메일을 먼저 확인해주세요.")
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
        st.success("가입 요청을 접수했습니다. private admin app에서 바로 검토할 수 있습니다.")
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
    return clean(first_non_empty(normalize_row_dict(row), "Review", "review_status", "검토 여부", "검토여부"))


def _count_dashboard_urgent_notices(rows: pd.DataFrame, *, max_days: int = 30) -> int:
    if rows is None or rows.empty:
        return 0
    count = 0
    period_values = series_from_candidates(rows, ["notice_period", "접수기간", "period"]).fillna("").astype(str)
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
                "recommendation": ["추천"],
                "status": [],
                "deadline": [],
                "field": [],
                "review": [],
                "sort": "추천순",
                "archive_reason": [],
            },
            page_no=1,
            page_size=20,
        )
    elif card_key == "review_needed":
        route = route_core.build_rfp_queue_route(
            filters={
                "recommendation": ["추천"],
                "status": [],
                "deadline": [],
                "field": [],
                "review": ["", "검토전", "미검토"],
                "sort": "추천순",
                "archive_reason": [],
            },
            page_no=1,
            page_size=20,
        )
    elif card_key == "urgent_notice":
        route = route_core.build_notice_queue_route(
            filters={
                "status": ["진행중", "예정"],
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


def _legacy_render_dashboard_kpi_cards(recommended_rows: pd.DataFrame, notice_rows: pd.DataFrame) -> None:
    recommended_only_rows = pd.DataFrame()
    if recommended_rows is not None and not recommended_rows.empty:
        recommendation_series = series_from_candidates(
            recommended_rows,
            ["_queue_recommendation", "Recommendation", "recommendation", "llm_recommendation"],
        ).fillna("").astype(str).apply(_normalize_recommendation_value)
        recommended_only_rows = recommended_rows[recommendation_series.eq("추천")].copy()
    recommended_count = len(recommended_only_rows) if not recommended_only_rows.empty else 0
    review_needed = 0
    favorite_count = 0
    if not recommended_only_rows.empty:
        review_series = recommended_only_rows.apply(_dashboard_review_value, axis=1)
        review_needed = int(review_series.isin(["", "검토전", "미검토"]).sum())
    if notice_rows is not None and not notice_rows.empty:
        favorite_series = notice_rows.apply(_dashboard_review_value, axis=1)
        favorite_count = int(favorite_series.eq(FAVORITE_REVIEW_STATUS).sum())
    urgent_count = _count_dashboard_urgent_notices(notice_rows)

    cards = [
        ("recommended_rfp", "추천 RFP", str(recommended_count), "추천 RFP Queue로 바로 이동", "↗"),
        ("review_needed", "검토 필요", str(review_needed), "검토전 추천 과제만 모아서 보기", "•"),
        ("urgent_notice", "마감 임박", str(urgent_count), "30일 이내 진행중/예정 공고 보기", "!"),
        ("favorite_notice", "관심공고", str(favorite_count), "Favorites 목록으로 바로 이동", "★"),
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
                f"{label}  {icon}\n{value}\n{copy}\n바로가기 ->",
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
    notice_id = clean(first_non_empty(row, "공고ID", "notice_id"))
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
        clean(selection.get("notice_id")) == clean(first_non_empty(row, "공고ID", "notice_id"))
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


def _workspace_row_id(row: dict[str, object] | pd.Series) -> str:
    return clean(first_non_empty(row, "Row ID", "_row_id"))


def _workspace_notice_id(row: dict[str, object] | pd.Series) -> str:
    return clean(first_non_empty(row, "Notice ID", "공고ID", "notice_id"))


def _workspace_source_key(row: dict[str, object] | pd.Series) -> str:
    return resolve_route_source_key_for_row(row, source_key=first_non_empty(row, "source_key", "_source_key")) or "iris"


def _build_notice_summary_lookup(opportunity_rows: pd.DataFrame) -> pd.DataFrame:
    if opportunity_rows.empty:
        return pd.DataFrame(
            columns=[
                "source_key",
                "Notice ID",
                "Recommendation",
                "Score",
                "Budget",
                "Summary",
                "Keywords",
                "Project",
            ]
        )

    working = opportunity_rows.copy()
    working["Notice ID"] = series_from_candidates(working, ["Notice ID", "notice_id"])
    working["source_key"] = series_from_candidates(working, ["source_key", "_source_key"])
    working["Recommendation"] = series_from_candidates(working, ["Recommendation", "recommendation", "추천여부"])
    working["Score"] = to_numeric_column(series_from_candidates(working, ["Score", "rfp_score", "점수"]))
    working["Budget"] = series_from_candidates(working, ["Budget", "budget", "예산"])
    working["Summary"] = series_from_candidates(working, ["Reason", "reason", "llm_reason"])
    working["Keywords"] = series_from_candidates(working, ["Keywords", "keywords", "llm_keywords"])
    working["Project"] = series_from_candidates(working, ["Project", "project_name", "llm_project_name", "해당 과제명"])
    working = working.sort_values(
        by=["Score", "_sort_date", "Project"],
        ascending=[False, False, True],
        na_position="last",
    )
    best = working.drop_duplicates(subset=["source_key", "Notice ID"], keep="first").copy()
    return best[
        ["source_key", "Notice ID", "Recommendation", "Score", "Budget", "Summary", "Keywords", "Project"]
    ]


def _normalize_workspace_notice_rows(
    source_df: pd.DataFrame,
    opportunity_rows: pd.DataFrame,
) -> pd.DataFrame:
    if source_df is None or source_df.empty:
        return pd.DataFrame(
            columns=[
                "Row ID",
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
                "Scope",
                "Recommendation",
                "Score",
                "Budget",
                "Summary",
                "Keywords",
                "Project",
                "D-Day",
                "_sort_date",
            ]
        )

    working = source_df.copy()
    normalized = pd.DataFrame(index=working.index.copy())
    normalized["Row ID"] = series_from_candidates(working, ["_collection_id", "_favorite_id", "Row ID"])
    normalized["source_key"] = series_from_candidates(working, ["source_key", "_source_key"])
    normalized["Source"] = series_from_candidates(working, ["매체", "source_label", "Source"])
    normalized["Notice ID"] = series_from_candidates(working, ["공고ID", "notice_id", "Notice ID"])
    normalized["Title"] = series_from_candidates(working, ["공고명", "notice_title", "Title"])
    normalized["Notice No"] = series_from_candidates(working, ["공고번호", "notice_no", "ancm_no", "Notice No"])
    normalized["Status"] = series_from_candidates(working, ["공고상태", "status", "rcve_status", "Status"])
    normalized["Period"] = series_from_candidates(working, ["접수기간", "신청기간", "period", "Period"])
    normalized["Review"] = series_from_candidates(working, ["검토 여부", "검토여부", "review_status", "Review"])
    normalized["Agency"] = series_from_candidates(working, ["전문기관", "담당부서", "agency", "Agency"])
    normalized["Ministry"] = series_from_candidates(working, ["소관부처", "주관부처", "ministry", "Ministry"])
    normalized["Date"] = series_from_candidates(working, ["공고일자", "registered_at", "ancm_de", "등록일", "Date"])
    normalized["Scope"] = series_from_candidates(working, ["_notice_scope", "Scope"])
    normalized["Detail Link"] = working.apply(
        lambda row: resolve_external_detail_link(row, source_key=clean(first_non_empty(row, "source_key", "_source_key"))),
        axis=1,
    )
    normalized["Recommendation"] = series_from_candidates(working, ["대표추천도", "_queue_recommendation", "Recommendation"])
    normalized["Score"] = to_numeric_column(series_from_candidates(working, ["대표점수", "_queue_score", "Score"]))
    normalized["Budget"] = series_from_candidates(working, ["대표예산", "예산", "_queue_budget", "Budget"])
    normalized["Summary"] = series_from_candidates(working, ["대표추천이유", "_queue_analysis", "_queue_reason", "Summary"])
    normalized["Keywords"] = series_from_candidates(working, ["대표키워드", "_queue_keywords", "Keywords"])
    normalized["Project"] = series_from_candidates(working, ["대표과제명", "_queue_project_name", "Project"])
    normalized["_sort_date"] = parse_date_column(normalized["Date"])

    summary_lookup = _build_notice_summary_lookup(opportunity_rows)
    if not summary_lookup.empty:
        merged = normalized.merge(summary_lookup, on=["source_key", "Notice ID"], how="left", suffixes=("", "_joined"))
        for column in ["Recommendation", "Budget", "Summary", "Keywords", "Project"]:
            merged[column] = series_from_candidates(merged, [column, f"{column}_joined"])
        merged["Score"] = to_numeric_column(series_from_candidates(merged, ["Score", "Score_joined"]))
        normalized = merged

    source_defaults = {"iris": "IRIS", "tipa": "MSS", "nipa": "NIPA"}
    normalized["Source"] = normalized.apply(
        lambda row: clean(row.get("Source")) or source_defaults.get(clean(row.get("source_key")), "-"),
        axis=1,
    )
    normalized["D-Day"] = normalized.apply(
        lambda row: format_dashboard_deadline_badge(row.get("Period"), row.get("Status")),
        axis=1,
    )
    return normalized.sort_values(
        by=["_sort_date", "Source", "Title"],
        ascending=[False, True, True],
        na_position="last",
    )


def _build_workspace_notice_rows(
    datasets: dict[str, pd.DataFrame],
    source_datasets: dict[str, object] | None,
    opportunity_rows: pd.DataFrame,
) -> pd.DataFrame:
    return _normalize_workspace_notice_rows(
        build_crawled_notice_collection(datasets, source_datasets),
        opportunity_rows,
    )


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
        if st.button("✕", key=f"{panel_key}_close", use_container_width=True, disabled=selected_row is None):
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
            f'<div><div class="summary-panel-meta-label">기간 / D-day</div><div class="summary-panel-meta-value">{escape(ctx["period"])} · {escape(ctx["deadline"])}</div></div>'
            f'<div><div class="summary-panel-meta-label">예산</div><div class="summary-panel-meta-value">{escape(ctx["budget"])}</div></div>'
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
        if st.button("RFP 상세 보기", key=f"{panel_key}_detail", type="primary", use_container_width=True):
            navigate_to_opportunity_detail(source_key, detail_target_id)

    st.markdown('<div style="height:0.45rem"></div>', unsafe_allow_html=True)
    _render_same_tab_link_button(
        "원문공고 열기",
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
        if st.button("✕", key=f"{panel_key}_close", use_container_width=True, disabled=selected_row is None):
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
    title_text = clean(first_non_empty(selected_row, "공고명", "notice_title")) or "-"
    period_text = clean(first_non_empty(selected_row, "notice_period", "접수기간", "period")) or "-"
    summary_text = clean(first_non_empty(selected_row, "_queue_analysis", "summary", "_queue_project_name")) or "연결된 RFP 분석이 아직 없습니다."
    keywords = _extract_dashboard_keywords(selected_row)
    status_text = normalize_notice_status_label(first_non_empty(selected_row, "status", "rcve_status", "공고상태")) or "-"
    recommendation_text = clean(first_non_empty(selected_row, "_queue_recommendation", "recommendation")) or "보통"
    top_badges = "".join(
        [
            '<span class="summary-panel-type">Notice</span>',
            _pill_html(recommendation_text),
            _pill_html(status_text, kind="deadline"),
        ]
    )
    detail_target_id = clean(first_non_empty(selected_row, "공고ID", "notice_id"))
    current_value = clean(first_non_empty(selected_row, "review_status", "검토여부", "검토 여부"))
    source_line = " / ".join(
        part
        for part in [
            clean(first_non_empty(selected_row, "매체", "source_label", "source_site")) or (source_key or "IRIS").upper(),
            clean(first_non_empty(selected_row, "전문기관", "agency", "담당부서")),
        ]
        if clean(part) and part != "-"
    )
    budget_text = clean(first_non_empty(selected_row, "_queue_budget", "budget", "예산")) or "-"
    deadline_text = format_dashboard_deadline_badge(period_text, status_text) or "-"
    detail_link = resolve_external_detail_link(selected_row, source_key=source_key)
    keyword_html = "".join(f'<span class="summary-panel-keyword">{escape(keyword)}</span>' for keyword in keywords[:6])

    st.markdown(
        (
            f'<div class="summary-panel-badges">{top_badges}</div>'
            f'<div class="summary-panel-title">{escape(title_text)}</div>'
            f'<div class="summary-panel-source">{escape(source_line or "-")}</div>'
            '<div class="summary-panel-meta-grid">'
            f'<div><div class="summary-panel-meta-label">기간 / D-day</div><div class="summary-panel-meta-value">{escape(period_text)} · {escape(deadline_text)}</div></div>'
            f'<div><div class="summary-panel-meta-label">예산</div><div class="summary-panel-meta-value">{escape(budget_text)}</div></div>'
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
        if st.button("Notice 상세 보기", key=f"{panel_key}_detail", type="primary", use_container_width=True):
            navigate_to_notice_detail(source_key, detail_target_id)

    st.markdown('<div style="height:0.45rem"></div>', unsafe_allow_html=True)
    _render_same_tab_link_button(
        "원문공고 열기",
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
            empty_title="카드나 공고를 선택하면 요약이 열립니다.",
            empty_copy="Dashboard 본문은 그대로 유지한 채 우측 패널에서 핵심 정보만 먼저 보고, 필요한 경우에만 상세 페이지로 이동할 수 있습니다.",
            close_callback=lambda: (_clear_dashboard_selection(), st.rerun()),
        )
        return
    _render_notice_preview_panel(
        selected_row,
        panel_key="dashboard_notice_preview",
        empty_title="카드나 공고를 선택하면 요약이 열립니다.",
        empty_copy="Dashboard 본문은 그대로 유지한 채 우측 패널에서 핵심 정보만 먼저 보고, 필요한 경우에만 상세 페이지로 이동할 수 있습니다.",
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
    working["_dashboard_notice_id"] = series_from_candidates(working, ["공고ID", "notice_id"]).fillna("").astype(str).str.strip()
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
    return working.sort_values(by=["_sort_date", "매체", "공고명"], ascending=[False, True, True], na_position="last")


def _render_dashboard_top_rfp_cards(
    rows: pd.DataFrame,
    *,
    selected_item_id: str = "",
    on_select=None,
    visible_count: int = 5,
) -> None:
    if rows.empty:
        st.info("표시할 추천 Opportunity가 없습니다.")
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
                    f'<div><div class="rfp-card-meta-label">기관</div><div class="rfp-card-meta-value">{escape(ctx["agency"])}</div></div>'
                    f'<div><div class="rfp-card-meta-label">기간 / D-day</div><div class="rfp-card-meta-value">{escape(ctx["period"])} / {escape(ctx["deadline"])}</div></div>'
                    f'<div><div class="rfp-card-meta-label">예산</div><div class="rfp-card-meta-value">{escape(ctx["budget"])}</div></div>'
                    f'<div><div class="rfp-card-meta-label">출처</div><div class="rfp-card-meta-value">{escape(ctx["source"])}</div></div>'
                    '</div>'
                    '</div>'
                ),
                unsafe_allow_html=True,
            )
            st.markdown('<div class="rfp-card-action-slot">', unsafe_allow_html=True)
            if st.button(
                "선택됨" if is_active else "요약 보기",
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
        st.info("최근 표시할 공고가 없습니다.")
        return

    st.markdown(
        '<div class="notice-row-shell"><div class="notice-row-head"><div>상태</div><div>추천여부</div><div>공고명</div><div>기관</div><div>등록일</div><div>D-day</div><div>예산</div><div>요약</div><div>관심공고</div></div>',
        unsafe_allow_html=True,
    )
    for idx, (_, row) in enumerate(rows.head(limit).iterrows(), start=1):
        source_key = resolve_route_source_key_for_row(row, source_key=row.get("source_key"))
        is_active = _is_selected_dashboard_notice(row)
        status_text = normalize_notice_status_label(first_non_empty(row, "status", "rcve_status", "공고상태")) or "-"
        recommendation_text = clean(row.get("_queue_recommendation")) or "-"
        title_text = clean(first_non_empty(row, "공고명", "notice_title")) or "-"
        agency_text = clean(first_non_empty(row, "전문기관", "agency", "담당부서")) or "-"
        notice_date = clean(first_non_empty(row, "registered_at", "공고일자", "ancm_de")) or "-"
        period_text = clean(first_non_empty(row, "notice_period", "접수기간", "period")) or ""
        budget_text = clean(first_non_empty(row, "_queue_budget", "budget", "예산")) or "-"
        summary_text = clean(first_non_empty(row, "_queue_analysis", "_queue_project_name")) or "연결된 RFP 분석이 아직 없습니다."
        dday_text = format_dashboard_deadline_badge(period_text, status_text) or "-"
        notice_id = clean(first_non_empty(row, "공고ID", "notice_id"))
        st.markdown(f'<div class="notice-row-body{" is-active" if is_active else ""}">', unsafe_allow_html=True)
        row_cols = st.columns([1.0, 1.1, 3.0, 1.6, 1.0, 0.9, 1.2, 2.3, 0.9], gap="small")
        with row_cols[0]:
            st.markdown(_pill_html(status_text, kind="deadline"), unsafe_allow_html=True)
        with row_cols[1]:
            st.markdown(_pill_html(recommendation_text), unsafe_allow_html=True)
        with row_cols[2]:
            st.markdown(f'<div class="notice-row-title">{escape(truncate_text(title_text, max_chars=78))}</div>', unsafe_allow_html=True)
            if notice_id and st.button(
                "선택됨" if is_active else "요약 보기",
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
                current_value=clean(row.get("review_status") or row.get("검토여부")),
                source_key=source_key or "iris",
                notice_title=title_text,
                button_key=f"dashboard_notice_favorite_{idx}",
                compact=True,
                icon_only=True,
                use_container_width=False,
            )
        st.markdown("</div>", unsafe_allow_html=True)
    st.markdown("</div>", unsafe_allow_html=True)


def _legacy_render_dashboard_workspace(
    datasets: dict[str, pd.DataFrame],
    source_datasets: dict[str, object] | None,
) -> None:
    opportunity_index = build_dashboard_opportunity_index(datasets, source_datasets)
    if not opportunity_index.empty:
        opportunity_index = _sort_opportunity_rows_for_display(opportunity_index)
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
            f'<div class="dashboard-greeting-title">{escape(current_user_label)}님, 오늘도 좋은 기회를 찾아보세요!</div>'
            '<div class="dashboard-greeting-copy">AI 분석 기반으로 선별한 R&amp;D Opportunity를 추천드립니다.</div>'
            '</div>'
            '<div class="dashboard-greeting-meta">'
            f'<span class="dashboard-greeting-pill">추천 RFP {len(recommended_rows.head(5))}건</span>'
            f'<span class="dashboard-greeting-pill">최근 공고 {len(preview_rows)}건</span>'
            f'<span class="dashboard-greeting-pill">분석 완료 {len(opportunity_index)}건</span>'
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
                '<div class="oppty-section-header"><div><div class="oppty-section-title">🔥 추천 RFP Top 5</div><div class="oppty-section-subtitle">추천 카드에서 핵심 정보만 먼저 보고, 필요한 경우에만 우측 Summary Panel에서 상세 검토로 이어집니다.</div></div></div>',
                unsafe_allow_html=True,
            )
        with top_right:
            st.markdown('<div style="height:1rem"></div>', unsafe_allow_html=True)
            if st.button("전체 RFP Queue 보기 >", key="dashboard_to_rfp_queue", use_container_width=True):
                navigate_to_route_state(route_core.build_rfp_queue_route(), push=True)
        _render_dashboard_top_rfp_cards(recommended_rows, selected_item_id="", on_select=None, visible_count=5)

        notice_left, notice_right = st.columns([6, 2.1], gap="medium")
        with notice_left:
            st.markdown(
                '<div class="oppty-section-header"><div><div class="oppty-section-title">Recent Notice Inbox</div><div class="oppty-section-subtitle">최근 공고를 compact table로 빠르게 훑고, 필요한 공고만 Summary Panel로 확인합니다.</div></div></div>',
                unsafe_allow_html=True,
            )
        with notice_right:
            notice_action_col, notice_size_col = st.columns([2.3, 1.2], gap="small")
            with notice_action_col:
                st.markdown('<div style="height:1rem"></div>', unsafe_allow_html=True)
                if st.button("전체 Notice Queue 보기 >", key="dashboard_to_notice_queue", use_container_width=True):
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
        st.info("표시할 추천 Opportunity가 없습니다.")
        return
    max_start = max(len(rows) - visible_count, 0)
    start = int(st.session_state.get(carousel_key, 0))
    start = max(0, min(start, max_start))
    nav_left, nav_mid, nav_right = st.columns([1, 4, 1])
    with nav_left:
        if st.button("◀", key=f"{carousel_key}_prev", use_container_width=True, disabled=start <= 0):
            start = max(0, start - visible_count)
    with nav_mid:
        st.markdown(f'<div class="oppty-carousel-summary">{start + 1}-{min(start + visible_count, len(rows))} / {len(rows)}</div>', unsafe_allow_html=True)
    with nav_right:
        if st.button("▶", key=f"{carousel_key}_next", use_container_width=True, disabled=start >= max_start):
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
                    f'<div><div class="rfp-card-meta-label">기관</div><div class="rfp-card-meta-value">{escape(ctx["agency"])}</div></div>'
                    f'<div><div class="rfp-card-meta-label">기간</div><div class="rfp-card-meta-value">{escape(ctx["period"])}</div></div>'
                    f'<div><div class="rfp-card-meta-label">예산</div><div class="rfp-card-meta-value">{escape(ctx["budget"])}</div></div>'
                    f'<div><div class="rfp-card-meta-label">소스</div><div class="rfp-card-meta-value">{escape(ctx["source"])}</div></div>'
                    '</div>'
                    '</div>'
                ),
                unsafe_allow_html=True,
            )
            action_cols = st.columns(3, gap="small")
            detail_link = resolve_external_detail_link(row, source_key=source_key)
            with action_cols[0]:
                _render_same_tab_link_button(
                    "원문공고",
                    detail_link,
                    kind="secondary",
                    key=f"{carousel_key}_origin_disabled_{rank}",
                )
            with action_cols[1]:
                if st.button(
                    "요약 보기" if not is_active else "선택됨",
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
        st.info("최근 표시할 공고가 없습니다.")
        return
    st.markdown(
        '<div class="notice-row-shell"><div class="notice-row-head"><div>상태</div><div>추천</div><div>출처</div><div>공고명</div><div>등록일</div><div>D-day</div><div>예산</div><div>요약</div><div>관심</div></div>',
        unsafe_allow_html=True,
    )
    for idx, (_, row) in enumerate(rows.head(limit).iterrows(), start=1):
        source_key = resolve_route_source_key_for_row(row, source_key=row.get("source_key"))
        is_active = _is_selected_dashboard_notice(row)
        status_text = normalize_notice_status_label(first_non_empty(row, "status", "rcve_status", "공고상태")) or "-"
        recommendation_text = clean(row.get("_queue_recommendation")) or "-"
        title_text = clean(first_non_empty(row, "공고명", "notice_title")) or "-"
        source_text = clean(first_non_empty(row, "매체", "source_label", "source_site")) or (source_key or "IRIS").upper()
        agency_text = clean(first_non_empty(row, "전문기관", "agency", "담당부서")) or "-"
        notice_date = clean(first_non_empty(row, "registered_at", "공고일자", "ancm_de")) or "-"
        period_text = clean(first_non_empty(row, "notice_period", "접수기간", "period")) or ""
        budget_text = clean(first_non_empty(row, "_queue_budget", "budget", "예산")) or "-"
        summary_text = clean(first_non_empty(row, "_queue_analysis", "_queue_project_name")) or "연결된 RFP 분석이 아직 없습니다."
        dday_text = format_dashboard_deadline_badge(period_text, status_text) or "-"
        notice_id = clean(first_non_empty(row, "공고ID", "notice_id"))
        row_cols = st.columns([1.1, 1.1, 1.2, 3.4, 1.2, 1.0, 1.1, 2.7, 0.9], gap="small")
        with row_cols[0]:
            st.markdown(_pill_html(status_text, kind="deadline"), unsafe_allow_html=True)
        with row_cols[1]:
            st.markdown(_pill_html(recommendation_text), unsafe_allow_html=True)
        with row_cols[2]:
            st.markdown(f'<div class="notice-row-meta">{escape(source_text)} · {escape(agency_text)}</div>', unsafe_allow_html=True)
        with row_cols[3]:
            st.markdown(f'<div class="notice-row-title">{escape(truncate_text(title_text, max_chars=84))}</div>', unsafe_allow_html=True)
            if notice_id and st.button(
                "요약 보기" if not is_active else "선택됨",
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
                current_value=clean(row.get("review_status") or row.get("검토여부")),
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
    del source_config, mode_config, show_internal_tabs
    _inject_opportunity_workspace_styles()
    _render_dashboard_workspace(datasets, source_datasets)
    return

    opportunity_index = build_dashboard_opportunity_index(datasets, source_datasets)
    if not opportunity_index.empty:
        opportunity_index = _sort_opportunity_rows_for_display(opportunity_index)
    recommended_rows = opportunity_index[build_positive_recommendation_mask(opportunity_index)].copy() if not opportunity_index.empty else pd.DataFrame()
    notice_rows = _build_dashboard_notice_inbox_rows(datasets, source_datasets)
    preview_rows = notice_rows.head(12).copy() if not notice_rows.empty else pd.DataFrame()
    search_key = "dashboard_global_notice_search"

    render_page_header(
        "R&D Opportunity Dashboard",
        "추천된 Opportunity를 먼저 검토하고, 필요할 때만 Notice Inbox로 내려가는 intelligence workspace입니다.",
        eyebrow="Intelligence Workspace",
    )
    st.markdown(f"### {escape(get_current_user_label() or '사용자')}님, 오늘도 좋은 기회를 찾아보세요!")
    st.caption(
        f"AI 분석 기반으로 선별한 Opportunity {len(recommended_rows.head(10))}건과 최근 Notice {len(preview_rows)}건을 바로 검토할 수 있습니다."
    )

    search_col, action_col = st.columns([4.6, 1], gap="medium")
    with search_col:
        st.text_input(
            "dashboard-notice-search",
            key=search_key,
            placeholder="공고명 / 과제명 / 기관명 검색",
            label_visibility="collapsed",
        )
    with action_col:
        st.markdown('<div style="height:0.1rem"></div>', unsafe_allow_html=True)
        if st.button("Notice 검색", key="dashboard_notice_search_submit", use_container_width=True):
            navigate_to_route_state(
                route_core.build_notice_queue_route(
                    filters={
                        "status": [],
                        "recommendation": [],
                        "search": clean(st.session_state.get(search_key, "")),
                        "source": [],
                        "page_size": 20,
                    },
                    page_no=1,
                    page_size=20,
                ),
                push=True,
            )
    workspace_col, summary_col = st.columns([5.4, 2.15], gap="large")
    with workspace_col:
        section_left, section_right = st.columns([6, 1.8], gap="medium")
        with section_left:
            st.markdown(
                '<div class="oppty-section-header"><div><div class="oppty-section-title">Recommended RFP Queue</div><div class="oppty-section-subtitle">분석 완료된 Opportunity를 추천순 Top 10 기준으로 먼저 보고, 실제 지원 검토 대상으로 이어집니다.</div></div></div>',
                unsafe_allow_html=True,
            )
        with section_right:
            st.markdown('<div style="height:1.3rem"></div>', unsafe_allow_html=True)
            if st.button("RFP Queue 전체보기", key="dashboard_to_rfp_queue", use_container_width=True):
                navigate_to_route_state(route_core.build_rfp_queue_route(), push=True)
        _render_recommended_opportunity_cards(
            recommended_rows.head(10),
            page_key="dashboard",
            carousel_key="dashboard_recommended_rfp",
            visible_count=4,
            show_rank=True,
        )

        inbox_left, inbox_right = st.columns([6, 1.8], gap="medium")
        with inbox_left:
            st.markdown(
                '<div class="oppty-section-header"><div><div class="oppty-section-title">Recent Notice Inbox</div><div class="oppty-section-subtitle">최근 공고는 compact inbox로 빠르게 훑고, 필요한 공고만 Notice 상세에서 검토합니다.</div></div></div>',
                unsafe_allow_html=True,
            )
        with inbox_right:
            st.markdown('<div style="height:1.3rem"></div>', unsafe_allow_html=True)
            if st.button("전체 공고 보기", key="dashboard_to_notice_browser", use_container_width=True):
                navigate_to_route_state(route_core.build_notice_queue_route(), push=True)
        _render_recent_notice_inbox(preview_rows, limit=12)
    with summary_col:
        _render_dashboard_summary_panel(opportunity_index, notice_rows)


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

    if current_page_key == "notice_queue":
        render_notice_queue_page(datasets, source_datasets)
        return

    render_iris_page(current_page_key, datasets, source_datasets)


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


def get_source_renderers() -> dict[str, object]:
    return {
        "dashboard": render_dashboard_source,
        "notices": render_notices_source,
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
    normalized_source_key = normalize_opportunity_source_key(source_key) or "iris"
    renderer_lookup_key = normalize_opportunity_source_key(source_config.renderer_key if source_config else normalized_source_key)
    source_renderers = get_source_renderers()
    renderer = source_renderers.get(renderer_lookup_key) or source_renderers.get(normalized_source_key)
    if renderer is None:
        fallback_config = source_config or SourceRouteConfig("iris", "IRIS", mode_config.default_iris_page, False, "iris")
        render_iris_source(fallback_config, mode_config, datasets, source_datasets, show_internal_tabs=show_internal_tabs)
        return
    active_config = source_config or SourceRouteConfig(normalized_source_key, normalized_source_key, mode_config.default_iris_page, False, normalized_source_key)
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
    link = row_first_non_empty(row, *DETAIL_LINK_CANDIDATES)
    normalized_source = resolve_route_source_key_for_row(row, source_key=source_key)
    if normalized_source in {"tipa", "nipa"}:
        return link

    notice_id = row_first_non_empty(row, *NOTICE_ID_CANDIDATES)
    if not notice_id:
        return link

    status_key = row_first_non_empty(row, *STATUS_KEY_CANDIDATES)
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


MISSING_DISPLAY_TEXT = "없음"


def detail_display_text(label: str, value: object, *, missing_text: str = MISSING_DISPLAY_TEXT) -> str:
    text = sanitize_display_text(label, value)
    if text in {"", "-", "미정", "정보 없음"}:
        return missing_text
    return text


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
    "개념 및 개발 내용",
    "지원필요성(과제 배경)",
    "활용분야",
    "지원기간 및 예산·추진체계",
    "추천 이유",
}


def preview_max_chars_for_label(label: str) -> int:
    normalized = clean(label)
    if normalized in LONG_ANALYSIS_LABELS:
        return 900
    if "예산" in normalized or normalized.lower() == "budget":
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

    matches = re.findall(r"\d[\d,]*(?:\.\d+)?\s*(?:조원|억원|천만원|백만원|만원|원)", text)
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

    if "예산" in normalized_label or normalized_label.lower() == "budget":
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
    if "예산" in normalized_label or normalized_label.lower() == "budget":
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
    status_key = clean(row.get("status_key") or row.get("상태키"))
    period_text = clean(row.get("period") or row.get("접수기간"))
    period_end = extract_period_end(period_text)
    period_start = pd.to_datetime(
        clean(period_text).split("~", 1)[0].strip().replace(".", "-") if "~" in clean(period_text) else "",
        errors="coerce",
    )
    today = pd.Timestamp.now().normalize()

    if status_key == "ancmIng":
        if pd.notna(period_start) and period_start.normalize() > today:
            return "예정"
        if pd.notna(period_end) and period_end.normalize() < today:
            return "마감"
        return "접수중"
    if status_key == "ancmPre":
        return "예정"
    if status_key in {"ancmCls", "ancmEnd"}:
        return "마감"

    status_text = clean(row.get("rcve_status") or row.get("공고상태"))
    if "접수중" in status_text or "공고중" in status_text:
        if pd.notna(period_start) and period_start.normalize() > today:
            return "예정"
        if pd.notna(period_end) and period_end.normalize() < today:
            return "마감"
        return "접수중"
    if "예정" in status_text:
        return "예정"
    if "마감" in status_text:
        return "마감"

    if pd.notna(period_start) and period_start.normalize() > today:
        return "예정"
    if pd.notna(period_end) and period_end.normalize() < today:
        return "마감"

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
    return clean(row.get("notice_title")) or clean(row.get("공고명"))


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
    query_route = route_core.deserialize_route(
        query_params,
        default_route=default_route,
    )
    if not route_core.route_equals(route, query_route):
        route = route_core.set_current_route(query_route)
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


def get_current_comment_scope_key() -> str:
    if not is_user_scoped_operations_enabled():
        return ""
    return clean(get_current_operation_scope_key())


def _comment_scope_series(df: pd.DataFrame) -> pd.Series:
    primary_scope = _comment_text_series(df, "ip_based_uid")
    legacy_scope = _comment_text_series(df, "user_id")
    legacy_scope_mask = primary_scope.eq("") & legacy_scope.str.match(r"^(domain:|user:)", na=False)
    return primary_scope.where(primary_scope.ne(""), legacy_scope.where(legacy_scope_mask, ""))


def filter_comments_by_scope(comments_df: pd.DataFrame, *, scope_key: str = "") -> pd.DataFrame:
    if comments_df.empty:
        return _empty_comment_dataframe()

    working = _normalize_comment_dataframe(comments_df)
    normalized_scope = clean(scope_key) or get_current_comment_scope_key()
    if not normalized_scope:
        return working

    filtered = working[_comment_scope_series(working).eq(normalized_scope)].copy()
    if filtered.empty:
        return _empty_comment_dataframe()
    return filtered


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


def _legacy_find_auth_account_row(*, user_id: str = "", email: str = "") -> tuple[int, list[str], dict[str, str]] | tuple[int, None, None]:
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


def _legacy_sync_auth_account_status(
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
    if isinstance(user, dict):
        user_id = clean(user.get("user_id"))
        if user_id:
            return user_id
    return get_env("APP_USER_EMAIL") or get_env("DEFAULT_COMMENT_AUTHOR") or ""


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


def get_current_user_affiliation() -> str:
    user = st.session_state.get("auth_user") or {}
    if isinstance(user, dict):
        organization = clean(user.get("organization"))
        if organization:
            return organization
        latest_request = get_latest_signup_request_for_account(
            {
                "user_id": clean(user.get("user_id")),
                "email": clean(user.get("email")),
            }
        )
        organization = clean(latest_request.get("organization"))
        if organization:
            return organization
    return ""


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
    params.pop("logout", None)
    replace_query_params(params)
    st.rerun()


def consume_workspace_logout_query_action() -> None:
    if clean(get_query_param("logout")).lower() not in {"1", "true", "y", "yes"}:
        return
    logout_current_user()


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

    notice_ids = series_from_candidates(working, ["공고ID", "notice_id"])
    override_values = [
        lookup.get((source_key, normalize_notice_id_for_match(notice_id)), None)
        for notice_id in notice_ids
    ]
    override_series = pd.Series(override_values, index=working.index, dtype=object)
    override_mask = override_series.notna()
    if not override_mask.any():
        return working

    for column in ["검토 여부", "검토여부", "review_status"]:
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

    working = filter_comments_by_scope(comments_df)
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
    scope_key = get_current_comment_scope_key()
    timestamp = pd.Timestamp.now(tz="Asia/Seoul").strftime("%Y-%m-%d %H:%M:%S")
    row = {
        "comment_id": str(uuid.uuid4()),
        "post_id": build_comment_post_id(source_key, notice_id),
        "source": normalize_opportunity_source_key(source_key) or clean(source_key).lower() or "iris",
        "notice_id": notice_id,
        "notice_title": clean(notice_title),
        "user_id": current_user_id or nickname,
        "parent_id": "",
        "nickname": nickname,
        "content": content[:5000],
        "mention": "",
        "ip_address": "",
        "ip_based_uid": scope_key,
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
        raise RuntimeError("로그인 사용자 정보가 없어 검토 여부를 저장할 수 없습니다.")
    if not notice_id:
        raise RuntimeError("공고ID가 없어 검토 여부를 저장할 수 없습니다.")

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
        raise RuntimeError("사용자 검토 시트의 필수 컬럼이 없습니다.")

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


def _legacy_submit_signup_request(*, user_id: str, password: str, display_name: str, email: str) -> None:
    user_id = clean(user_id)
    password = clean(password)
    email = clean(email).lower()
    if not user_id:
        raise RuntimeError("아이디를 입력해 주세요.")
    if len(user_id) < 3:
        raise RuntimeError("아이디는 3자 이상이어야 합니다.")
    if not re.match(r"^[A-Za-z0-9_.-]+$", user_id):
        raise RuntimeError("아이디는 영문, 숫자, 점, 밑줄, 하이픈만 사용할 수 있습니다.")
    if len(password) < 6:
        raise RuntimeError("비밀번호는 6자 이상이어야 합니다.")
    allowed_domains = load_allowed_email_domains()
    email_domain = normalize_email_domain(email)
    if allowed_domains and email_domain not in allowed_domains:
        raise RuntimeError("허용된 회사 이메일 도메인만 가입 요청할 수 있습니다.")
    if get_auth_account(user_id):
        raise RuntimeError("이미 등록되었거나 승인 대기 중인 아이디입니다.")

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
    st.markdown("#### 가입 요청")
    allowed_domains = sorted(load_allowed_email_domains())
    if allowed_domains:
        st.caption("가입 가능 도메인: " + ", ".join(allowed_domains))
    with st.form("signup_form"):
        user_id = st.text_input("아이디", key="signup_user_id")
        display_name = st.text_input("이름", key="signup_display_name")
        organization = st.text_input("소속", key="signup_organization")
        email = st.text_input("이메일", key="signup_email")
        password = st.text_input("비밀번호", type="password", key="signup_password")
        password_confirm = st.text_input("비밀번호 확인", type="password", key="signup_password_confirm")
        submitted = st.form_submit_button("가입 요청", use_container_width=True)
    if submitted:
        if clean(password) != clean(password_confirm):
            st.error("비밀번호 확인이 일치하지 않습니다.")
            return
        try:
            submit_signup_request(
                user_id=user_id,
                password=password,
                display_name=display_name,
                organization=organization,
                email=email,
            )
            st.success("가입 요청을 보냈습니다. 관리자가 승인하면 로그인할 수 있습니다.")
        except Exception as exc:
            st.error(f"가입 요청 실패: {exc}")


def render_login_page(mode_config: AppModeConfig, accounts: dict[str, dict[str, str]]) -> None:
    st.markdown("<div style='height: 12vh;'></div>", unsafe_allow_html=True)
    _, center_col, _ = st.columns([1.2, 1, 1.2])
    with center_col:
        st.title(mode_config.header_title)
        st.caption("같은 이메일 도메인을 가진 사용자끼리는 댓글, 관심공고, 검토 상태를 함께 공유합니다.")
        if mode_config.mode == "viewer":
            login_tab, signup_tab = st.tabs(["로그인", "가입 요청"])
            with login_tab:
                with st.form("login_form"):
                    user_id = st.text_input("아이디")
                    password = st.text_input("비밀번호", type="password")
                    submitted = st.form_submit_button("로그인", use_container_width=True)
                if submitted:
                    account = accounts.get(clean(user_id))
                    if account and clean(account.get("status")).lower() == "approved" and verify_password(password, account.get("password_hash", "")):
                        latest_request = get_latest_signup_request_for_account(account)
                        st.session_state["auth_user"] = {
                            "user_id": clean(account.get("user_id")),
                            "display_name": clean(account.get("display_name")),
                            "email": clean(account.get("email")),
                            "organization": clean(latest_request.get("organization")),
                            "role": clean(account.get("role")) or "viewer",
                        }
                        token = encode_auth_token(account.get("user_id", ""))
                        st.session_state["auth_token"] = token
                        replace_query_params(with_auth_params(get_query_params_dict()))
                        st.rerun()
                    elif account and clean(account.get("status")).lower() == "pending":
                        st.warning("아직 활성화되지 않은 계정입니다. 관리자에게 활성화 상태를 확인해 주세요.")
                    elif account and clean(account.get("status")).lower() == "rejected":
                        st.error("사용이 중지된 계정입니다. 관리자에게 문의해 주세요.")
                    else:
                        st.error("아이디 또는 비밀번호를 확인해 주세요.")
            with signup_tab:
                render_signup_form()
        else:
            with st.form("login_form"):
                user_id = st.text_input("아이디")
                password = st.text_input("비밀번호", type="password")
                submitted = st.form_submit_button("로그인", use_container_width=True)
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
                    st.warning("아직 활성화되지 않은 계정입니다. 관리자에게 활성화 상태를 확인해 주세요.")
                elif account and clean(account.get("status")).lower() == "rejected":
                    st.error("사용이 중지된 계정입니다. 관리자에게 문의해 주세요.")
                else:
                    st.error("아이디 또는 비밀번호를 확인해 주세요.")


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


def submit_signup_request(*, user_id: str, password: str, display_name: str, organization: str, email: str) -> None:
    user_id = clean(user_id)
    password = clean(password)
    email = clean(email).lower()
    display_name = clean(display_name) or user_id
    organization = clean(organization)
    if not user_id:
        raise RuntimeError("아이디를 입력해 주세요.")
    if len(user_id) < 3:
        raise RuntimeError("아이디는 3자 이상이어야 합니다.")
    if not re.match(r"^[A-Za-z0-9_.-]+$", user_id):
        raise RuntimeError("아이디는 영문, 숫자, 밑줄, 점, 하이픈만 사용할 수 있습니다.")
    if not organization:
        raise RuntimeError("소속을 입력해 주세요.")
    if len(password) < 6:
        raise RuntimeError("비밀번호는 6자 이상이어야 합니다.")

    allowed_domains = load_allowed_email_domains()
    email_domain = normalize_email_domain(email)
    if allowed_domains and email_domain not in allowed_domains:
        raise RuntimeError("허용된 회사 이메일 도메인만 가입 요청할 수 있습니다.")
    if get_auth_account(user_id):
        raise RuntimeError("이미 등록됐거나 승인 대기 중인 아이디입니다.")

    existing_requests = get_signup_requests_for_email(email)
    if not existing_requests.empty:
        latest_status = clean(existing_requests.iloc[0].get("status")).upper()
        if latest_status in {"PENDING", "HOLD"}:
            raise RuntimeError("같은 이메일로 진행 중인 가입 요청이 이미 있습니다.")
        if latest_status == "APPROVED":
            raise RuntimeError("이미 승인된 가입 요청이 있습니다.")

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
            "organization": organization,
            "account_type": "viewer",
            "request_note": f"requested_user_id={user_id}",
            "status": "PENDING",
        }
    )
    load_sheet_as_dataframe.clear()
    load_auth_user_accounts.clear()


def resolve_notice_source_key(row: dict | pd.Series | None) -> str:
    candidate_values: list[str] = []
    if row is not None:
        row_dict = normalize_row_dict(row)
        candidate_values.extend(
            [
                clean(row_dict.get("source_key")),
                clean(row_dict.get("_source_key")),
                clean(row_dict.get("source")),
                clean(row_dict.get("Source")),
                clean(row_dict.get("source_site")),
                clean(row_dict.get("매체")),
            ]
        )
    candidate_values.append(clean(get_query_param("source")))

    for candidate in candidate_values:
        normalized = normalize_opportunity_source_key(candidate)
        if normalized and normalized != "favorites":
            return normalized
    return "iris"


def normalize_mss_notice_df(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df

    working = df.copy()
    working["registered_at"] = series_from_candidates(working, ["registered_at", "ancm_de", "등록일"])
    working["period"] = series_from_candidates(working, ["period", "신청기간"])
    working["agency"] = series_from_candidates(working, ["agency", "department", "담당부서"])
    working["notice_title"] = series_from_candidates(working, ["notice_title", "title", "공고명"])
    working["notice_no"] = series_from_candidates(working, ["notice_no", "ancm_no", "공고번호"])
    working["status"] = series_from_candidates(working, ["status", "공고상태"])
    working["views"] = series_from_candidates(working, ["views", "조회"])
    working["detail_link"] = series_from_candidates(working, ["detail_link", "상세링크"])
    working["review_status"] = series_from_candidates(working, ["review_status", "검토 여부", "검토여부"])
    working["notice_id"] = series_from_candidates(working, ["notice_id", "공고ID"])
    working["_sort_date"] = parse_date_column(working["registered_at"])

    working["등록일"] = working["registered_at"]
    working["신청기간"] = working["period"]
    working["담당부서"] = working["agency"]
    working["전문기관"] = working["agency"]
    working["공고명"] = working["notice_title"]
    working["공고번호"] = working["notice_no"]
    working["상태"] = working["status"]
    working["공고상태"] = working["status"]
    working["조회"] = working["views"]
    working["상세링크"] = working["detail_link"]
    working["검토 여부"] = working["review_status"]
    working["공고ID"] = working["notice_id"]
    return working.sort_values(by=["_sort_date", "공고번호", "공고명"], ascending=[False, False, True], na_position="last")


def normalize_nipa_notice_df(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df

    working = df.copy()
    working["registered_at"] = series_from_candidates(working, ["registered_at", "ancm_de", "등록일"])
    working["period"] = series_from_candidates(working, ["period", "신청기간"])
    working["business_name"] = series_from_candidates(working, ["business_name", "project_name", "사업명"])
    working["agency"] = series_from_candidates(working, ["agency", "department", "담당부서", "전문기관"])
    working["notice_title"] = series_from_candidates(working, ["notice_title", "title", "공고명"])
    working["notice_no"] = series_from_candidates(working, ["notice_no", "ancm_no", "공고번호", "row_number"])
    working["status"] = series_from_candidates(working, ["status", "상태", "공고상태"])
    working["detail_link"] = series_from_candidates(working, ["detail_link", "상세링크"])
    working["review_status"] = series_from_candidates(working, ["review_status", "검토 여부", "검토여부"])
    working["notice_id"] = series_from_candidates(working, ["notice_id", "공고ID"])
    working["d_day"] = series_from_candidates(working, ["d_day", "남은신청기간"])
    working["author"] = series_from_candidates(working, ["author", "작성자"])
    working["_sort_date"] = parse_date_column(working["registered_at"])

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
    return working.sort_values(by=["_sort_date", "공고번호", "공고명"], ascending=[False, False, True], na_position="last")


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


def _update_review_status_in_sheets(
    *,
    notice_id: str,
    review_status: str,
    sheet_names: list[str],
    missing_message: str,
) -> None:
    notice_id = clean(notice_id)
    if not notice_id:
        raise RuntimeError("공고ID가 없어 검토 여부를 저장할 수 없습니다.")

    checked_sheets: list[str] = []
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
        notice_id_col = find_header_column(header, NOTICE_ID_CANDIDATES)
        if not notice_id_col:
            continue

        review_col = find_header_column(header, REVIEW_STATUS_CANDIDATES)
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

    joined_sheets = ", ".join(checked_sheets) or "unknown"
    raise RuntimeError(missing_message.format(sheet_names=joined_sheets, notice_id=notice_id))


def update_notice_review_status(notice_id: str, review_status: str, source_key: str = "iris") -> None:
    normalized_source = normalize_opportunity_source_key(source_key) or clean(source_key).lower() or "iris"
    if normalized_source == "tipa":
        update_mss_review_status(notice_id, review_status)
        return
    if normalized_source == "nipa":
        update_nipa_review_status(notice_id, review_status)
        return

    _update_review_status_in_sheets(
        notice_id=notice_id,
        review_status=review_status,
        sheet_names=[get_env("NOTICE_MASTER_SHEET", "IRIS_NOTICE_MASTER")],
        missing_message="IRIS 시트({sheet_names})에서 공고ID {notice_id}를 찾지 못했습니다.",
    )


def update_mss_review_status(notice_id: str, review_status: str) -> None:
    _update_review_status_in_sheets(
        notice_id=notice_id,
        review_status=review_status,
        sheet_names=[
            get_env("MSS_CURRENT_SHEET") or get_env("MSS_NOTICE_SHEET", "MSS_CURRENT"),
            get_env("MSS_PAST_SHEET", "MSS_PAST"),
        ],
        missing_message="MSS 시트({sheet_names})에서 공고ID {notice_id}를 찾지 못했습니다.",
    )
    build_source_datasets.clear()


def update_nipa_review_status(notice_id: str, review_status: str) -> None:
    _update_review_status_in_sheets(
        notice_id=notice_id,
        review_status=review_status,
        sheet_names=[
            get_env("NIPA_CURRENT_SHEET", "NIPA_CURRENT"),
            get_env("NIPA_PAST_SHEET", "NIPA_PAST"),
            get_env("NIPA_NOTICE_MASTER_SHEET", "NIPA_NOTICE_MASTER"),
        ],
        missing_message="NIPA 시트({sheet_names})에서 공고ID {notice_id}를 찾지 못했습니다.",
    )
    build_source_datasets.clear()


def enrich_notice_df(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    enriched = df.copy()
    enriched["공고ID"] = series_from_candidates(enriched, ["공고ID", "notice_id"])
    enriched["공고일자"] = series_from_candidates(enriched, ["공고일자", "registered_at", "ancm_de"])
    enriched["공고번호"] = series_from_candidates(enriched, ["공고번호", "notice_no", "ancm_no"])
    enriched["공고명"] = series_from_candidates(enriched, ["공고명", "notice_title", "title"])
    enriched["전문기관"] = series_from_candidates(enriched, ["전문기관", "agency"])
    enriched["소관부처"] = series_from_candidates(enriched, ["소관부처", "ministry"])
    enriched["검토 여부"] = series_from_candidates(enriched, ["검토 여부", "검토여부", "review_status"])
    enriched["상세링크"] = series_from_candidates(enriched, ["상세링크", "detail_link"])
    if "대표점수" in enriched.columns:
        enriched["대표점수"] = to_numeric_column(enriched["대표점수"])
    else:
        enriched["대표점수"] = 0
    enriched["공고상태"] = series_from_candidates(enriched, ["공고상태", "status", "rcve_status"])
    enriched["접수기간"] = series_from_candidates(enriched, ["접수기간", "period"])
    enriched["상태키"] = series_from_candidates(enriched, ["상태키", "status_key"])
    enriched["rcve_status"] = series_from_candidates(enriched, ["rcve_status", "공고상태"])
    enriched["period"] = series_from_candidates(enriched, ["period", "접수기간"])
    enriched["status_key"] = series_from_candidates(enriched, ["status_key", "상태키"])
    enriched["상세링크"] = enriched.apply(resolve_external_detail_link, axis=1)
    enriched["detail_link"] = enriched["상세링크"]
    enriched["_view_status"] = enriched.apply(classify_notice_status_for_view, axis=1)
    enriched["공고상태"] = series_from_candidates(enriched, ["_view_status", "공고상태", "rcve_status"])
    enriched["rcve_status"] = series_from_candidates(enriched, ["_view_status", "rcve_status", "공고상태"])
    if "공고일자" in enriched.columns:
        enriched["_sort_date"] = parse_date_column(enriched["공고일자"])
    else:
        enriched["_sort_date"] = pd.NaT
    return enriched.sort_values(
        by=["_sort_date", "대표점수", "공고명"],
        ascending=[False, False, True],
        na_position="last",
    )


def enrich_opportunity_df(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    enriched = df.copy()

    enriched["rfp_score"] = to_numeric_column(series_from_candidates(enriched, ["점수", "rfp_score"]))
    enriched["budget"] = series_from_candidates(enriched, ["예산", "budget"]).fillna("").astype(str).apply(extract_budget_summary)

    enriched["공고일자"] = series_from_candidates(enriched, ["공고일자", "registered_at", "ancm_de"])
    enriched["공고번호"] = series_from_candidates(enriched, ["공고번호", "notice_no", "ancm_no"])
    enriched["전문기관명"] = series_from_candidates(enriched, ["전문기관명", "전문기관", "agency"])
    enriched["공고명"] = series_from_candidates(enriched, ["공고명", "notice_title"])
    enriched["추천여부"] = series_from_candidates(enriched, ["추천여부", "recommendation"])
    enriched["공고상태"] = series_from_candidates(enriched, ["공고상태", "status", "rcve_status"])
    enriched["접수기간"] = series_from_candidates(enriched, ["접수기간", "period"])
    enriched["검토여부"] = series_from_candidates(enriched, ["검토여부", "검토 여부", "review_status"])
    enriched["상세링크"] = series_from_candidates(enriched, ["상세링크", "detail_link"])
    enriched["해당 과제명"] = series_from_candidates(enriched, ["해당 과제명", "과제명", "project_name", "llm_project_name"])
    enriched["점수"] = series_from_candidates(enriched, ["점수", "rfp_score", "llm_fit_score"])
    enriched["예산"] = series_from_candidates(
        enriched,
        ["예산", "budget", "llm_total_budget_text", "total_budget_text"],
    ).fillna("").astype(str).apply(extract_budget_summary)

    enriched["notice_title"] = series_from_candidates(enriched, ["공고명", "notice_title"])
    enriched["project_name"] = series_from_candidates(enriched, ["과제명", "project_name"])
    enriched["rfp_title"] = series_from_candidates(enriched, ["RFP 제목", "rfp_title"])
    enriched["recommendation"] = series_from_candidates(enriched, ["추천여부", "recommendation"])
    enriched["agency"] = series_from_candidates(enriched, ["전문기관명", "전문기관", "agency"])
    enriched["ministry"] = series_from_candidates(enriched, ["소관부처", "ministry"])
    enriched["ancm_de"] = series_from_candidates(enriched, ["공고일자", "registered_at", "ancm_de"])
    enriched["ancm_no"] = series_from_candidates(enriched, ["공고번호", "notice_no", "ancm_no"])
    enriched["rcve_status"] = series_from_candidates(enriched, ["공고상태", "status", "rcve_status"])
    enriched["period"] = series_from_candidates(enriched, ["접수기간", "period"])
    enriched["detail_link"] = series_from_candidates(enriched, ["상세링크", "detail_link"])
    enriched["review_status"] = series_from_candidates(enriched, ["검토여부", "검토 여부", "review_status"])
    enriched["notice_id"] = series_from_candidates(enriched, ["공고ID", "notice_id"])
    enriched["상세링크"] = enriched.apply(resolve_external_detail_link, axis=1)
    enriched["detail_link"] = enriched["상세링크"]
    enriched["document_id"] = series_from_candidates(enriched, ["문서ID", "document_id"])
    enriched["keywords"] = series_from_candidates(enriched, ["키워드", "keywords"])
    enriched["reason"] = series_from_candidates(enriched, ["추천이유", "reason"])
    enriched["concept_and_development"] = series_from_candidates(enriched, ["개념 및 개발 내용", "concept_and_development"])
    enriched["support_necessity"] = series_from_candidates(enriched, ["지원필요성(과제 배경)", "support_necessity"])
    enriched["application_field"] = series_from_candidates(enriched, ["활용분야", "application_field"])
    enriched["support_plan"] = series_from_candidates(enriched, ["지원기간 및 예산·추진체계", "support_plan"])
    enriched["technical_background"] = series_from_candidates(enriched, ["기술개발 배경 및 지원필요성", "technical_background"])
    enriched["development_content"] = series_from_candidates(enriched, ["기술개발 내용", "development_content"])
    enriched["support_need"] = series_from_candidates(enriched, ["지원필요성", "support_need"])
    enriched["document_type"] = series_from_candidates(enriched, ["문서유형", "document_type"])
    enriched["file_type"] = series_from_candidates(enriched, ["파일유형", "file_type"])
    enriched["source_site"] = series_from_candidates(enriched, ["출처사이트", "source_site"])
    enriched["notice_is_current"] = series_from_candidates(enriched, ["notice_is_current", "is_current"])
    enriched["notice_status"] = series_from_candidates(enriched, ["notice_status", "공고상태", "rcve_status"])
    enriched["notice_period"] = series_from_candidates(enriched, ["notice_period", "접수기간", "period"])
    enriched["file_name"] = series_from_candidates(enriched, ["파일명", "file_name"])
    enriched["file_path"] = series_from_candidates(enriched, ["파일경로", "file_path"])
    enriched["document_role"] = series_from_candidates(enriched, ["문서역할", "document_role"])
    enriched["project_name_source"] = series_from_candidates(enriched, ["과제명근거", "project_name_source"])
    enriched["project_name_confidence"] = series_from_candidates(enriched, ["과제명신뢰도", "project_name_confidence"])
    enriched["rfp_title_source"] = series_from_candidates(enriched, ["RFP제목근거", "rfp_title_source"])
    enriched["evidence"] = series_from_candidates(enriched, ["근거문장", "evidence"])
    enriched["conflict_flags"] = series_from_candidates(enriched, ["충돌플래그", "conflict_flags"])

    if "공고일자" in enriched.columns:
        enriched["_sort_date"] = parse_date_column(enriched["공고일자"])
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
    if notice_df.empty or "공고ID" not in notice_df.columns:
        return enriched

    notice_meta = notice_df.copy()
    notice_meta["공고ID"] = notice_meta["공고ID"].fillna("").astype(str).str.strip()
    keep_columns = [
        "공고ID",
        "공고일자",
        "공고번호",
        "전문기관",
        "공고명",
        "공고상태",
        "접수기간",
        "검토 여부",
        "상세링크",
        "소관부처",
        "상태키",
        "status_key",
        "is_current",
    ]
    available_columns = [column for column in keep_columns if column in notice_meta.columns]
    notice_meta = notice_meta[available_columns].drop_duplicates(subset=["공고ID"], keep="first")

    enriched["notice_id"] = series_from_candidates(enriched, ["notice_id", "공고ID"])
    merged = enriched.merge(notice_meta, left_on="notice_id", right_on="공고ID", how="left", suffixes=("", "_notice"))

    fallback_pairs = {
        "공고일자": ["공고일자", "ancm_de"],
        "공고번호": ["공고번호", "ancm_no"],
        "전문기관명": ["전문기관명", "agency", "전문기관"],
        "공고명": ["공고명", "notice_title"],
        "추천여부": ["추천여부", "recommendation"],
        "공고상태": ["공고상태", "rcve_status"],
        "접수기간": ["접수기간", "period"],
        "검토여부": ["검토여부", "review_status", "검토 여부"],
        "상세링크": ["상세링크", "detail_link"],
        "소관부처": ["소관부처", "ministry"],
        "상태키": ["상태키", "status_key"],
        "notice_is_current": ["notice_is_current", "is_current"],
        "notice_status": ["notice_status", "공고상태", "rcve_status"],
        "notice_period": ["notice_period", "접수기간", "period"],
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
    merged["notice_title"] = series_from_candidates(merged, ["notice_title", "공고명"])
    merged["agency"] = series_from_candidates(merged, ["agency", "전문기관", "전문기관명"])
    merged["ministry"] = series_from_candidates(merged, ["ministry", "주관부처"])
    merged["ancm_de"] = series_from_candidates(merged, ["ancm_de", "공고일자"])
    merged["ancm_no"] = series_from_candidates(merged, ["ancm_no", "공고번호"])
    merged["rcve_status"] = series_from_candidates(merged, ["rcve_status", "공고상태"])
    merged["period"] = series_from_candidates(merged, ["period", "접수기간"])
    merged["detail_link"] = series_from_candidates(merged, ["detail_link", "상세링크"])
    merged["상세링크"] = merged.apply(resolve_external_detail_link, axis=1)
    merged["detail_link"] = merged["상세링크"]
    merged["review_status"] = series_from_candidates(merged, ["review_status", "검토여부", "검토완료여부"])
    merged["notice_is_current"] = series_from_candidates(merged, ["notice_is_current", "is_current", "is_current_notice"])
    merged["notice_status"] = series_from_candidates(merged, ["notice_status", "공고상태", "rcve_status"])
    merged["notice_period"] = series_from_candidates(merged, ["notice_period", "접수기간", "period"])

    return merged


def enrich_summary_df(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    enriched = df.copy()
    if "대표점수" in enriched.columns:
        enriched["대표점수"] = to_numeric_column(enriched["대표점수"])
    if "예산" in enriched.columns:
        enriched["예산"] = enriched["예산"].fillna("").astype(str).apply(extract_budget_summary)
    if "공고일자" in enriched.columns:
        enriched["_sort_date"] = parse_date_column(enriched["공고일자"])
    else:
        enriched["_sort_date"] = pd.NaT
    return enriched.sort_values(
        by=["_sort_date", "대표점수", "공고명"],
        ascending=[False, False, True],
        na_position="last",
    )


def enrich_error_df(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    enriched = df.copy()
    enriched["source_site"] = series_from_candidates(enriched, ["source_site", "출처사이트"])
    enriched["notice_id"] = series_from_candidates(enriched, ["notice_id", "공고ID"])
    enriched["notice_title"] = series_from_candidates(enriched, ["notice_title", "공고명"])
    enriched["project_name"] = series_from_candidates(enriched, ["project_name", "과제명"])
    enriched["rfp_title"] = series_from_candidates(enriched, ["rfp_title", "RFP 제목"])
    enriched["file_name"] = series_from_candidates(enriched, ["file_name", "파일명"])
    enriched["validation_errors"] = series_from_candidates(enriched, ["validation_errors", "검증오류"])
    enriched["updated_at"] = series_from_candidates(enriched, ["updated_at", "수정일시"])
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
        or compact_text.endswith("마감")
        or compact_text.endswith("종료")
    )


def normalize_notice_status_label(value: object) -> str:
    text = clean(value)
    lowered = text.lower()
    if not text:
        return ""
    if "예정" in text or "pre" in lowered:
        return "예정"
    if "접수중" in text or "공고중" in text or "진행" in text or "ing" in lowered or "open" in lowered:
        return "접수중"
    if "마감" in text or "종료" in text or "closed" in lowered or "end" in lowered:
        return "마감"
    return text


def notice_status_badge_class(value: object) -> str:
    text = normalize_notice_status_label(value) or clean(value)
    if text == "접수중":
        return "is-open"
    if text == "예정":
        return "is-pending"
    if "임박" in text:
        return "is-deadline"
    if text == "마감":
        return "is-closed"
    return "is-pending"


def notice_status_badge_label(value: object) -> str:
    text = normalize_notice_status_label(value) or clean(value) or "미정"
    if text == "예정":
        return "접수예정"
    if text == "마감":
        return "접수마감"
    return text


def notice_status_badge_html(value: object) -> str:
    label = notice_status_badge_label(value)
    css_class = notice_status_badge_class(value)
    return f'<span class="notice-mailbox-status {css_class}">{escape(label)}</span>'


def normalize_review_status_label(value: object) -> str:
    return clean(value).replace(" ", "")


def is_archived_review_status_value(value: object) -> bool:
    return normalize_review_status_label(value) in {
        status.replace(" ", "")
        for status in ARCHIVE_REVIEW_STATUS_VALUES
    }


def derive_archive_reason_for_app(row: dict[str, object] | pd.Series) -> str:
    row_dict = normalize_row_dict(row)
    manual_archive = clean(first_non_empty(row_dict, "manual_archive")).upper() == "Y"
    review_status = first_non_empty(row_dict, "review_status", "검토여부", "검토 여부")
    current_value = first_non_empty(row_dict, "notice_is_current", "is_current")
    status_text = first_non_empty(row_dict, "notice_status", "status", "rcve_status", "공고상태")
    period_text = first_non_empty(row_dict, "notice_period", "period", "접수기간", "신청기간")
    period_end = extract_period_end(period_text)

    if manual_archive:
        return "manual_archive"
    if is_archived_review_status_value(review_status):
        return "review_archived"
    if pd.notna(period_end):
        period_end_ts = pd.Timestamp(period_end).normalize()
        if period_end_ts < pd.Timestamp.now().normalize():
            if clean(current_value) == "N" or normalize_notice_status_label(status_text) == "마감":
                return "notice_closed"
            return "application_closed"
    return ""


def derive_archive_reason_label_for_app(row: dict[str, object] | pd.Series) -> str:
    row_dict = normalize_row_dict(row)
    existing = first_non_empty(row_dict, "archive_reason_label")
    if existing:
        return existing
    mapping = {
        "notice_closed": "공고 마감",
        "application_closed": "접수 마감",
        "manual_archive": "수동 보관",
        "review_archived": "검토 보관",
    }
    return mapping.get(derive_archive_reason_for_app(row_dict), "")


def build_notice_archive_mask(df: pd.DataFrame) -> pd.Series:
    if df.empty:
        return pd.Series(dtype="bool")

    status_series = series_from_candidates(
        df,
        ["공고상태", "상태", "status", "rcve_status", "notice_status"],
    )
    review_series = series_from_candidates(
        df,
        ["검토 여부", "검토여부", "review_status"],
    )
    closed_mask = status_series.fillna("").astype(str).apply(is_closed_status_value)
    review_mask = review_series.fillna("").astype(str).apply(is_archived_review_status_value)
    return closed_mask | review_mask


def build_notice_status_scope_mask(df: pd.DataFrame, status_scope: str) -> pd.Series:
    if df.empty:
        return pd.Series(dtype="bool")
    if status_scope == "전체":
        return pd.Series(True, index=df.index)

    status_series = series_from_candidates(
        df,
        ["공고상태", "상태", "status", "rcve_status", "notice_status"],
    )
    normalized = status_series.fillna("").astype(str).apply(normalize_notice_status_label)
    return normalized.eq(status_scope)


def build_opportunity_status_scope_mask(df: pd.DataFrame, status_scope: str) -> pd.Series:
    if df.empty:
        return pd.Series(dtype="bool")
    if status_scope == "전체":
        return pd.Series(True, index=df.index)

    status_series = series_from_candidates(
        df,
        ["notice_status", "공고상태", "status", "rcve_status"],
    )
    normalized = status_series.fillna("").astype(str).apply(normalize_notice_status_label)
    return normalized.eq(status_scope)


def build_summary_current_mask(df: pd.DataFrame) -> pd.Series:
    if df.empty:
        return pd.Series(dtype="bool")

    mask = pd.Series(True, index=df.index)
    if "is_current" in df.columns:
        mask = mask & df["is_current"].fillna("").astype(str).str.strip().eq("Y")
    if "공고상태" in df.columns:
        mask = mask & ~df["공고상태"].apply(is_closed_status_value)
    return mask


def build_opportunity_archive_mask(df: pd.DataFrame) -> pd.Series:
    if df.empty:
        return pd.Series(dtype="bool")

    archive_mask = pd.Series(False, index=df.index)
    review_series = series_from_candidates(df, ["review_status", "검토여부", "검토 여부"])
    archive_mask = archive_mask | review_series.apply(is_archived_review_status_value)

    for status_source in ["notice_status", "status", "rcve_status", "공고상태"]:
        if status_source in df.columns:
            archive_mask = archive_mask | df[status_source].apply(is_closed_status_value)

    status_key_source = "status_key" if "status_key" in df.columns else "상태키"
    if status_key_source in df.columns:
        status_key = df[status_key_source].fillna("").astype(str).str.strip()
        archive_mask = archive_mask | status_key.isin(["ancmCls", "ancmEnd"])

    period_source = "notice_period" if "notice_period" in df.columns else "period" if "period" in df.columns else "접수기간"
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
    if df.empty or status_scope == "전체":
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
        "확인 후 해당 rfp에 접수",
        "기술 분류",
        "연구개발계획서 작성서식",
        "r&d 자율성트랙",
    }:
        return True
    if compact.startswith("><") or compact.count("><") >= 2:
        return True
    return any(
        marker in compact
        for marker in [
            "관리번호",
            "선정예정 과제수",
            "당해 연구비",
            "내역 사업명",
            "대분류",
            "중분류",
            "소분류",
            "지원기간 지원규모",
            "작성서식",
        ]
    )


def build_placeholder_opportunity_mask(df: pd.DataFrame) -> pd.Series:
    if df.empty:
        return pd.Series(dtype=bool)
    project_name = series_from_candidates(df, ["llm_project_name", "project_name", "과제명"]).fillna("").astype(str).str.strip()
    rfp_title = series_from_candidates(df, ["llm_rfp_title", "rfp_title", "RFP 제목"]).fillna("").astype(str).str.strip()
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
    if notice_df.empty or "공고ID" not in enriched.columns or "공고ID" not in notice_df.columns:
        return enriched

    notice_meta = notice_df.copy()
    notice_meta["공고ID"] = notice_meta["공고ID"].fillna("").astype(str).str.strip()
    keep_columns = ["공고ID", "상세링크", "검토 여부", "전문기관", "소관부처", "공고상태", "접수기간", "공고일자", "상태키", "status_key", "is_current"]
    available_columns = [column for column in keep_columns if column in notice_meta.columns]
    notice_meta = notice_meta[available_columns].drop_duplicates(subset=["공고ID"], keep="first")

    enriched["공고ID"] = enriched["공고ID"].fillna("").astype(str).str.strip()
    merged = enriched.merge(notice_meta, on="공고ID", how="left", suffixes=("", "_notice"))

    for target in ["상세링크", "검토 여부", "전문기관", "소관부처", "공고상태", "접수기간", "공고일자", "상태키", "status_key", "is_current"]:
        candidate_columns = [target]
        notice_target = f"{target}_notice"
        if notice_target in merged.columns:
            candidate_columns.append(notice_target)
        merged[target] = series_from_candidates(merged, candidate_columns)

    merged["상세링크"] = merged.apply(resolve_external_detail_link, axis=1)
    return merged


def build_notice_analysis_summary(opportunity_df: pd.DataFrame) -> pd.DataFrame:
    if opportunity_df.empty or "notice_id" not in opportunity_df.columns:
        return pd.DataFrame(
            columns=["공고ID", "대표추천도", "대표점수", "대표과제명", "대표예산", "대표추천이유", "대표키워드"]
        )

    working = opportunity_df.copy()
    if "rfp_score" in working.columns:
        working["rfp_score"] = to_numeric_column(working["rfp_score"])
    else:
        working["rfp_score"] = 0

    recommendation_rank = {
        "추천": 3,
        "검토권장": 2,
        "보통": 1,
        "비추천": 0,
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
            "공고ID": best["notice_id"].fillna("").astype(str).str.strip(),
            "대표추천도": best.get("llm_recommendation", best.get("recommendation", "")).fillna("").astype(str).str.strip(),
            "대표점수": best.get("llm_fit_score", best["rfp_score"]),
            "대표과제명": best["_project_name"],
            "대표예산": best["_budget"],
            "대표추천이유": best["_reason"],
            "대표키워드": best["_keywords"],
        }
    )


def merge_notice_with_analysis(notice_df: pd.DataFrame, opportunity_df: pd.DataFrame) -> pd.DataFrame:
    if notice_df.empty:
        return notice_df

    summary_df = build_notice_analysis_summary(opportunity_df)
    merged = notice_df.copy()

    if summary_df.empty or "공고ID" not in merged.columns:
        for column in ["대표추천도", "대표점수", "대표과제명", "대표예산", "대표추천이유", "대표키워드"]:
            if column not in merged.columns:
                merged[column] = ""
        return merged

    merged["공고ID"] = merged["공고ID"].fillna("").astype(str).str.strip()
    merged = merged.merge(summary_df, on="공고ID", how="left", suffixes=("", "_analysis"))

    for column in ["대표추천도", "대표과제명", "대표예산", "대표추천이유", "대표키워드"]:
        merged[column] = merged[column].fillna("").astype(str).str.strip()
    merged["대표점수"] = to_numeric_column(merged["대표점수"])
    return merged


def build_contains_mask(df: pd.DataFrame, columns: list[str], query: str) -> pd.Series:
    if not query:
        return pd.Series(True, index=df.index)

    mask = pd.Series(False, index=df.index)
    for column in columns:
        if column in df.columns:
            mask = mask | df[column].fillna("").str.contains(query, case=False, na=False)
    return mask


def _legacy_apply_multiselect_filter(df: pd.DataFrame, column: str, label: str, key: str) -> pd.DataFrame:
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


def _legacy_render_sidebar_search(key: str = "sidebar_search") -> str:
    st.sidebar.markdown("## Common Filters")
    return st.sidebar.text_input("통합 검색", "", key=unified_sidebar_filter_key(key))


def _legacy_render_notice_filter_sidebar(
    key_prefix: str,
    *,
    current_only_default: bool = True,
    status_default: str = "전체",
    show_current_only: bool = True,
    show_status_scope: bool = True,
) -> tuple[str, bool, str]:
    search_text = render_sidebar_search(f"{key_prefix}_search")

    current_only = current_only_default
    if show_current_only:
        current_only = st.sidebar.checkbox(
            "현재 공고만",
            value=current_only_default,
            key=unified_sidebar_filter_key(f"{key_prefix}_current"),
        )

    status_scope = status_default
    if show_status_scope:
        status_options = ["전체", "접수중", "예정", "마감"]
        default_status = status_default if status_default in status_options else "전체"
        status_scope = st.sidebar.selectbox(
            "공고 상태",
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
            placeholder="공고명 / 과제명 / 기관명 검색",
            label_visibility="collapsed",
        )
    with header_cols[2]:
        st.markdown('<div class="workspace-toolbar">', unsafe_allow_html=True)
        st.markdown('<div class="workspace-toolbar-note">🔔</div>', unsafe_allow_html=True)
        st.markdown(f'<div class="workspace-user-chip">👤 {escape(user_label or user_id or "User")}</div>', unsafe_allow_html=True)
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
    st.markdown(
        """
        <style>
        :root {
          --linear-bg: #f8fafc;
          --linear-panel: #ffffff;
          --linear-surface: #ffffff;
          --linear-surface-hover: #f1f5f9;
          --linear-border-subtle: rgba(15, 23, 42, 0.06);
          --linear-border: rgba(15, 23, 42, 0.10);
          --linear-text: #15233b;
          --linear-text-secondary: #21314d;
          --linear-text-muted: #6c7f9d;
          --linear-text-faint: #94a3b8;
          --linear-accent: #2563eb;
          --linear-accent-bg: #2563eb;
          --linear-accent-hover: #1d4ed8;
          --linear-success: #10b981;
          --linear-danger: #f87171;
          --linear-shadow: rgba(15, 23, 42, 0.04) 0px 10px 24px;
          --linear-radius-sm: 6px;
          --linear-radius-md: 14px;
          --linear-radius-lg: 16px;
        }
        html, body, [class*="css"], .stApp {
          font-family: "Segoe UI", "Noto Sans KR", "Apple SD Gothic Neo", sans-serif;
          font-feature-settings: "cv01", "ss03";
        }
        body {
          background: var(--linear-bg);
          color: var(--linear-text);
        }
        .stApp,
        [data-testid="stAppViewContainer"],
        [data-testid="stAppViewBlockContainer"] {
          background: var(--linear-bg);
          color: var(--linear-text);
        }
        .main .block-container {
          max-width: 1440px;
          padding-top: 1.15rem;
          padding-bottom: 3rem;
          padding-left: clamp(1.2rem, 2vw, 2rem);
          padding-right: clamp(1.2rem, 2vw, 2rem);
        }
        h1 {
          color: var(--linear-text) !important;
          font-size: 2.55rem !important;
          font-weight: 510 !important;
          line-height: 1.02 !important;
          letter-spacing: -0.065rem !important;
          margin-bottom: 0.15rem !important;
        }
        h2, h3 {
          color: var(--linear-text) !important;
          letter-spacing: -0.02em;
        }
        [data-testid="stCaptionContainer"] p,
        .stCaption p {
          color: var(--linear-text-muted) !important;
          font-size: 0.95rem !important;
        }
        [data-testid="stSidebar"] {
          background: var(--linear-panel);
          border-right: 1px solid var(--linear-border-subtle);
        }
        [data-testid="stHeader"] {
          background: rgba(255, 255, 255, 0.92);
          border-bottom: 1px solid var(--linear-border-subtle);
        }
        [data-testid="stToolbar"] {
          right: 1rem;
        }
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
    start_text = re.split(r"\s*[~〜-]\s*", text, maxsplit=1)[0].strip()
    return pd.to_datetime(start_text.replace(".", "-"), errors="coerce")


def build_public_d_day(period_value: object) -> str:
    period_end = extract_period_end(period_value)
    if pd.isna(period_end):
        return ""
    days = int((period_end.normalize() - pd.Timestamp.now().normalize()).days)
    if days < 0:
        return "마감"
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
    merged = {**top_related, **normalize_row_dict(row)}
    period = public_first_non_empty(merged, "접수기간", "notice_period", "period", "신청기간")
    budget = extract_budget_summary(
        public_first_non_empty(
            merged,
            "대표예산",
            "사업비",
            "예산",
            "llm_total_budget_text",
            "total_budget_text",
            "budget",
            "llm_per_project_budget_text",
            "per_project_budget_text",
        )
    )
    title = public_first_non_empty(
        merged,
        "llm_project_name" if kind == "opportunity" else "공고명",
        "project_name",
        "공고명",
        "notice_title",
        "llm_rfp_title",
        "rfp_title",
    )
    notice_title = public_first_non_empty(merged, "공고명", "notice_title")
    subtitle_parts = [
        public_first_non_empty(merged, "공고상태", "rcve_status"),
        public_first_non_empty(merged, "사업명", "notice_title") if kind != "opportunity" else notice_title,
    ]
    subtitle = " | ".join(part for part in subtitle_parts if part and part != title)
    notice_date = public_first_non_empty(merged, "공고일자", "ancm_de", "registered_at")
    ministry = public_first_non_empty(merged, "소관부처", "ministry", "주관부처")
    agency = public_first_non_empty(merged, "전문기관명", "전문기관", "agency", "담당부서")
    org_type = public_first_non_empty(
        merged,
        "지원 가능 기관 유형",
        "지원가능기관유형",
        "eligible_org_type",
        "llm_eligible_org_type",
        "applicant_type",
    )
    region = public_first_non_empty(
        merged,
        "지원 가능 소재지",
        "지원가능소재지",
        "eligible_region",
        "llm_eligible_region",
        "region",
    )
    sales = public_first_non_empty(
        merged,
        "지원 가능 매출액 / 사업연수",
        "매출액",
        "사업연수",
        "eligible_sales",
        "llm_eligible_sales",
    )
    lab = public_first_non_empty(
        merged,
        "부설 연구소 필요 유무",
        "부설연구소",
        "lab_required",
        "llm_lab_required",
    )
    requirement_values = [org_type, region, sales, lab]
    requirement_count = sum(1 for value in requirement_values if value and value not in {"-", "-/-"})
    score = clean(public_first_non_empty(merged, "llm_fit_score", "rfp_score", "대표점수"))
    if score:
        try:
            requirement_count = max(requirement_count, min(4, round(float(score) / 25)))
        except Exception:
            pass
    display_requirement_count = max(0, min(4, requirement_count))
    progress = max(1, display_requirement_count) * 25
    d_day = build_public_d_day(period)
    tags = split_public_tags(public_first_non_empty(merged, "대표키워드", "llm_keywords", "keywords", "keyword"), limit=3)

    info_rows = [
        ("신청 기간", period),
        ("지원금", budget),
        ("부처", ministry),
        ("전문기관명", agency),
        ("공고등록일", notice_date),
    ]
    fit_rows = [
        ("지원 가능 기관 유형", org_type or "-"),
        ("지원 가능 소재지", region or "전국"),
        ("지원 가능 매출액 / 사업연수", sales or "-/-"),
        ("부설 연구소 필요 유무", lab or "-"),
    ]
    info_html = []
    for label, value in info_rows:
        if label == "지원금" and value:
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
            <div class="public-save-button"><span class="public-save-icon">♡</span><span>저장하기</span></div>
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
    merged = {**top_related, **normalize_row_dict(row)}
    period = public_first_non_empty(merged, "접수기간", "notice_period", "period", "신청기간")
    d_day = build_public_d_day(period)
    support_type = public_first_non_empty(merged, "지원 유형", "공모유형", "pbofr_type", "project_type") or "연구개발"
    keywords = split_public_tags(public_first_non_empty(merged, "대표키워드", "llm_keywords", "keywords", "keyword"), limit=8)
    org_type = public_first_non_empty(merged, "지원 가능 기관 유형", "지원가능기관유형", "eligible_org_type", "llm_eligible_org_type", "applicant_type")
    region = public_first_non_empty(merged, "지원 가능 소재지", "지원가능소재지", "eligible_region", "llm_eligible_region", "region") or "전국"
    sales = public_first_non_empty(merged, "지원 가능 매출액 / 사업연수", "매출액", "사업연수", "eligible_sales", "llm_eligible_sales") or "-/-"
    lab = public_first_non_empty(merged, "부설 연구소 필요 유무", "부설연구소", "lab_required", "llm_lab_required") or "-"
    total_budget = extract_budget_summary(public_first_non_empty(merged, "사업 규모", "사업비", "대표예산", "llm_total_budget_text", "total_budget_text", "budget"))
    grant = extract_budget_summary(public_first_non_empty(merged, "지원금", "과제별 예산", "llm_per_project_budget_text", "per_project_budget_text")) or total_budget
    deadline = extract_period_end(period)
    deadline_text = deadline.strftime("%Y-%m-%d") if pd.notna(deadline) else ""
    summary = public_first_non_empty(
        merged,
        "과제 분석",
        "llm_summary",
        "summary",
        "대표추천이유",
        "llm_reason",
        "reason",
        "text_preview",
    )
    summary = build_project_analysis_text(merged) if clean(summary) else summary
    overview = public_first_non_empty(
        merged,
        "사업 개요 및 배경",
        "과제 개요",
        "llm_concept_and_development",
        "concept_and_development",
        "지원필요성(과제 배경)",
        "support_necessity",
        "technical_background",
    )
    objective = public_first_non_empty(
        merged,
        "과제 목표",
        "llm_application_field",
        "application_field",
        "활용분야",
    )
    detail = public_first_non_empty(
        merged,
        "과제 내용",
        "지원 내용",
        "llm_support_plan",
        "support_plan",
        "지원기간 및 예산·추진체계",
        "텍스트 미리보기",
        "text_preview",
    )
    requirement_history = public_first_non_empty(
        merged,
        "과제 수행 이력 요건",
        "기타 지원 조건",
        "other_requirements",
        "llm_other_requirements",
    )
    contribution = public_first_non_empty(
        merged,
        "기관 분담률",
        "matching_fund",
        "llm_matching_fund",
    )
    extra_detail = public_first_non_empty(
        merged,
        "기타 세부 사항",
        "기타 지원 조건",
        "llm_requirements",
        "requirements",
    )

    info_items = [
        ("지원 유형", support_type),
        ("핵심 키워드", " ".join(keywords)),
        ("사업 규모", total_budget),
        ("지원금", grant),
        ("지원 가능 기관", org_type),
        ("공고 등록일", public_first_non_empty(merged, "공고일자", "ancm_de", "registered_at")),
        ("공고 마감일", deadline_text),
        ("신청 기간", f"{d_day}\n{period}" if d_day else period),
    ]
    requirements = [
        ("지원 가능 기관 유형", org_type or "-"),
        ("지원 가능 소재지", region),
        ("지원 가능 매출액 / 사업연수", sales),
        ("부설 연구소 필요 유무", lab),
    ]
    detail_items = [
        ("공모 유형", public_first_non_empty(merged, "공모유형", "pbofr_type")),
        ("과제 기간", public_first_non_empty(merged, "과제 기간", "project_period", "support_period")),
        ("사업 규모", total_budget),
        ("지원금", grant),
        ("지원 내용", detail),
        ("기관 분담률", contribution),
        ("기타 세부 사항", extra_detail),
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
        f'<div class="rnd-section"><div class="rnd-section-title">주요 정보</div><div class="rnd-info-grid">{info_grid(info_items)}</div></div>',
    ]
    if summary:
        sections.append(f'<div class="rnd-section"><div class="rnd-section-title">과제 분석</div><div class="rnd-section-body">{escape(summary)}</div></div>')
    sections.append(f'<div class="rnd-section"><div class="rnd-section-title">요건 충족도</div><div class="rnd-requirement-list">{requirement_html}</div></div>')
    support_requirements = [("기업부설연구소 요건", lab), ("과제 수행 이력 요건", requirement_history)]
    sections.append(f'<div class="rnd-section"><div class="rnd-section-title">지원 요건</div><div class="rnd-info-grid">{info_grid(support_requirements)}</div></div>')
    overview_body = "\n\n".join(part for part in [overview, objective] if clean(part))
    if overview_body:
        sections.append(f'<div class="rnd-section"><div class="rnd-section-title">과제 개요</div><div class="rnd-section-body">{escape(overview_body)}</div></div>')
    sections.append(f'<div class="rnd-section"><div class="rnd-section-title">과제 세부 내용</div><div class="rnd-info-grid">{info_grid(detail_items)}</div></div>')

    st.markdown(f'<div class="rnd-detail-stack">{"".join(sections)}</div>', unsafe_allow_html=True)


def render_detail_card(title: str, fields: list[tuple[str, str]]) -> None:
    items = []
    for label, value in fields:
        raw_value = sanitize_display_text(label, value)
        display_value = display_value_for_label(label, value)
        if not clean(display_value):
            display_value = detail_display_text(label, value)

        if display_value != MISSING_DISPLAY_TEXT and should_use_expandable_value(label, raw_value):
            preview_text, _ = split_preview_and_remainder(
                raw_value,
                max_chars=preview_max_chars_for_label(label),
            )
            if "예산" in clean(label):
                preview_text = display_value
            items.append(
                (
                    f'<div class="detail-field">'
                    f'<div class="detail-label">{escape(label)}</div>'
                    f'<details class="detail-more">'
                    f'<summary>'
                    f'<span class="detail-preview-text">{escape(preview_text)}</span>'
                    f'<span class="detail-toggle-text">더보기</span>'
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
            '<div class="detail-field"><div class="detail-value">표시할 정보가 없습니다.</div></div>'
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
        r"^(전략적합도|전략 적합도|기술적합도|기술 관련도|기술관련도|시장정렬|시장 정렬|시장정합성|시장 정합성|긴급도|긴급성|소프트웨어적합도|소프트웨어 적합도|하드웨어의존도|하드웨어 의존도)\s*:\s*",
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
        first_non_empty(merged, "llm_total_budget_text", "total_budget_text", "budget", "대표예산", "사업비")
    )
    period_text = _analysis_clause(
        first_non_empty(merged, "rfp_period", "project_period", "support_period", "notice_period", "period", "접수기간"),
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

    software_markers = ["ai", "데이터", "platform", "플랫폼", "api", "cloud", "saas", "알고리즘", "분석", "서비스", "시뮬레이션"]
    hardware_markers = ["센서", "부품", "장비", "디바이스", "모듈", "제조", "반도체", "배터리", "소재", "로봇", "시제품", "양산"]
    sw_hits = sum(1 for marker in software_markers if marker in merged_blob.lower())
    hw_hits = sum(1 for marker in hardware_markers if marker in merged_blob.lower())

    paragraphs: list[str] = []
    if objective:
        _append_analysis_paragraph(paragraphs, _ensure_analysis_sentence(f"이 과제는 {objective}을 목표로 한다"))
    elif reason_text:
        _append_analysis_paragraph(paragraphs, _ensure_analysis_sentence(reason_text))

    if development:
        _append_analysis_paragraph(paragraphs, _ensure_analysis_sentence(f"핵심 개발 범위는 {development} 중심으로 구성된다"))
    elif keywords:
        _append_analysis_paragraph(
            paragraphs,
            _ensure_analysis_sentence(f"핵심 기술 요소는 {', '.join(keywords[:4])} 중심으로 해석된다"),
        )

    if market_fields:
        _append_analysis_paragraph(
            paragraphs,
            _ensure_analysis_sentence(
                f"특히 {', '.join(market_fields[:3])} 분야와의 연결성이 높아 실제 사업화와 인접 시장 확장 가능성을 함께 검토할 만하다"
            ),
        )
    elif support_need:
        _append_analysis_paragraph(
            paragraphs,
            _ensure_analysis_sentence(f"{support_need} 수요와 직접 연결될 가능성이 있어 사업 기회 관점에서 검토 가치가 있다"),
        )

    if sw_hits >= max(2, hw_hits + 1):
        _append_analysis_paragraph(
            paragraphs,
            "데이터·AI·플랫폼 연계 비중이 높아 소프트웨어·플랫폼 중심 기업에 적합한 Opportunity로 판단된다.",
        )
    elif hw_hits >= max(2, sw_hits + 1):
        _append_analysis_paragraph(
            paragraphs,
            "장비·부품·제조 연계 비중이 높아 하드웨어 통합과 실증 수행 역량이 중요한 과제로 판단된다.",
        )
    else:
        _append_analysis_paragraph(
            paragraphs,
            "소프트웨어와 현장 실증 요소가 함께 요구되는 융합형 과제로, 서비스 운영 역량과 기술 구현 역량을 함께 갖춘 조직에 적합하다.",
        )

    execution_bits: list[str] = []
    if period_text:
        execution_bits.append(f"사업기간은 {period_text} 수준이다")
    if total_budget:
        execution_bits.append(f"예산 규모는 {total_budget}로 확인된다")
    if support_plan:
        execution_bits.append(f"{support_plan} 등을 고려하면 실증 및 운영 연계 가능성을 검토할 만하다")
    if execution_bits:
        _append_analysis_paragraph(paragraphs, _ensure_analysis_sentence(". ".join(execution_bits)))

    if reason_text and len(paragraphs) < 5:
        _append_analysis_paragraph(paragraphs, _ensure_analysis_sentence(reason_text))

    if not paragraphs:
        return "연결된 RFP 분석이 아직 없습니다.\n\n공고 원문과 연결 Opportunity를 함께 확인해주세요."
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
UNFAVORITE_REVIEW_STATUS = "???"
STATUS_FILTER_OPTIONS: list[tuple[str, str]] = [
    ("??", "??"),
    ("???", "???"),
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
    ("????", "favorite"),
    ("??/??", "archive"),
]
RECOMMENDATION_RANK = {
    "??": 3,
    "??": 1,
    "???": 0,
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
    row_dict = normalize_row_dict(row)
    title_text = _normalize_key_text(first_non_empty(row_dict, "notice_title", "공고명"))
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
            return _truncate_queue_text(f"{project_text}. {field_text} 분야와 연결된 과제로 검토할 수 있습니다.")
        return _truncate_queue_text(project_text)
    if market_text and field_text:
        return _truncate_queue_text(f"{market_text}과 {field_text} 분야 확장 가능성이 있는 과제로 보입니다.")
    if field_text:
        return _truncate_queue_text(f"{field_text} 분야 중심의 과제로 판단됩니다.")
    if market_text:
        return _truncate_queue_text(f"{market_text} 시장과의 연결성이 높은 과제로 보입니다.")
    if keyword_text:
        return _truncate_queue_text(f"{keyword_text} 중심의 기술 Opportunity로 검토할 수 있습니다.")
    return ""

def _review_value(row: dict | pd.Series | None) -> str:
    row_dict = normalize_row_dict(row)
    return clean(first_non_empty(row_dict, "review_status", "검토 여부", "검토여부"))

def _review_series(rows: pd.DataFrame) -> pd.Series:
    return _safe_series(rows, ["review_status", "검토 여부", "검토여부"])

def _is_favorite(row_or_value: dict | pd.Series | str | None) -> bool:
    value = _review_value(row_or_value) if isinstance(row_or_value, (dict, pd.Series)) else clean(row_or_value)
    return value == FAVORITE_REVIEW_STATUS

def _favorite_button_label(current_value: str) -> tuple[bool, str]:
    is_favorite = _is_favorite(current_value)
    return is_favorite, "해제" if is_favorite else "등록"

def _favorite_badge_html() -> str:
    return '<span class="notice-chip notice-chip-favorite">관심</span>'

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
    if callable(update_notice_review_status):
        update_notice_review_status(
            notice_id,
            review_status,
            source_key=normalized_source,
        )
    elif callable(save_review_status):
        save_review_status(
            notice_id=notice_id,
            review_status=review_status,
            source_key=normalized_source,
        )
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
    if get_query_param("favorite_toggle") != "1":
        return

    notice_id = clean(get_query_param("favorite_notice_id"))
    source_key = clean(get_query_param("favorite_source_key")) or "iris"
    current_value = clean(get_query_param("favorite_current_value"))
    notice_title = clean(get_query_param("favorite_notice_title"))
    next_value = UNFAVORITE_REVIEW_STATUS if current_value == FAVORITE_REVIEW_STATUS else FAVORITE_REVIEW_STATUS

    try:
        if notice_id:
            _persist_review_status(
                notice_id=notice_id,
                source_key=source_key,
                review_status=next_value,
                notice_title=notice_title,
            )
    finally:
        params = get_query_params_dict()
        for key in [
            "favorite_toggle",
            "favorite_notice_id",
            "favorite_source_key",
            "favorite_current_value",
            "favorite_notice_title",
        ]:
            params.pop(key, None)
        _replace_params(_auth_params(params))
        st.rerun()

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
        button_label = "★" if is_favorite else "☆"
    next_value = UNFAVORITE_REVIEW_STATUS if is_favorite else FAVORITE_REVIEW_STATUS
    safe_key = _css_safe_key(button_key)
    if compact:
        active_bg = "#fff7ed" if is_favorite else "#ffffff"
        active_border = "#fdba74" if is_favorite else "#cbd5e1"
        active_color = "#c2410c" if is_favorite else "#64748b"
        min_width = "42px" if icon_only else "auto"
        padding = "0" if icon_only else "0.15rem 0.8rem"
        font_size = "1.02rem" if icon_only else "0.88rem"
        style_block = (
            """
            <style>
            .st-key-__SAFE_KEY__ {{
              display: flex;
              justify-content: flex-end;
            }}
            .st-key-__SAFE_KEY__ button {{
              min-height: 36px !important;
              min-width: __MIN_WIDTH__ !important;
              padding: __PADDING__ !important;
              border-radius: 999px !important;
              border: 1px solid __ACTIVE_BORDER__ !important;
              background: __ACTIVE_BG__ !important;
              color: __ACTIVE_COLOR__ !important;
              font-size: __FONT_SIZE__ !important;
              font-weight: 800 !important;
              white-space: nowrap !important;
              box-shadow: none !important;
            }}
            </style>
            """
            .replace("__SAFE_KEY__", safe_key)
            .replace("__MIN_WIDTH__", min_width)
            .replace("__PADDING__", padding)
            .replace("__ACTIVE_BORDER__", active_border)
            .replace("__ACTIVE_BG__", active_bg)
            .replace("__ACTIVE_COLOR__", active_color)
            .replace("__FONT_SIZE__", font_size)
        )
        st.markdown(
            style_block,
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
        "all": "전체",
        "전체": "전체",
        "current": "진행중",
        "진행중": "진행중",
        "scheduled": "예정",
        "예정": "예정",
        "archive": "마감",
        "closed": "마감",
        "마감": "마감",
    }
    return alias_map.get(normalized, "전체")

def _normalize_recommendation_value(value: object) -> str:
    text = clean(value)
    lowered = text.lower()
    if not text:
        return ""
    if any(marker in lowered for marker in ("비추천", "미추천", "not recommend", "reject")):
        return "비추천"
    if "검토권장" in text:
        return "보통"
    if "보통" in text:
        return "보통"
    if "추천" in text or "recommend" in lowered:
        return "추천"
    if "검토" in text or "보류" in text or "hold" in lowered:
        return "보통"
    return text

def build_positive_recommendation_mask(df: pd.DataFrame) -> pd.Series:
    if df is None or df.empty:
        return pd.Series(dtype=bool)
    recommendation_series = series_from_candidates(
        df,
        ["Recommendation", "_queue_recommendation", "recommendation", "추천여부", "llm_recommendation"],
    ).fillna("").astype(str)
    return recommendation_series.apply(_normalize_recommendation_value).eq("추천")


def _recommendation_sort_rank(value: object) -> int:
    normalized = _normalize_recommendation_value(value)
    if normalized == "추천":
        return 0
    if normalized == "보통":
        return 1
    if normalized in {"비추천", "미추천"}:
        return 3
    if clean(normalized):
        return 2
    return 2


def is_positive_recommendation(value: object) -> bool:
    return _normalize_recommendation_value(clean(value)) == "추천"


def _reset_favorites_workspace_filters() -> None:
    st.session_state.pop("favorites_workspace_type", None)
    st.session_state.pop("favorites_workspace_search", None)
    st.session_state.pop("favorites_workspace_sort", None)


def _sort_favorites_workspace_entries(rows: pd.DataFrame, sort_option: str) -> pd.DataFrame:
    if rows.empty:
        return rows

    working = rows.copy()
    if "_sort_date" not in working.columns:
        working["_sort_date"] = pd.NaT

    if sort_option == "추천도순":
        working["_queue_score_sort"] = (
            to_numeric_column(working["Score"])
            if "Score" in working.columns
            else pd.Series(0, index=working.index, dtype="float64")
        )
        return working.sort_values(
            by=["_queue_score_sort", "_sort_date", "Title"],
            ascending=[False, False, True],
            na_position="last",
            kind="stable",
        )
    if sort_option == "최신 등록일순":
        return working.sort_values(
            by=["_sort_date", "Title"],
            ascending=[False, True],
            na_position="last",
            kind="stable",
        )
    working["_queue_deadline_sort"] = working["D-Day"].apply(
        lambda value: 0
        if clean(value) == "D-Day"
        else (int(re.fullmatch(r"D-(\d+)", clean(value)).group(1)) if re.fullmatch(r"D-(\d+)", clean(value)) else 9999)
    )
    return working.sort_values(
        by=["_queue_deadline_sort", "_sort_date", "Title"],
        ascending=[True, False, True],
        na_position="last",
        kind="stable",
    )


def _encode_notice_queue_query_list(values: list[str]) -> str:
    cleaned = [clean(value) for value in values if clean(value)]
    return json.dumps(cleaned, ensure_ascii=False)


def _decode_notice_queue_query_list(raw_value: str) -> list[str]:
    text = clean(raw_value)
    if not text:
        return []
    try:
        parsed = json.loads(text)
    except Exception:
        parsed = [item.strip() for item in text.split(",")]
    if not isinstance(parsed, list):
        return []
    return [clean(item) for item in parsed if clean(item)]


def _current_notice_queue_filter_params() -> dict[str, str]:
    params: dict[str, str] = {}
    search_text = clean(st.session_state.get("notice_queue_workspace_search"))
    selected_status = st.session_state.get("notice_queue_workspace_status_inline") or []
    selected_recommendations = st.session_state.get("notice_queue_workspace_recommendation") or []
    sort_option = clean(st.session_state.get("notice_queue_workspace_sort"))

    if search_text:
        params["nq_search"] = search_text
    if selected_status:
        params["nq_status"] = _encode_notice_queue_query_list(list(selected_status))
    if selected_recommendations:
        params["nq_reco"] = _encode_notice_queue_query_list(list(selected_recommendations))
    if sort_option:
        params["nq_sort"] = sort_option
    return params


def _notice_queue_filter_signature(
    *,
    search_text: object | None = None,
    selected_status: list[str] | tuple[str, ...] | None = None,
    selected_recommendations: list[str] | tuple[str, ...] | None = None,
    sort_option: object | None = None,
) -> tuple[str, tuple[str, ...], tuple[str, ...], str]:
    return (
        clean(st.session_state.get("notice_queue_workspace_search") if search_text is None else search_text),
        tuple(
            sorted(
                clean(value)
                for value in (
                    st.session_state.get("notice_queue_workspace_status_inline")
                    if selected_status is None
                    else selected_status
                ) or []
                if clean(value)
            )
        ),
        tuple(
            sorted(
                clean(value)
                for value in (
                    st.session_state.get("notice_queue_workspace_recommendation")
                    if selected_recommendations is None
                    else selected_recommendations
                ) or []
                if clean(value)
            )
        ),
        clean(st.session_state.get("notice_queue_workspace_sort") if sort_option is None else sort_option),
    )


def _notice_queue_query_signature() -> tuple[str, tuple[str, ...], tuple[str, ...], str]:
    return (
        clean(get_query_param("nq_search")),
        tuple(sorted(clean(value) for value in _decode_notice_queue_query_list(get_query_param("nq_status")) if clean(value))),
        tuple(sorted(clean(value) for value in _decode_notice_queue_query_list(get_query_param("nq_reco")) if clean(value))),
        clean(get_query_param("nq_sort")) or "최신 등록일순",
    )


def _reset_notice_queue_filters(page_state_key: str, filter_reset_keys: list[str]) -> None:
    for key in filter_reset_keys:
        st.session_state.pop(key, None)
    st.session_state.pop(page_state_key, None)
    st.session_state.pop("notice_queue_workspace_filter_signature", None)


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
    return clean(first_non_empty(row, *NOTICE_ID_CANDIDATES))

def _get_notice_row_by_id(rows: pd.DataFrame, notice_id: str) -> dict | pd.Series | None:
    selected_notice_id = clean(notice_id)
    if rows is None or rows.empty or not selected_notice_id:
        return None
    for column in NOTICE_ID_CANDIDATES:
        selected_row = get_row_by_column_value(rows, column, selected_notice_id)
        if selected_row is not None:
            return selected_row
    return None

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
    working["notice_id"] = _safe_series(working, ["notice_id", "공고ID", "Notice ID", "source_notice_id"])
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
        ["llm_recommendation", "recommendation", "추천여부", "Recommendation"],
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
        ["target_market", "대표관심영역", "llm_score_target_markets"],
    )
    working["_queue_support_type"] = _safe_series(
        working,
        ["pbofr_type", "공모유형", "support_type", "project_type"],
    )
    working["_queue_notice_period"] = _safe_series(
        working,
        ["notice_period", "period", "접수기간", "신청기간", "요청기간"],
    )
    working["_queue_notice_no"] = _safe_series(
        working,
        ["notice_no", "ancm_no", "공고번호"],
    )
    working["_queue_notice_date"] = _safe_series(
        working,
        ["registered_at", "ancm_de", "공고일자", "등록일"],
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
    enriched["_notice_id"] = _safe_series(enriched, ["공고ID", "notice_id"])
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
    enriched["notice_no"] = _safe_series(enriched, ["notice_no", "공고번호", "ancm_no", "_queue_notice_no"])
    enriched["registered_at"] = _safe_series(enriched, ["registered_at", "공고일자", "ancm_de", "_queue_notice_date"])
    enriched["pbofr_type"] = _safe_series(enriched, ["pbofr_type", "공모유형", "support_type", "_queue_support_type"])
    enriched["notice_period"] = _safe_series(enriched, ["notice_period", "접수기간", "period", "신청기간", "_queue_notice_period"])
    enriched["_queue_analysis"] = enriched.apply(_compose_queue_analysis, axis=1)
    return enriched

def _matches_search(rows: pd.DataFrame, search_text: str) -> pd.Series:
    query = clean(search_text).lower()
    if rows.empty or not query:
        return pd.Series(True, index=rows.index)

    columns = [
        "공고명",
        "notice_title",
        "_queue_project_name",
        "전문기관",
        "agency",
        "소관부처",
        "주관부처",
        "ministry",
        "매체",
        "source_label",
        "공고번호",
        "notice_no",
    ]
    stacked = pd.Series("", index=rows.index, dtype="object")
    for column in columns:
        if column in rows.columns:
            stacked = stacked + " " + rows[column].fillna("").astype(str)
    return stacked.str.lower().str.contains(query, na=False)

def _normalize_status_filter_values(value: object) -> list[str]:
    allowed_values = [option for option, _ in STATUS_FILTER_OPTIONS if clean(option) and option != "전체"]
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
    allowed_values = [option for option, _ in RECOMMENDATION_FILTER_OPTIONS if clean(option) and option != "전체"]
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
            "진행중": "current",
            "예정": "scheduled",
            "마감": "archive",
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
        period_series = series_from_candidates(filtered, ["notice_period", "접수기간", "period"]).fillna("").astype(str)
        status_series = series_from_candidates(filtered, ["status", "rcve_status", "공고상태"]).fillna("").astype(str)
        today = pd.Timestamp.now().normalize()

        def _within_deadline_limit(period_text: str, status_text: str) -> bool:
            normalized_status = normalize_notice_status_label(status_text)
            if "마감" in normalized_status:
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

    st.markdown(
        (
            '<div class="notice-queue-footer">'
            f'<div class="notice-queue-footer-meta">총 {total_rows:,}건 · 페이지 {current_page} / {total_pages}</div>'
            f'<nav class="notice-queue-pagination-wrap" aria-label="pagination">{number_html}{nav_html}</nav>'
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
    if status == "마감":
        return "notice-chip notice-chip-status is-archive"
    if status == "예정":
        return "notice-chip notice-chip-status is-scheduled"
    return "notice-chip notice-chip-status"

def _recommendation_badge_html(value: str) -> str:
    normalized = _normalize_recommendation_value(value)
    if not normalized:
        return '<span class="notice-chip notice-chip-neutral">분석대기</span>'
    if normalized == "추천":
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
    empty_message: str = "??? ??? ????.",
    selected_notice_id: str = "",
    on_select=None,
) -> None:
    if rows is None or rows.empty:
        st.info(empty_message)
        return

    current_route = route_core.get_current_route()

    def _detail_href(notice_id: str, source_key: str) -> str:
        route_builder = route_core.build_favorites_route if page_key == "favorites" else route_core.build_notice_queue_route
        route = route_builder(
            filters=dict(current_route.get("filters") or {}),
            page_no=int(current_route.get("page_no") or 1),
            page_size=int(current_route.get("page_size") or 20),
            view="detail",
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
        status = normalize_notice_status_label(first_non_empty(row, "status", "rcve_status", "????"))
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
                    f'<a class="notice-table-title" href="{escape(_detail_href(notice_id, source_key or "iris"), quote=True)}" target="_self">{escape(_truncate_queue_text(title, max_chars=110))}</a>',
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

def _render_notice_queue_screen(
    source_df: pd.DataFrame,
    opportunity_df: pd.DataFrame,
    detail_opportunity_df: pd.DataFrame,
) -> None:
    del opportunity_df
    source_df = _enrich_notice_rows(source_df, detail_opportunity_df)
    current_route = route_core.get_current_route(route_core.build_notice_queue_route())
    if (
        clean(current_route.get("page")) == NOTICE_QUEUE_DETAIL_PAGE_KEY
        and clean(current_route.get("view")) in {"detail", "summary"}
    ):
        selected_row = _get_notice_row_by_id(source_df, clean(current_route.get("item_id")))
        back_col, info_col = st.columns([1.9, 4.1])
        with back_col:
            if st.button("← Notice Queue로 돌아가기", key=f"{NOTICE_QUEUE_DETAIL_PAGE_KEY}_back_to_table", use_container_width=False, type="secondary"):
                go_back_route(route_core.build_notice_queue_route())
                st.rerun()
        with info_col:
            st.markdown('<div class="page-note">Notice Queue / Notice Detail</div>', unsafe_allow_html=True)
        if selected_row is None:
            st.info("표시할 공고가 없습니다.")
            return
        render_notice_detail_from_row(selected_row, detail_opportunity_df)
        return

    render_page_header(
        "Notice Browser",
        "추천 Opportunity를 검토한 뒤, 원문 공고를 빠르게 훑고 상세 검토로 이어지는 compact browser입니다.",
        eyebrow="Notices",
    )
    _inject_opportunity_workspace_styles()
    render_notice_queue_ui_styles()
    _inject_notice_queue_dashboard_styles()
    if source_df is None or source_df.empty:
        st.info("??? ??? ????.")
        return

    filters = _get_notice_filters()
    status_widget_key = _notice_filter_widget_key("status")
    recommendation_widget_key = _notice_filter_widget_key("recommendation")
    search_widget_key = _notice_filter_widget_key("search")
    st.session_state.setdefault(status_widget_key, filters["status"])
    st.session_state.setdefault(recommendation_widget_key, filters["recommendation"])
    st.session_state.setdefault(search_widget_key, filters["search"])

    st.markdown(
        '<div class="queue-shell-note">공고상태, 추천여부, 출처, 검색만 남겨 빠르게 스캔하고 필요한 공고만 Notice 상세로 들어갈 수 있게 구성했습니다.</div>',
        unsafe_allow_html=True,
    )
    st.markdown('<div class="queue-filter-label">Filter / Search</div>', unsafe_allow_html=True)
    st.markdown(
        '<div class="queue-filter-help">최근 공고와 아카이브를 빠르게 좁혀보고, 필요한 공고만 상세 검토로 이어집니다.</div>',
        unsafe_allow_html=True,
    )
    source_widget_key = _notice_filter_widget_key("source")
    page_size_widget_key = _notice_filter_widget_key("page_size")
    page_index_state_key = f"{NOTICE_QUEUE_DETAIL_PAGE_KEY}_page_index"
    st.session_state.setdefault(source_widget_key, filters.get("source", []))
    st.session_state.setdefault(page_size_widget_key, int(filters.get("page_size") or 20))
    st.session_state.setdefault(page_index_state_key, int(current_route.get("page_no") or 1))

    display_col, summary_col = st.columns([5.4, 2.15], gap="large")
    with display_col:
        filter_cols = st.columns(3)
        with filter_cols[0]:
            st.multiselect(
                "추천여부",
                options=[value for value, _ in RECOMMENDATION_FILTER_OPTIONS if value != "??"],
                key=recommendation_widget_key,
                placeholder="전체",
            )
        with filter_cols[1]:
            st.multiselect(
                "공고상태",
                options=[value for value, _ in STATUS_FILTER_OPTIONS if value != "??"],
                key=status_widget_key,
                placeholder="전체",
            )
        with filter_cols[2]:
            source_options = [label for label, _ in TOP_TAB_OPTIONS if label not in {"관심공고", "보관/마감"}]
            st.multiselect(
                "출처",
                options=source_options,
                key=source_widget_key,
                placeholder="전체",
            )

        st.markdown('<div class="queue-search-label">검색</div>', unsafe_allow_html=True)
        search_col, page_size_col, reset_col = st.columns([5, 1, 1])
        with search_col:
            st.text_input(
                "search-filter",
                key=search_widget_key,
                placeholder="공고명 / 과제명 / 기관명 검색",
                label_visibility="collapsed",
            )
        with page_size_col:
            st.selectbox(
                "Page size",
                options=[20, 50, 100],
                key=page_size_widget_key,
                label_visibility="collapsed",
            )
        with reset_col:
            st.markdown('<div style="height: 1.9rem;"></div>', unsafe_allow_html=True)
            if st.button("초기화", key=f"{NOTICE_QUEUE_DETAIL_PAGE_KEY}_search_reset", use_container_width=True):
                _reset_notice_filters()
                st.rerun()

        filters = {
            "status": st.session_state.get(status_widget_key, []),
            "recommendation": st.session_state.get(recommendation_widget_key, []),
            "search": clean(st.session_state.get(search_widget_key, "")),
            "source": st.session_state.get(source_widget_key, []),
            "page_size": int(st.session_state.get(page_size_widget_key, 20) or 20),
            "dday_max": int(filters.get("dday_max") or 0),
            "include_closed": bool(filters.get("include_closed", False)),
        }
        filtered_view_rows = _apply_notice_filters(
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
            filtered_view_rows = filtered_view_rows[
                filtered_view_rows["source_key"].fillna("").astype(str).str.strip().isin(allowed_values)
            ].copy()

        page_size = int(filters["page_size"] or 20)
        total_rows = len(filtered_view_rows)
        total_pages = max(1, math.ceil(total_rows / page_size)) if page_size else 1
        current_page = int(current_route.get("page_no") or st.session_state.get(page_index_state_key, 1) or 1)
        current_page = max(1, min(current_page, total_pages))
        st.session_state[page_index_state_key] = current_page
        selected_notice_id = clean(current_route.get("item_id")) if clean(current_route.get("view")) == "summary" else ""

        start_idx = (current_page - 1) * page_size
        page_rows = filtered_view_rows.iloc[start_idx:start_idx + page_size].copy()

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
        selected_row = _get_notice_row_by_id(filtered_view_rows, selected_notice_id) if selected_notice_id else None
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
        view="summary" if clean(current_route.get("item_id")) and clean(current_route.get("view")) == "summary" else "list",
        item_id=clean(current_route.get("item_id")) if clean(current_route.get("view")) == "summary" else "",
        source_key=clean(current_route.get("source_key")) or "iris",
    )
    route_core.set_current_route(route_snapshot)
    replace_query_params(with_auth_params(route_core.serialize_route(route_snapshot)))


def _build_recent_comment_lookup() -> dict[tuple[str, str], str]:
    try:
        comments_df = load_notice_comments()
    except Exception:
        return {}
    if comments_df.empty:
        return {}
    working = filter_comments_by_scope(comments_df)
    if working.empty:
        return {}
    working["source_key"] = working["source"].fillna("").astype(str).str.strip().str.lower().apply(normalize_opportunity_source_key)
    working["notice_key"] = working["notice_id"].apply(normalize_notice_id_for_match)
    working = working.sort_values(by=["created_at_sort"], ascending=False, na_position="last")
    lookup: dict[tuple[str, str], str] = {}
    for _, row in working.iterrows():
        key = (clean(row.get("source_key")), clean(row.get("notice_key")))
        if key[0] and key[1] and key not in lookup:
            lookup[key] = clean(row.get("content")) or clean(row.get("comment"))
    return lookup


def _recent_comment_for_notice(comment_lookup: dict[tuple[str, str], str], source_key: str, notice_id: str) -> str:
    return clean(comment_lookup.get((normalize_opportunity_source_key(source_key), normalize_notice_id_for_match(notice_id)), ""))


def _build_notice_review_lookup(notice_rows: pd.DataFrame) -> dict[tuple[str, str], str]:
    if notice_rows is None or notice_rows.empty:
        return {}

    working = notice_rows.copy()
    working["source_key"] = series_from_candidates(working, ["source_key", "_source_key"]).fillna("").astype(str).str.strip().apply(normalize_opportunity_source_key)
    working["notice_key"] = series_from_candidates(working, ["Notice ID", "공고ID", "notice_id"]).fillna("").astype(str).str.strip().apply(normalize_notice_id_for_match)
    working["review_value"] = series_from_candidates(working, ["Review", "검토 여부", "검토여부", "review_status"]).fillna("").astype(str).str.strip()
    working = working[(working["source_key"] != "") & (working["notice_key"] != "")]
    if working.empty:
        return {}

    lookup: dict[tuple[str, str], str] = {}
    for _, row in working.iterrows():
        lookup[(clean(row.get("source_key")), clean(row.get("notice_key")))] = clean(row.get("review_value"))
    return lookup


def _build_favorites_workspace_entries(
    opportunity_rows: pd.DataFrame,
    notice_rows: pd.DataFrame,
) -> pd.DataFrame:
    del opportunity_rows
    comment_lookup = _build_recent_comment_lookup()
    notice_review_lookup = _build_notice_review_lookup(notice_rows)
    favorite_notice_keys = {
        key
        for key, value in notice_review_lookup.items()
        if clean(value) == FAVORITE_REVIEW_STATUS
    }
    rows: list[dict[str, object]] = []
    for _, row in notice_rows.iterrows():
        source_key = _workspace_source_key(row)
        notice_id = _workspace_notice_id(row)
        favorite_key = (normalize_opportunity_source_key(source_key), normalize_notice_id_for_match(notice_id))
        if favorite_key not in favorite_notice_keys:
            continue
        rows.append(
            {
                "Type": "Notice",
                "Source": clean(row.get("Source")) or "-",
                "Title": clean(row.get("Title")) or "-",
                "Subtitle": clean(row.get("Agency")) or "-",
                "Status": normalize_notice_status_label(row.get("Status")) or clean(row.get("Status")) or "-",
                "Review": FAVORITE_REVIEW_STATUS,
                "D-Day": clean(row.get("D-Day")) or "-",
                "Recommendation": clean(row.get("Recommendation")) or "-",
                "Score": row.get("Score"),
                "Budget": clean(row.get("Budget")) or "-",
                "Recent Memo": truncate_text(_recent_comment_for_notice(comment_lookup, source_key, notice_id), max_chars=72) or "-",
                "Agency": clean(row.get("Agency")) or "-",
                "Ministry": clean(row.get("Ministry")) or "-",
                "Period": clean(row.get("Period")) or "-",
                "Summary": truncate_text(clean(row.get("Summary")), max_chars=150) or "-",
                "RFP Count": clean(row.get("RFP Count")) or "",
                "Keywords": ", ".join(_extract_dashboard_keywords(row, limit=3)),
                "_sort_date": row.get("_sort_date"),
                "_selection_type": "notice",
                "_selection_id": notice_id,
                "_source_key": source_key,
                "_notice_id": notice_id,
            }
        )
    if not rows:
        return pd.DataFrame()
    return pd.DataFrame(rows)


def _render_favorites_table(rows: pd.DataFrame, *, key_prefix: str) -> None:
    if rows.empty:
        st.info("저장된 관심 항목이 없습니다.")
        return
    del key_prefix

    def _deadline_sort_value(value: object) -> int | None:
        text = clean(value)
        if not text or text in {"-", "예정", "미정", "마감"}:
            return None
        if text == "D-Day":
            return 0
        match = re.fullmatch(r"D-(\d+)", text)
        if not match:
            return None
        return int(match.group(1))

    def _selection_href(row: pd.Series) -> str:
        if clean(row.get("_selection_type")) == "rfp":
            return build_route_href("rfp_queue", clean(row.get("_selection_id")), source_key="iris")
        return build_route_href("notice_queue", clean(row.get("_selection_id")), source_key="notices")

    def _status_class(value: object) -> str:
        text = normalize_notice_status_label(value) or clean(value)
        if text == "접수중":
            return "is-open"
        if text == "예정":
            return "is-pending"
        if "임박" in text:
            return "is-deadline"
        if text == "마감":
            return "is-closed"
        if text == FAVORITE_REVIEW_STATUS:
            return "is-open"
        return "is-pending"

    row_html: list[str] = []
    for _, row in rows.iterrows():
        is_rfp = clean(row.get("Type")) == "RFP"
        row_class = ""
        status_value = clean(row.get("Status")) or FAVORITE_REVIEW_STATUS
        dday_label = clean(row.get("D-Day")) or "-"
        dday_sort = _deadline_sort_value(dday_label)
        tone_class = "is-calm"
        if dday_sort is not None and dday_sort <= 3:
            tone_class = "is-critical"
        elif dday_sort is not None and dday_sort <= 14:
            tone_class = "is-warning"
        favorite_href = build_favorite_toggle_href(
            page_key="favorites",
            notice_id=clean(row.get("_notice_id")),
            current_value=clean(row.get("Review")) or FAVORITE_REVIEW_STATUS,
            source_key=clean(row.get("_source_key")) or "iris",
        )
        favorite_label = "해제" if clean(row.get("Review")) == FAVORITE_REVIEW_STATUS else "등록"
        favorite_class = " is-active" if clean(row.get("Review")) == FAVORITE_REVIEW_STATUS else ""
        type_badge = "RFP" if is_rfp else "공고"
        agency_value = clean(row.get("Agency")) or clean(row.get("Subtitle")) or "미정"
        period_value = clean(row.get("Period")) or "미정"
        rfp_count = clean(row.get("RFP Count")) or "-"
        status_label = normalize_notice_status_label(status_value) or status_value or "미정"
        keyword_values = [clean(item) for item in clean(row.get("Keywords")).split(",") if clean(item)]
        keyword_chips = "".join(
            f'<span class="queue-grid-chip">{escape(keyword)}</span>'
            for keyword in keyword_values[:3]
        )
        if not keyword_chips:
            keyword_chips = '<span class="queue-grid-chip is-empty">키워드 없음</span>'
        keyword_class = "queue-grid-keywords is-single" if len(keyword_values) <= 1 else "queue-grid-keywords"
        keywords_html = f'<div class="{keyword_class}">{keyword_chips}</div>'
        row_html.append(
            "".join(
                [
                    f'<div class="favorites-grid-row{row_class}">',
                    f'<div class="queue-grid-cell"><span class="favorites-grid-kind">{escape(type_badge)}</span></div>',
                    f'<div class="queue-grid-cell"><span class="notice-mailbox-status {_status_class(status_label)}">{escape(status_label)}</span></div>',
                    '<div class="queue-grid-cell is-title">',
                    f'<a class="queue-grid-title" href="{escape(_selection_href(row), quote=True)}" target="_self">{escape(truncate_text(row.get("Title"), max_chars=108))}</a>',
                    f'<div class="queue-grid-subtitle">{escape(truncate_text(clean(row.get("Source")) or "-", max_chars=18))} / {escape(truncate_text(clean(row.get("Subtitle")) or "-", max_chars=58))}</div>',
                    '</div>',
                    f'<div class="queue-grid-cell is-keywords">{keywords_html}</div>',
                    f'<div class="queue-grid-cell">{escape(truncate_text(agency_value, max_chars=24))}</div>',
                    f'<div class="queue-grid-cell queue-grid-period">{escape(truncate_text(period_value, max_chars=30))}</div>',
                    f'<div class="queue-grid-cell"><span class="queue-grid-dday {tone_class}">{escape(dday_label)}</span></div>',
                    f'<div class="queue-grid-cell queue-grid-rfp-count is-center">{escape(rfp_count)}</div>',
                    f'<div class="queue-grid-cell is-center"><a class="queue-grid-favorite{favorite_class}" href="{escape(favorite_href, quote=True)}" target="_self">{favorite_label}</a></div>',
                    '</div>',
                ]
            )
        )

    st.markdown(
        (
            '<div class="favorites-grid-scroll"><div class="favorites-grid">'
            '<div class="favorites-grid-head">'
            '<div>구분</div><div>상태</div><div>공고 / 과제명</div><div>키워드</div><div>기관</div><div>기간</div><div>D-day</div><div>RFP 수</div><div>관심</div>'
            '</div>'
            + "".join(row_html)
            + '</div></div>'
        ),
        unsafe_allow_html=True,
    )


def render_favorite_notice_page(
    notice_view_df: pd.DataFrame,
    opportunity_df: pd.DataFrame,
    source_datasets: dict[str, object] | None = None,
) -> None:
    render_notice_queue_ui_styles()

    @st.fragment
    def _render_favorites_workspace_fragment() -> None:
        datasets = {
            "notice_current": filter_current_notice_rows(notice_view_df),
            "pending": notice_view_df[build_notice_status_scope_mask(notice_view_df, "예정")].copy() if not notice_view_df.empty else pd.DataFrame(),
            "notice_archive": filter_archived_notice_rows(notice_view_df),
            "opportunity": opportunity_df,
        }
        opportunity_rows = build_dashboard_opportunity_index({"opportunity": opportunity_df}, source_datasets)
        notice_rows = _build_workspace_notice_rows(datasets, source_datasets, opportunity_rows)
        favorite_entries = _build_favorites_workspace_entries(opportunity_rows, notice_rows)
        if favorite_entries.empty:
            st.info("저장된 관심 항목이 없습니다.")
            return
        type_options = ["All"] + [
            option
            for option in ["Notice", "RFP"]
            if favorite_entries["Type"].eq(option).any()
        ]

        with st.container(key="favorites_toolbar_shell"):
            toolbar_cols = st.columns([1.05, 1.15, 3.92, 0.48], gap="small")
            with toolbar_cols[0]:
                type_filter = st.selectbox(
                    "타입",
                    options=type_options,
                    index=0,
                    key="favorites_workspace_type",
                    label_visibility="collapsed",
                )
            with toolbar_cols[1]:
                sort_option = st.selectbox(
                    "정렬",
                    options=["마감 임박순", "최신 등록일순", "추천도순"],
                    index=0,
                    key="favorites_workspace_sort",
                    label_visibility="collapsed",
                )
            with toolbar_cols[2]:
                search_text = st.text_input(
                    "검색",
                    key="favorites_workspace_search",
                    placeholder="공고명 / 기관명 검색",
                    label_visibility="collapsed",
                )
            with toolbar_cols[3]:
                st.button(
                    "초기화",
                    key="favorites_workspace_reset",
                    use_container_width=True,
                    on_click=_reset_favorites_workspace_filters,
                )
        filtered_entries = favorite_entries.copy()
        if type_filter != "All":
            filtered_entries = filtered_entries[filtered_entries["Type"].eq(type_filter)]
        if clean(search_text):
            filtered_entries = filtered_entries[
                build_contains_mask(filtered_entries, ["Title", "Subtitle", "Agency", "Keywords"], search_text)
            ]
        filtered_entries = _sort_favorites_workspace_entries(filtered_entries, clean(sort_option) or "마감 임박순")
        st.markdown(f'<div class="notice-queue-toolbar-meta">저장된 관심 항목 {len(filtered_entries):,}건</div>', unsafe_allow_html=True)
        _render_favorites_table(filtered_entries, key_prefix="favorites_workspace")

    _render_favorites_workspace_fragment()

def render_notice_queue_page(datasets: dict[str, pd.DataFrame], source_datasets: dict[str, object] | None) -> None:
    opportunity_rows = build_dashboard_opportunity_index(datasets, source_datasets)
    source_df = build_crawled_notice_collection(datasets, source_datasets)
    notice_rows = _normalize_workspace_notice_rows(source_df, opportunity_rows)
    if not notice_rows.empty:
        rfp_count_lookup = _build_notice_rfp_count_lookup(opportunity_rows)
        notice_rows["RFP Count"] = notice_rows.apply(
            lambda row: rfp_count_lookup.get(
                (clean(row.get("source_key")) or "iris", clean(row.get("Notice ID"))),
                int(row.get("RFP Count") or 0),
            ),
            axis=1,
        )
    page_state_key = "notice_queue_workspace_page_index"
    page_size = 10
    filter_reset_keys = [
        "notice_queue_workspace_search",
        "notice_queue_workspace_status_inline",
        "notice_queue_workspace_recommendation",
        "notice_queue_workspace_sort",
    ]

    @st.fragment
    def _render_notice_queue_workspace_fragment() -> None:
        render_notice_queue_ui_styles()
        with st.container(key="notice_queue_toolbar_shell"):
            toolbar_cols = st.columns([4.7, 1.05, 1.05, 1.0, 0.6], gap="small")
            with toolbar_cols[0]:
                search_text = st.text_input(
                    "Notice Search",
                    key="notice_queue_workspace_search",
                    placeholder="공고명 / 기관명 / 공고번호 / 요약 검색",
                    label_visibility="collapsed",
                )
            status_options = sorted({
                normalize_notice_status_label(value) or clean(value)
                for value in notice_rows["Status"].tolist()
                if clean(value)
            }) if not notice_rows.empty else []
            with toolbar_cols[1]:
                selected_status = st.multiselect(
                    "공고상태",
                    options=status_options,
                    key="notice_queue_workspace_status_inline",
                    placeholder="공고상태",
                    label_visibility="collapsed",
                )
            recommendation_options = sorted({
                clean(value)
                for value in notice_rows["Recommendation"].tolist()
                if clean(value) and clean(value) != "-"
            }) if not notice_rows.empty else []
            with toolbar_cols[2]:
                selected_recommendations = st.multiselect(
                    "추천",
                    options=recommendation_options,
                    key="notice_queue_workspace_recommendation",
                    placeholder="추천",
                    label_visibility="collapsed",
                )
            with toolbar_cols[3]:
                sort_option = st.selectbox(
                    "정렬",
                    options=["최신 등록일순", "마감 임박순", "추천도순"],
                    key="notice_queue_workspace_sort",
                    label_visibility="collapsed",
                )
            with toolbar_cols[4]:
                st.button(
                    "초기화",
                    key="notice_queue_reset_top",
                    use_container_width=True,
                    on_click=_reset_notice_queue_filters,
                    kwargs={
                        "page_state_key": page_state_key,
                        "filter_reset_keys": filter_reset_keys,
                    },
                )

        current_signature = _notice_queue_filter_signature(
            search_text=search_text,
            selected_status=selected_status,
            selected_recommendations=selected_recommendations,
            sort_option=sort_option,
        )
        previous_signature = st.session_state.get("notice_queue_workspace_filter_signature")
        if previous_signature != current_signature:
            st.session_state[page_state_key] = 1
            st.session_state["notice_queue_workspace_filter_signature"] = current_signature

        filtered = notice_rows.copy()
        if not selected_status:
            filtered = filtered[
                filtered["Status"].apply(lambda value: (normalize_notice_status_label(value) or clean(value)) != "마감")
            ]
        if clean(search_text):
            filtered = filtered[
                build_contains_mask(filtered, ["Title", "Agency", "Ministry", "Notice No", "Notice ID", "Summary"], search_text)
            ]
        if selected_status:
            filtered = filtered[
                filtered["Status"].apply(lambda value: normalize_notice_status_label(value) or clean(value)).isin(selected_status)
            ]
        if selected_recommendations:
            filtered = filtered[filtered["Recommendation"].isin(selected_recommendations)]

        if sort_option == "마감 임박순":
            filtered = filtered.assign(
                _queue_deadline_sort=filtered["D-Day"].apply(
                    lambda value: 0
                    if clean(value) == "D-Day"
                    else (int(re.fullmatch(r"D-(\d+)", clean(value)).group(1)) if re.fullmatch(r"D-(\d+)", clean(value)) else 9999)
                )
            ).sort_values(by=["_queue_deadline_sort", "_sort_date", "Title"], ascending=[True, False, True], na_position="last")
        elif sort_option == "추천도순":
            filtered = filtered.assign(
                _queue_score_sort=to_numeric_column(filtered["Score"])
            ).sort_values(by=["_queue_score_sort", "_sort_date", "Title"], ascending=[False, False, True], na_position="last")
        else:
            filtered = filtered.sort_values(by=["_sort_date", "Title"], ascending=[False, True], na_position="last")

        total_rows = len(filtered)
        total_pages = max(1, math.ceil(total_rows / page_size)) if page_size else 1
        query_page_no = safe_int(get_query_param("page_no"), 0)
        query_signature = _notice_queue_query_signature()
        if query_page_no and current_signature == query_signature:
            current_page = query_page_no
        else:
            current_page = int(st.session_state.get(page_state_key, 1) or 1)
        current_page = max(1, min(current_page, total_pages))
        st.session_state[page_state_key] = current_page
        start_idx = (current_page - 1) * page_size
        page_rows = filtered.iloc[start_idx:start_idx + page_size].copy()

        st.markdown(
            f'<div class="notice-queue-toolbar-meta">검색 결과 {total_rows:,}건</div>',
            unsafe_allow_html=True,
        )
        _render_notice_queue_table(page_rows, key_prefix="notice_queue_workspace")

        pagination_html = _render_notice_queue_pagination_controls(
            page_state_key=page_state_key,
            control_key_prefix="notice_queue_workspace_bottom",
            current_page=current_page,
            total_pages=total_pages,
            total_rows=total_rows,
            page_size=page_size,
        )
        st.markdown(
            '<div class="notice-queue-footer">'
            f'<div class="notice-queue-page-slot">{pagination_html}</div>'
            '</div>',
            unsafe_allow_html=True,
        )

    _render_notice_queue_workspace_fragment()

def render_notices_source(
    source_config,
    mode_config,
    datasets: dict[str, pd.DataFrame],
    source_datasets: dict[str, object] | None,
    *,
    show_internal_tabs: bool = True,
) -> None:
    del source_config, mode_config, show_internal_tabs
    render_notice_queue_page(datasets, source_datasets)

def resolve_route_source_key_for_row(row: dict | pd.Series | None, source_key: str | None = None) -> str:
    candidate_values = [source_key]
    if row is not None:
        row_dict = normalize_row_dict(row)
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
        normalized = normalize_opportunity_source_key(candidate)
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
        st.info("표시할 데이터가 없습니다.")
        return

    column_widths = {
        "공고일자": "92px",
        "공고번호": "132px",
        "전문기관": "126px",
        "전문기관명": "126px",
        "공고명": "280px",
        "notice_title": "280px",
        "해당 과제명": "280px",
        "project_name": "280px",
        "공고상태": "92px",
        "접수기간": "156px",
        "추천여부": "84px",
        "추천도": "84px",
        "추천도 및 점수": "96px",
        "점수": "84px",
        "예산": "122px",
        "budget": "122px",
        "검토 여부": "84px",
        "검토여부": "84px",
        "상세링크": "76px",
        "detail_link": "76px",
    }
    compact_limits = {
        "공고명": 56,
        "notice_title": 56,
        "해당 과제명": 52,
        "project_name": 52,
        "접수기간": 26,
        "예산": 24,
        "budget": 24,
    }

    internal_link_columns = {"공고명", "notice_title", "해당 과제명", "연결 과제명", "project_name"}

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
            if column in {"상세링크", "detail_link"}:
                raw_link = clean(row.get(column))
                if raw_link:
                    cell_html.append(
                        '<td class="list-link-cell"{style}><a class="list-link-out" href="{href}" title="{title}" target="_blank" rel="noopener noreferrer">원문</a></td>'.format(
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
    generic_titles = {"", "사업명", "과제명", "rfp", "rfp제목", "사업명rfp명과제수"}
    if normalized in generic_titles:
        return True
    if re.search(r"\.(pdf|hwpx|hwp|zip|docx?)$", lowered, flags=re.IGNORECASE):
        return True
    if file_normalized and normalized == file_normalized:
        return True
    if notice_normalized and normalized == notice_normalized:
        return True
    if "붙임" in text and re.search(r"\.(pdf|hwpx|hwp|zip|docx?)", lowered, flags=re.IGNORECASE):
        return True
    return False


def choose_display_project_title(row: dict) -> str:
    notice_title = first_non_empty(row, "Notice Title", "notice_title", "공고명")
    file_name = first_non_empty(row, "file_name", "File Name", "파일명")
    candidates = [
        "llm_project_name",
        "project_name",
        "Project",
        "해당 과제명",
        "과제명",
        "llm_rfp_title",
        "rfp_title",
        "RFP 제목",
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
        return "마감"
    return clean(fallback) or "-"


def resolve_local_file_path(row: dict | pd.Series | None) -> Path | None:
    row_dict = normalize_row_dict(row)
    if not row_dict:
        return None

    for key in ["file_path", "파일경로"]:
        candidate = clean(row_dict.get(key))
        if not candidate:
            continue
        path = Path(candidate)
        if not path.is_absolute():
            path = (PROJECT_ROOT / candidate).resolve()
        if path.exists() and path.is_file():
            return path
    return None


def ensure_notice_analysis_fallback(row: dict, top_related: dict) -> dict:
    merged = normalize_row_dict(row)
    if not top_related:
        return merged

    fallback_map = {
        "대표과제명": ["llm_project_name", "project_name", "rfp_title"],
        "대표추천도": ["llm_recommendation", "recommendation"],
        "대표점수": ["llm_fit_score", "rfp_score"],
        "대표예산": ["llm_total_budget_text", "total_budget_text", "llm_per_project_budget_text", "per_project_budget_text", "budget"],
        "대표추천이유": ["llm_reason", "reason"],
        "대표키워드": ["llm_keywords", "keywords"],
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
    notice_id = clean(row.get("공고ID"))
    if notice_id and "notice_id" in working.columns:
        notice_key = normalize_notice_id_for_match(notice_id)
        matched = working[
            working["notice_id"].apply(normalize_notice_id_for_match).eq(notice_key)
        ].copy()
        if not matched.empty:
            return matched

    notice_title = clean(row.get("공고명"))
    if notice_title and "공고명" in working.columns:
        matched = working[working["공고명"].fillna("").astype(str).str.strip().eq(notice_title)].copy()
        if not matched.empty:
            return matched
    if notice_title and "notice_title" in working.columns:
        matched = working[working["notice_title"].fillna("").astype(str).str.strip().eq(notice_title)].copy()
        if not matched.empty:
            return matched

    ancm_no = clean(row.get("공고번호"))
    if ancm_no and "공고번호" in working.columns:
        matched = working[working["공고번호"].fillna("").astype(str).str.strip().eq(ancm_no)].copy()
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
        st.info("공개 배포에서는 검토 여부 수정이 비활성화되어 있습니다.")
        return

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
                if is_user_scoped_operations_enabled():
                    upsert_user_review_status(
                        user_id=get_current_operation_scope_key(),
                        source_key=source_key,
                        notice_id=notice_id,
                        notice_title=notice_title,
                        review_status=review_value,
                    )
                else:
                    update_notice_review_status(
                        notice_id,
                        review_value,
                        source_key=source_key,
                    )
                st.success("검토 여부를 저장했습니다.")
                st.rerun()
            except Exception as exc:
                st.error(f"저장 실패: {exc}")


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
    else:
        update_notice_review_status(
            notice_id,
            review_status,
            source_key=source_key,
        )


def _inject_detail_comment_styles() -> None:
    if st.session_state.get("_detail_comment_styles_injected"):
        return
    st.session_state["_detail_comment_styles_injected"] = True
    st.markdown(
        """
        <style>
        [class*="st-key-detail_comments_panel_"] {
          border: 1px solid var(--border);
          border-radius: 20px;
          background: var(--surface);
          overflow: visible;
        }
        .detail-comments-header {
          display: flex;
          align-items: flex-start;
          justify-content: space-between;
          gap: 16px;
          padding: 20px 28px;
          border-bottom: 1px solid var(--border);
          background: linear-gradient(180deg, rgba(59, 130, 246, 0.10) 0%, rgba(59, 130, 246, 0.05) 100%);
        }
        .detail-comments-header-copy {
          min-width: 0;
          display: flex;
          flex-direction: column;
          gap: 4px;
        }
        .detail-comments-subtitle {
          color: var(--text-subtle);
          font-size: 0.84rem;
          font-weight: 700;
          line-height: 1.55;
        }
        .detail-comments-count {
          display: inline-flex;
          align-items: center;
          min-height: 32px;
          padding: 0 12px;
          border-radius: 999px;
          border: 1px solid rgba(59, 130, 246, 0.14);
          background: rgba(255, 255, 255, 0.92);
          color: var(--text-strong);
          font-size: 0.84rem;
          font-weight: 800;
          white-space: nowrap;
        }
        [class*="st-key-detail_comments_body_"] [data-testid="stVerticalBlock"] {
          gap: 0.85rem !important;
          padding: 18px 24px 20px;
        }
        [class*="st-key-detail_comments_form_"] [data-testid="stForm"],
        [class*="st-key-detail_comments_form_"] [data-testid="stForm"] > div {
          border: 0 !important;
          background: transparent !important;
          box-shadow: none !important;
          padding: 0 !important;
          margin: 0 !important;
        }
        .detail-comments-author {
          color: var(--text-body);
          font-size: 0.98rem;
          font-weight: 500;
          line-height: 1.72;
          margin: 0;
        }
        .detail-comments-history-title {
          color: var(--text-subtle);
          font-size: 0.84rem;
          font-weight: 800;
          letter-spacing: 0.01em;
          line-height: 1.55;
          margin: 0.2rem 0 0;
        }
        .detail-comments-entry-meta {
          display: flex;
          align-items: center;
          flex-wrap: wrap;
          gap: 0.45rem;
          color: var(--text-subtle);
          font-size: 0.84rem;
          font-weight: 700;
          line-height: 1.55;
          margin: 0;
        }
        .detail-comments-entry-date::after {
          content: "•";
          margin-left: 0.45rem;
          color: var(--border-strong);
        }
        .detail-comments-entry-author {
          color: var(--text-strong);
          font-weight: 800;
        }
        .detail-comments-entry-text {
          margin-top: 0.15rem;
          color: var(--text-body);
          font-size: 0.98rem;
          font-weight: 400;
          line-height: 1.72;
          word-break: break-word;
          white-space: normal;
        }
        .detail-comments-empty {
          margin: 0;
          color: var(--text-subtle);
          font-size: 0.96rem;
          font-weight: 500;
          line-height: 1.72;
        }
        .detail-comments-divider {
          height: 1px;
          margin: 14px 0 0;
          background: var(--border);
        }
        [class*="st-key-detail_comments_history_"] [data-testid="stVerticalBlock"] {
          gap: 0.75rem !important;
        }
        [class*="st-key-detail_comment_entry_"] {
          border: 1px solid var(--border);
          border-radius: 16px;
          background: linear-gradient(180deg, rgba(248, 250, 252, 0.94) 0%, rgba(255, 255, 255, 1) 100%);
          box-shadow: 0 10px 22px rgba(15, 23, 42, 0.04);
          padding: 16px 18px !important;
        }
        [class*="st-key-detail_comment_entry_"] [data-testid="stVerticalBlock"] {
          gap: 0.7rem !important;
          padding: 0 !important;
        }
        [class*="st-key-detail_comments_form_"] [data-testid="stTextArea"] {
          margin-top: 0.35rem !important;
        }
        [class*="st-key-detail_comments_form_"] .stTextArea textarea {
          min-height: 132px;
          padding: 0.95rem 1rem !important;
          border-radius: 16px !important;
          border: 1px solid var(--border) !important;
          background: var(--surface) !important;
          box-shadow: none !important;
          color: var(--text-body) !important;
          font-size: 0.98rem !important;
          font-weight: 400 !important;
          line-height: 1.72 !important;
        }
        [class*="st-key-detail_comments_form_"] .stTextArea textarea::placeholder {
          color: var(--text-subtle) !important;
          opacity: 1 !important;
        }
        [class*="st-key-detail_comments_form_"] .stTextArea textarea:focus {
          border-color: var(--border-strong) !important;
          box-shadow: 0 0 0 3px rgba(59, 130, 246, 0.08) !important;
        }
        [class*="st-key-detail_comments_form_"] .stButton > button {
          border-radius: 14px !important;
          white-space: nowrap !important;
          min-height: 42px !important;
          min-width: 132px !important;
          font-size: 0.96rem !important;
          font-weight: 700 !important;
        }
        [class*="st-key-detail_comments_form_"] .stFormSubmitButton {
          display: flex;
          justify-content: flex-end;
        }
        [class*="st-key-detail_comment_entry_"] [data-testid="stHorizontalBlock"] {
          align-items: start !important;
          gap: 0.75rem !important;
          justify-content: space-between !important;
        }
        [class*="st-key-detail_comment_entry_"] .stButton {
          display: flex;
          justify-content: flex-end;
        }
        [class*="st-key-detail_comment_entry_"] .stButton > button {
          min-height: 34px !important;
          padding: 0 0.75rem !important;
          border-radius: 999px !important;
          font-size: 0.84rem !important;
        }
        @media (max-width: 900px) {
          .detail-comments-header {
            flex-direction: column;
            align-items: flex-start;
            padding: 18px 20px;
          }
          [class*="st-key-detail_comments_body_"] [data-testid="stVerticalBlock"] {
            padding: 16px 20px 18px;
          }
          [class*="st-key-detail_comments_form_"] .stFormSubmitButton > button {
            width: 100% !important;
          }
        }
        </style>
        """,
        unsafe_allow_html=True,
    )


def _push_ui_flash(scope_key: str, kind: str, message: str) -> None:
    st.session_state[f"_ui_flash::{clean(scope_key)}"] = {
        "kind": clean(kind) or "info",
        "message": clean(message),
    }


def _render_ui_flash(scope_key: str, *, presentation: str = "inline") -> None:
    flash = st.session_state.pop(f"_ui_flash::{clean(scope_key)}", None)
    if not isinstance(flash, dict):
        return
    message = clean(flash.get("message"))
    if not message:
        return
    kind = clean(flash.get("kind")).lower()
    if presentation == "toast" and hasattr(st, "toast"):
        icon = "❌" if kind == "error" else ("⚠️" if kind == "warning" else "✅")
        st.toast(message, icon=icon)
        return
    if kind == "error":
        st.error(message)
    elif kind == "warning":
        st.warning(message)
    else:
        st.success(message)


def _delete_notice_comment_with_flash(*, comment_id: str, current_user_id: str, flash_scope: str) -> None:
    try:
        delete_notice_comment(comment_id, current_user_id)
        _push_ui_flash(flash_scope, "success", "댓글을 삭제했습니다.")
    except Exception as exc:
        _push_ui_flash(flash_scope, "error", f"댓글 삭제 실패: {exc}")




def render_notice_comments(
    row: dict,
    section_key: str,
    *,
    show_title: bool = True,
    modern_layout: bool = False,
) -> None:
    if modern_layout:
        _inject_detail_comment_styles()

    notice_id = clean(first_non_empty(row, *NOTICE_ID_CANDIDATES))
    notice_title = clean(first_non_empty(row, *NOTICE_TITLE_CANDIDATES))
    source_key = resolve_notice_source_key(row)
    current_user_id = clean(get_current_user_id())
    author_id = current_user_id or clean(get_env("DEFAULT_COMMENT_AUTHOR")) or "익명"
    flash_scope = f"comments::{section_key}"
    save_feedback = ""

    if not notice_id:
        if modern_layout:
            with st.container(key=f"detail_comments_panel_{section_key}"):
                st.markdown(
                    (
                        '<div class="detail-comments-header">'
                        '<div class="detail-comments-header-copy">'
                        '<div class="detail-fact-title">댓글</div>'
                        '<div class="detail-comments-subtitle">작성자는 로그인 ID로 자동 기록됩니다.</div>'
                        "</div>"
                        '<div class="detail-comments-count">댓글 0건</div>'
                        "</div>"
                    ),
                    unsafe_allow_html=True,
                )
                with st.container(key=f"detail_comments_body_{section_key}"):
                    st.markdown('<div class="detail-comments-empty">공고ID가 없어 댓글을 연결할 수 없습니다.</div>', unsafe_allow_html=True)
        else:
            st.caption("공고ID가 없어 댓글을 연결할 수 없습니다.")
        return

    def _load_matched_comments() -> pd.DataFrame:
        try:
            comments_df = load_notice_comments()
        except Exception as exc:
            st.warning(f"댓글 이력을 불러오지 못했습니다: {exc}")
            return _empty_comment_dataframe()
        return filter_notice_comments(comments_df, source_key=source_key, notice_id=notice_id)

    if modern_layout:
        @st.fragment
        def _render_comments_fragment() -> None:
            matched = _load_matched_comments()
            save_feedback = ""
            with st.container(key=f"detail_comments_panel_{section_key}"):
                st.markdown(
                    (
                        '<div class="detail-comments-header">'
                        '<div class="detail-comments-header-copy">'
                        '<div class="detail-fact-title">댓글</div>'
                        '<div class="detail-comments-subtitle">작성자는 로그인 ID로 자동 기록됩니다.</div>'
                        "</div>"
                        f'<div class="detail-comments-count">댓글 {len(matched)}건</div>'
                        "</div>"
                    ),
                    unsafe_allow_html=True,
                )
                _render_ui_flash(flash_scope, presentation="inline")

                with st.container(key=f"detail_comments_body_{section_key}"):
                    with st.form(f"{section_key}_comment_form", clear_on_submit=True, border=False):
                        st.markdown(
                            f'<div class="detail-comments-author">작성자 {escape(author_id)}</div>',
                            unsafe_allow_html=True,
                        )
                        comment_text = st.text_area(
                            "의견",
                            key=f"{section_key}_comment_text",
                            height=150,
                            placeholder="이 공고에 대한 메모나 검토 의견을 남겨주세요.",
                            label_visibility="collapsed",
                        )
                        submitted = st.form_submit_button("댓글 저장", use_container_width=True)

                    if submitted:
                        try:
                            append_notice_comment(
                                source_key=source_key,
                                notice_id=notice_id,
                                notice_title=notice_title,
                                author=author_id,
                                comment=comment_text,
                            )
                            save_feedback = "댓글을 저장했습니다."
                            _push_ui_flash(flash_scope, "success", "댓글을 저장했습니다.")
                            matched = _load_matched_comments()
                        except Exception as exc:
                            st.error(f"댓글 저장 실패: {exc}")

                    with st.container(key=f"detail_comments_history_{section_key}"):
                        if save_feedback:
                            st.success(save_feedback)
                        st.markdown('<div class="detail-comments-history-title">댓글 이력</div>', unsafe_allow_html=True)
                        if matched.empty:
                            st.markdown('<div class="detail-comments-empty">아직 등록된 댓글이 없습니다.</div>', unsafe_allow_html=True)
                        else:
                            comment_rows = matched.to_dict("records")
                            for idx, comment_row in enumerate(comment_rows):
                                comment_id = clean(comment_row.get("comment_id"))
                                created_at = clean(comment_row.get("created_at"))
                                nickname = clean(comment_row.get("nickname")) or clean(comment_row.get("author")) or "익명"
                                content = clean(comment_row.get("content")) or clean(comment_row.get("comment"))
                                allow_delete = bool(comment_id and can_delete_comment(comment_row, current_user_id))
                                meta_parts = []
                                if created_at:
                                    meta_parts.append(
                                        f'<span class="detail-comments-entry-date">{escape(created_at)}</span>'
                                    )
                                if nickname:
                                    meta_parts.append(
                                        f'<span class="detail-comments-entry-author">{escape(nickname)}</span>'
                                    )

                                with st.container(key=f"detail_comment_entry_{section_key}_{comment_id or idx}"):
                                    meta_col, action_col = st.columns([4.2, 0.8], gap="small")
                                    with meta_col:
                                        st.markdown(
                                            f'<div class="detail-comments-entry-meta">{"".join(meta_parts)}</div>',
                                            unsafe_allow_html=True,
                                        )
                                    with action_col:
                                        if allow_delete:
                                            st.button(
                                                "삭제",
                                                key=f"{section_key}_delete_comment_{comment_id}",
                                                use_container_width=False,
                                                on_click=_delete_notice_comment_with_flash,
                                                kwargs={
                                                    "comment_id": comment_id,
                                                    "current_user_id": current_user_id,
                                                    "flash_scope": flash_scope,
                                                },
                                            )
                                    st.markdown(
                                        f'<div class="detail-comments-entry-text">{escape(content).replace(chr(10), "<br>")}</div>',
                                        unsafe_allow_html=True,
                                    )

        _render_comments_fragment()
        return

    with st.form(f"{section_key}_comment_form", clear_on_submit=True):
        comment = st.text_area("의견", key=f"{section_key}_comment_text", height=110)
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

    matched = _load_matched_comments()
    if save_feedback:
        st.success(save_feedback)
    if show_title:
        st.markdown('<div class="detail-section-title">댓글</div>', unsafe_allow_html=True)
    if matched.empty:
        st.info("아직 등록된 댓글이 없습니다.")
        return

    st.caption(f"댓글 이력 {len(matched)}건")
    for comment_row in matched.to_dict("records"):
        comment_id = clean(comment_row.get("comment_id"))
        created_at = clean(comment_row.get("created_at"))
        author = clean(comment_row.get("nickname")) or clean(comment_row.get("author")) or "익명"
        comment_text = clean(comment_row.get("content")) or clean(comment_row.get("comment"))
        allow_delete = bool(comment_id and can_delete_comment(comment_row, current_user_id))
        with st.container(border=True):
            st.caption(" · ".join([value for value in [created_at, author] if value]))
            st.write(comment_text)
            if allow_delete:
                st.button(
                    "댓글 삭제",
                    key=f"{section_key}_delete_comment_{comment_id}",
                    on_click=_delete_notice_comment_with_flash,
                    kwargs={
                        "comment_id": comment_id,
                        "current_user_id": current_user_id,
                        "flash_scope": flash_scope,
                    },
                )
    _render_ui_flash(flash_scope, presentation="toast")


def _legacy_render_notice_detail_from_row(row: dict, opportunity_df: pd.DataFrame) -> None:
    row = normalize_row_dict(row)
    if not row:
        st.info("표시할 공고가 없습니다.")
        return

    current_source = get_query_param("source") or "iris"
    source_key = resolve_notice_source_key(row)
    is_mss = source_key == "tipa" or current_source == "tipa"
    is_nipa = source_key == "nipa" or current_source == "nipa"
    if is_mss:
        detail_kicker = "중소기업벤처부 Notice Detail"
        detail_button_label = "중소기업벤처부 상세 바로가기"
        review_caption = "공고 검토 상태를 바꾸면 중소기업벤처부 시트에 즉시 반영됩니다."
    elif is_nipa:
        detail_kicker = "NIPA Notice Detail"
        detail_button_label = "NIPA 상세 바로가기"
        review_caption = "공고 검토 상태를 바꾸면 NIPA 시트에 즉시 반영됩니다."
    else:
        detail_kicker = "Notice Master Detail"
        detail_button_label = "IRIS 상세 바로가기"
        review_caption = "공고 검토 상태를 바꾸면 IRIS_NOTICE_MASTER에 즉시 반영됩니다."
    if is_user_scoped_operations_enabled():
        review_caption = "검토 상태는 로그인한 사용자 전용 운영관리 데이터로 저장됩니다."

    related = find_related_opportunities_for_notice(row, opportunity_df)
    top_related = {}
    if not related.empty:
        related = related.sort_values(
            by=["rfp_score", "project_name"],
            ascending=[False, True],
            na_position="last",
        )
        top_related = related.iloc[0].to_dict()
        row = ensure_notice_analysis_fallback(row, top_related)

    render_public_notice_card(row, top_related=top_related, kind="notice")

    top_left, top_right = st.columns([2, 1])
    with top_left:
        render_detail_card(
            "공고/과제 핵심 정보",
            [
                ("공고명", row.get("공고명")),
                ("공고번호", row.get("공고번호")),
                ("사업명", row.get("사업명")),
                ("전문기관", row.get("전문기관") or row.get("담당부서")),
                ("소관부처", row.get("소관부처")),
                ("해당 과제명", first_non_empty(top_related, "llm_project_name", "project_name", "대표과제명")),
                ("RFP 제목", first_non_empty(top_related, "llm_rfp_title", "rfp_title")),
                ("추천도", first_non_empty(top_related, "llm_recommendation", "recommendation", "대표추천도")),
                ("점수", clean(top_related.get("llm_fit_score") or top_related.get("rfp_score") or row.get("대표점수"))),
                ("총예산", first_non_empty(top_related, "llm_total_budget_text", "total_budget_text", "budget", "대표예산")),
                ("과제별 예산", first_non_empty(top_related, "llm_per_project_budget_text", "per_project_budget_text")),
            ],
        )
    with top_right:
        render_detail_card(
            "연결 정보",
            [
                ("공고ID", row.get("공고ID")),
                ("공고번호", row.get("공고번호")),
                ("전문기관", row.get("전문기관") or row.get("담당부서")),
                ("소관부처", row.get("소관부처")),
                ("공고상태", row.get("공고상태")),
                ("검토 여부", row.get("검토 여부")),
                ("공모유형", first_non_empty(top_related, "pbofr_type")),
            ],
        )

    action_favorite, action_left, action_right = st.columns([1.15, 1, 1.85])
    with action_favorite:
        render_favorite_scrap_button(
            notice_id=clean(row.get("怨듦퀬ID") or row.get("notice_id")),
            current_value=clean(row.get("寃???щ?") or row.get("review_status")),
            source_key=source_key,
            notice_title=clean(row.get("怨듦퀬紐?") or row.get("notice_title")),
            button_key=f"favorite_notice_{clean(row.get('怨듦퀬ID') or row.get('notice_id'))}",
        )
    with action_left:
        detail_link = resolve_external_detail_link(row, source_key=source_key)
        if detail_link:
            st.link_button(detail_button_label, detail_link, use_container_width=True)
    with action_right:
        st.caption(review_caption)

    st.markdown('<div class="detail-section-title">검토 상태</div>', unsafe_allow_html=True)
    left, right = st.columns(2)
    with left:
        render_detail_card(
            "과제 분석",
            [
                ("추천 이유", first_non_empty(top_related, "llm_reason", "reason", "대표추천이유")),
                (
                    "개념 및 개발 내용",
                    first_non_empty(
                        top_related,
                        "llm_concept_and_development",
                        "concept_and_development",
                        "개념 및 개발 내용",
                    ),
                ),
                (
                    "지원필요성(과제 배경)",
                    first_non_empty(
                        top_related,
                        "llm_support_necessity",
                        "support_necessity",
                        "지원필요성(과제 배경)",
                        "llm_technical_background",
                        "technical_background",
                        "기술개발 배경 및 지원필요성",
                    ),
                ),
                (
                    "활용분야",
                    first_non_empty(
                        top_related,
                        "llm_application_field",
                        "application_field",
                        "활용분야",
                    ),
                ),
                (
                    "지원기간 및 예산·추진체계",
                    first_non_empty(
                        top_related,
                        "llm_support_plan",
                        "support_plan",
                        "지원기간 및 예산·추진체계",
                    ),
                ),
                ("키워드", first_non_empty(top_related, "llm_keywords", "keywords", "대표키워드")),
                ("텍스트 미리보기", first_non_empty(top_related, "text_preview")),
            ],
        )
    with right:
        render_detail_card(
            "공고 정보",
            [
                ("공고일자", row.get("공고일자")),
                ("접수기간", row.get("접수기간")),
                ("현재공고 여부", row.get("is_current")),
                ("연결 과제 수", str(len(related)) if not related.empty else ""),
            ],
        )

    st.markdown('<div class="detail-section-title">검토 여부</div>', unsafe_allow_html=True)
    review_left, review_right = st.columns([1, 1])
    with review_left:
        render_detail_card(
            "현재 상태",
            [
                ("검토 여부", row.get("검토 여부")),
                ("현재 공고상태", row.get("공고상태")),
                ("추천여부", first_non_empty(top_related, "llm_recommendation", "recommendation", "대표추천도")),
            ],
        )
    with review_right:
        render_review_editor(
            notice_id=clean(row.get("공고ID")),
            current_value=clean(row.get("검토 여부")),
            form_key=f"notice_review_form_{clean(row.get('공고ID'))}",
            source_key=source_key,
            notice_title=clean(row.get("공고명")),
        )

    render_notice_comments(row, section_key=f"notice_{clean(row.get('공고ID'))}")

    st.markdown('<div class="detail-section-title">연결된 Opportunity</div>', unsafe_allow_html=True)
    if related.empty:
        st.info("이 공고에 연결된 Opportunity가 아직 없습니다.")
        return

    related_view = ensure_opportunity_row_ids(related)
    related_view["해당 과제명"] = series_from_candidates(related_view, ["llm_project_name", "project_name"])
    related_view["추천도"] = series_from_candidates(related_view, ["llm_recommendation", "recommendation", "추천여부"])
    related_view["점수"] = series_from_candidates(related_view, ["llm_fit_score", "rfp_score"])
    related_view["예산"] = series_from_candidates(related_view, ["llm_total_budget_text", "total_budget_text", "budget"])
    related_view["파일명"] = series_from_candidates(related_view, ["file_name"])
    render_clickable_table(
        related_view,
        [
            "공고명",
            "notice_title",
            "해당 과제명",
            "추천도",
            "점수",
            "예산",
            "파일명",
        ],
        page_key="rfp_queue",
        id_column="_row_id",
        source_key_column="source_key",
    )


def render_pending_detail_from_row(row: dict) -> None:
    row = normalize_row_dict(row)
    if not row:
        st.info("표시할 접수예정 공고가 없습니다.")
        return

    render_detail_header(
        title=clean(row.get("공고명")),
        kicker="Pending Notice Detail",
        chips=[
            (clean(row.get("공고상태")), "accent"),
            (clean(row.get("전문기관")), "neutral"),
            (clean(row.get("공고일자")), "neutral"),
            (f"검토: {clean(row.get('검토 여부') or '미지정')}", "neutral"),
        ],
    )

    top_left, top_right = st.columns([2, 1])
    with top_left:
        render_detail_card(
            "접수예정 공고 정보",
            [
                ("공고명", row.get("공고명")),
                ("접수기간", row.get("접수기간")),
                ("공고일자", row.get("공고일자")),
                ("전문기관", row.get("전문기관")),
                ("소관부처", row.get("소관부처")),
                ("공고번호", row.get("공고번호")),
            ],
        )
    with top_right:
        render_detail_card(
            "식별 정보",
            [
                ("공고ID", row.get("공고ID")),
                ("상태키", row.get("상태키")),
                ("공고상태", row.get("공고상태")),
                ("현재 공고여부", row.get("is_current")),
                ("검토 여부", row.get("검토 여부")),
            ],
        )

    action_left, action_right = st.columns([1, 2])
    with action_left:
        detail_link = resolve_external_detail_link(row)
        if detail_link:
            st.link_button("IRIS 상세 바로가기", detail_link, use_container_width=True)
    with action_right:
        st.caption("접수예정 공고는 별도 master 시트 기준으로 조회합니다.")

    st.markdown('<div class="detail-section-title">공고 메모</div>', unsafe_allow_html=True)
    render_detail_card(
        "운영 정보",
        [
            ("검토 여부", row.get("검토 여부")),
            ("공고상태", row.get("공고상태")),
            ("접수기간", row.get("접수기간")),
        ],
    )
    render_notice_comments(row, section_key=f"pending_{clean(row.get('공고ID'))}")


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
    if any(marker in text for marker in ["비추천", "미추천", "not recommend", "not recommended", "reject"]):
        return "badge-slate"
    if "추천" in text or "recommend" in text:
        return "badge-green"
    if "검토" in text or "hold" in text or "보류" in text:
        return "badge-amber"
    if "마감" in text or "closed" in text:
        return "badge-rose"
    return "badge-slate"


def _pill_html(text: object, *, kind: str = "recommendation", base_class: str = "queue-badge") -> str:
    safe_text = clean(text)
    if not safe_text:
        return ""
    return f'<span class="{base_class} {_badge_class(safe_text, kind=kind)}">{escape(safe_text)}</span>'


def _queue_row_context(row: dict[str, object] | pd.Series) -> dict[str, str]:
    row_dict = normalize_row_dict(row)
    recommendation = first_non_empty(
        row_dict,
        "_queue_recommendation",
        "Recommendation",
        "recommendation",
        "추천여부",
        "llm_recommendation",
        "대표추천도",
    ) or "검토"
    score = _score_value(first_non_empty(row_dict, "llm_fit_score", "rfp_score", "점수", "Score"))
    period = first_non_empty(row_dict, "notice_period", "period", "Period", "접수기간", "요청기간")
    deadline = format_dashboard_deadline_badge(period, first_non_empty(row_dict, "status", "Status"))
    budget = extract_budget_summary(first_non_empty(row_dict, "budget", "Budget", "llm_total_budget_text", "total_budget_text")) or "-"
    agency = first_non_empty(row_dict, "agency", "Agency", "전문기관", "전문기관명") or "-"
    ministry = first_non_empty(row_dict, "ministry", "Ministry", "부처", "주무부처") or "-"
    project = choose_display_project_title(row_dict)
    notice = first_non_empty(row_dict, "notice_title", "Notice Title", "공고명")
    reason = first_non_empty(row_dict, "llm_reason", "reason", "Reason", "llm_concept_and_development", "concept_and_development")
    risk = first_non_empty(row_dict, "llm_support_need", "support_need", "Support Need", "llm_eligibility", "eligibility", "Eligibility", "evidence")
    source_label = first_non_empty(row_dict, "Source", "source_site") or "-"
    status = first_non_empty(row_dict, "Status", "status", "rcve_status", "공고상태") or "-"
    review = first_non_empty(row_dict, "Review", "review_status", "검토여부") or "미검토"
    registered_at = first_non_empty(row_dict, "Date", "ancm_de", "공고일자", "registered_at") or "-"
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
        if not normalized or normalized == "-" or "검토" in normalized:
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
    if "마감" not in unique_options:
        unique_options.append("마감")
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
            first_non_empty(row, "notice_period", "period", "접수기간", "요청기간")
        )
        deadline_sorts.append(deadline_value)
        status_text = clean(ctx["status"])
        is_open = False
        if status_text:
            is_open = "마감" not in status_text and ("접수" in status_text or "진행" in status_text or "예정" in status_text)
        if not is_open and pd.notna(deadline_value):
            is_open = deadline_value >= today
        open_flags.append(bool(is_open))

    working["_queue_recommendation"] = [clean(ctx["recommendation"]) or "-" for ctx in contexts]
    working["_queue_recommendation_rank"] = [
        _recommendation_sort_rank(ctx["recommendation"]) for ctx in contexts
    ]
    working["_queue_score"] = [clean(ctx["score"]) or "-" for ctx in contexts]
    working["_queue_sort_score"] = to_numeric_column(
        series_from_candidates(working, ["llm_fit_score", "rfp_score", "점수", "Score"])
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
        ["project_name", "llm_project_name", "?대떦 怨쇱젣紐?"],
    ).fillna("").astype(str).str.strip()
    return working


def _sort_opportunity_rows_for_display(rows: pd.DataFrame) -> pd.DataFrame:
    if rows.empty:
        return rows
    working = rows.copy()
    if "_queue_recommendation_rank" not in working.columns:
        working = _build_queue_filter_frame(working)
    return working.sort_values(
        by=[
            "_queue_recommendation_rank",
            "_queue_sort_score",
            "_queue_deadline_sort",
            "_queue_project_sort",
        ],
        ascending=[True, False, True, True],
        na_position="last",
        kind="stable",
    )


def _render_rfp_queue_list(rows: pd.DataFrame, *, page_key: str) -> None:
    if rows.empty:
        st.info("표시할 RFP가 없습니다.")
        return

    items: list[str] = []
    archive_mode = "archive" in clean(page_key).lower()
    for _, row in rows.iterrows():
        ctx = _queue_row_context(row)
        row_id = clean(first_non_empty(row, "_row_id", "Row ID"))
        href = build_route_href(
            page_key,
            row_id,
            source_key=resolve_route_source_key_for_row(row, source_key=row.get("source_key")),
        ) if row_id else ""
        badges = "".join(
            [
                _pill_html(ctx["recommendation"]),
                _pill_html(ctx["score"], kind="score"),
                _pill_html(ctx["deadline"], kind="deadline"),
                _pill_html(ctx["archive_reason_label"], kind="archive") if archive_mode else "",
            ]
        )
        archive_reason_html = (
            f'<div class="queue-list-card-reason muted">보관 사유: {escape(ctx["archive_reason_label"])}</div>'
            if archive_mode and clean(ctx["archive_reason_label"])
            else ""
        )
        card_markup = "".join(
            [
                '<div class="queue-card queue-list-card">',
                f'<div class="queue-badge-row">{badges}</div>',
                f'<div class="queue-list-card-title">{escape(truncate_text(ctx["project"], max_chars=96))}</div>',
                f'<div class="queue-list-card-subtitle">{escape(truncate_text(ctx["notice"], max_chars=120))}</div>',
                '<div class="queue-list-card-meta">',
                f'<div class="queue-list-card-meta-item"><div class="queue-list-card-meta-label">전문기관</div><div class="queue-list-card-meta-value">{escape(ctx["agency"])}</div></div>',
                f'<div class="queue-list-card-meta-item"><div class="queue-list-card-meta-label">지원금</div><div class="queue-list-card-meta-value">{escape(ctx["budget"])}</div></div>',
                f'<div class="queue-list-card-meta-item"><div class="queue-list-card-meta-label">공고 상태</div><div class="queue-list-card-meta-value">{escape(ctx["status"])}</div></div>',
                '</div>',
                f'<div class="queue-list-card-reason">{escape(ctx["reason"])}</div>',
                archive_reason_html,
                '</div>',
            ]
        )
        if href:
            items.append(f'<a class="queue-list-link" href="{escape(href, quote=True)}" target="_self">{card_markup}</a>')
        else:
            items.append(card_markup)

    st.markdown(f'<div class="queue-list-shell">{"".join(items)}</div>', unsafe_allow_html=True)


def _legacy_render_opportunity_detail_from_row(row: dict) -> None:
    row = normalize_row_dict(row)
    if not row:
        st.info("표시할 Opportunity가 없습니다.")
        return

    source_key = resolve_notice_source_key(row)
    if source_key == "tipa":
        detail_button_label = "중소기업벤처부 상세 바로가기"
    elif source_key == "nipa":
        detail_button_label = "NIPA 상세 바로가기"
    else:
        detail_button_label = "IRIS 상세 바로가기"

    render_public_notice_card(row, kind="opportunity")

    top_left, top_right = st.columns([2, 1])
    with top_left:
        render_detail_card(
            "과제 핵심 정보",
            [
                ("해당 과제명", first_non_empty(row, "llm_project_name", "project_name")),
                ("공고명", first_non_empty(row, "notice_title", "\uacf5\uace0\uba85")),
                ("RFP 제목", first_non_empty(row, "llm_rfp_title", "rfp_title")),
                ("추천도", first_non_empty(row, "llm_recommendation", "recommendation")),
                ("점수", clean(row.get("llm_fit_score") or row.get("rfp_score"))),
                ("총예산", first_non_empty(row, "llm_total_budget_text", "total_budget_text", "budget")),
                ("과제별 예산", first_non_empty(row, "llm_per_project_budget_text", "per_project_budget_text")),
            ],
        )
    with top_right:
        render_detail_card(
            "연결 정보",
            [
                ("공고ID", row.get("notice_id")),
                ("공고번호", row.get("ancm_no")),
                ("전문기관", row.get("agency")),
                ("소관부처", row.get("ministry")),
                ("공고상태", row.get("rcve_status")),
                ("검토 여부", row.get("review_status")),
                ("공모유형", row.get("pbofr_type")),
            ],
        )

    action_favorite, action_left, action_right = st.columns([1.15, 1, 1.85])
    with action_favorite:
        render_favorite_scrap_button(
            notice_id=clean(row.get("notice_id")),
            current_value=clean(row.get("review_status")),
            source_key=source_key,
            notice_title=clean(row.get("notice_title")),
            button_key=f"favorite_opportunity_{clean(row.get('notice_id'))}",
        )
    with action_left:
        detail_link = resolve_external_detail_link(row, source_key=source_key)
        if detail_link:
            st.link_button(detail_button_label, detail_link, use_container_width=True)
    with action_right:
        st.caption("이 Opportunity는 notice_id 기준으로 공고와 연결됩니다.")

    download_path = resolve_local_file_path(row)
    if download_path:
        with open(download_path, "rb") as f:
            st.download_button(
                "추천 RFP 다운로드",
                data=f.read(),
                file_name=download_path.name,
                mime="application/octet-stream",
                use_container_width=True,
            )

    st.markdown('<div class="detail-section-title">검토 상태</div>', unsafe_allow_html=True)
    left, right = st.columns(2)

    with left:
        render_detail_card(
            "과제 분석",
            [
                ("추천 이유", first_non_empty(row, "llm_reason", "reason")),
                (
                    "개념 및 개발 내용",
                    first_non_empty(
                        row,
                        "llm_concept_and_development",
                        "concept_and_development",
                        "개념 및 개발 내용",
                    ),
                ),
                (
                    "지원필요성(과제 배경)",
                    first_non_empty(
                        row,
                        "llm_support_necessity",
                        "support_necessity",
                        "지원필요성(과제 배경)",
                        "llm_technical_background",
                        "technical_background",
                        "기술개발 배경 및 지원필요성",
                    ),
                ),
                (
                    "활용분야",
                    first_non_empty(
                        row,
                        "llm_application_field",
                        "application_field",
                        "활용분야",
                    ),
                ),
                (
                    "지원기간 및 예산·추진체계",
                    first_non_empty(
                        row,
                        "llm_support_plan",
                        "support_plan",
                        "지원기간 및 예산·추진체계",
                    ),
                ),
                ("키워드", first_non_empty(row, "llm_keywords", "keywords")),
                ("텍스트 미리보기", row.get("text_preview")),
            ],
        )
    with right:
        render_detail_card(
            "문서 및 판별 정보",
            [
                ("파일명", row.get("file_name")),
                ("문서유형", row.get("document_type")),
                ("문서역할", first_non_empty(row, "문서역할", "llm_document_role")),
                ("과제명 근거", first_non_empty(row, "과제명근거", "llm_project_name_source")),
                ("과제명 신뢰도", first_non_empty(row, "과제명신뢰도", "llm_project_name_confidence")),
                ("RFP 제목 근거", first_non_empty(row, "RFP제목근거", "llm_rfp_title_source")),
                ("근거 문장", first_non_empty(row, "근거문장", "llm_evidence")),
                ("충돌 플래그", first_non_empty(row, "충돌플래그", "llm_conflict_flags")),
                ("파일유형", row.get("file_type")),
                ("원천사이트", row.get("source_site")),
            ],
        )

    st.markdown('<div class="detail-section-title">검토 여부</div>', unsafe_allow_html=True)
    review_left, review_right = st.columns([1, 1])
    with review_left:
        render_detail_card(
            "현재 상태",
            [
                ("검토 여부", row.get("review_status")),
                ("현재 공고상태", row.get("rcve_status")),
            ],
        )
    with review_right:
        render_review_editor(
            notice_id=clean(row.get("notice_id")),
            current_value=clean(row.get("review_status")),
            form_key=f"opportunity_review_form_{clean(row.get('notice_id'))}",
            source_key=source_key,
            notice_title=clean(row.get("notice_title")),
        )

    comment_row = {
        **row,
        "공고ID": first_non_empty(row, "notice_id", "공고ID"),
        "공고명": first_non_empty(row, "notice_title", "공고명"),
        "검토 여부": first_non_empty(row, "review_status", "검토 여부"),
        "_source_key": source_key,
    }
    render_notice_comments(
        comment_row,
        section_key=f"opportunity_{source_key}_{clean(row.get('notice_id'))}",
    )



def _legacy_render_summary_detail_from_row(row: dict, opportunity_df: pd.DataFrame) -> None:
    row = normalize_row_dict(row)
    if not row:
        st.info("표시할 요약 공고가 없습니다.")
        return

    render_detail_header(
        title=clean(row.get("공고명")),
        kicker="Summary Detail",
        chips=[
            (clean(row.get("대표추천도")), "accent"),
            (clean(row.get("추천도 및 점수")), "neutral"),
            (clean(row.get("전문기관")), "neutral"),
            (clean(row.get("공고일자")), "neutral"),
        ],
    )

    top_left, top_right = st.columns([2, 1])
    with top_left:
        render_detail_card(
            "대표 과제 분석",
            [
                ("해당 과제명", row.get("해당 과제명")),
                ("추천도 및 점수", row.get("추천도 및 점수")),
                ("예산", row.get("예산")),
                ("과제수", row.get("과제수")),
                ("문서수", row.get("문서수")),
            ],
        )
    with top_right:
        render_detail_card(
            "공고 식별 정보",
            [
                ("공고ID", row.get("공고ID")),
                ("공고번호", row.get("공고번호")),
                ("전문기관", row.get("전문기관")),
                ("소관부처", row.get("소관부처")),
                ("검토 여부", row.get("검토 여부")),
            ],
        )

    action_left, action_right = st.columns([1, 2])
    with action_left:
        detail_link = resolve_external_detail_link(row)
        if detail_link:
            st.link_button("IRIS 상세 바로가기", detail_link, use_container_width=True)
    with action_right:
        st.caption("Summary는 대표 과제 기준으로 공고를 요약해서 보여줍니다.")

    related = pd.DataFrame()
    if not opportunity_df.empty and "notice_id" in opportunity_df.columns:
        notice_key = normalize_notice_id_for_match(row.get("공고ID"))
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

    st.markdown('<div class="detail-section-title">검토 상태</div>', unsafe_allow_html=True)
    left, right = st.columns(2)
    with left:
        render_detail_card(
            "공고 정보",
            [
                ("공고일자", row.get("공고일자")),
                ("공고상태", row.get("공고상태")),
                ("접수기간", row.get("접수기간")),
                ("is_current", row.get("is_current")),
            ],
        )
    with right:
        render_review_editor(
            notice_id=clean(row.get("공고ID")),
            current_value=clean(row.get("검토 여부")),
            form_key=f"summary_review_form_{clean(row.get('공고ID'))}",
            notice_title=clean(row.get("공고명")),
        )

    st.markdown('<div class="detail-section-title">대표 분석 요약</div>', unsafe_allow_html=True)
    render_detail_card(
        "과제 분석",
        [
            ("추천 이유", first_non_empty(top_related, "llm_reason", "reason", "대표추천이유")),
            (
                "개념 및 개발 내용",
                first_non_empty(
                    top_related,
                    "llm_concept_and_development",
                    "concept_and_development",
                    "개념 및 개발 내용",
                ),
            ),
            (
                "지원필요성(과제 배경)",
                first_non_empty(
                    top_related,
                    "llm_support_necessity",
                    "support_necessity",
                    "지원필요성(과제 배경)",
                    "llm_technical_background",
                    "technical_background",
                ),
            ),
            (
                "활용분야",
                first_non_empty(
                    top_related,
                    "llm_application_field",
                    "application_field",
                    "활용분야",
                ),
            ),
            (
                "지원기간 및 예산·추진체계",
                first_non_empty(
                    top_related,
                    "llm_support_plan",
                    "support_plan",
                    "지원기간 및 예산·추진체계",
                ),
            ),
            ("대표과제명", first_non_empty(top_related, "llm_project_name", "project_name", "대표과제명")),
            ("대표예산", first_non_empty(top_related, "llm_total_budget_text", "total_budget_text", "budget", "대표예산")),
            ("대표키워드", first_non_empty(top_related, "llm_keywords", "keywords", "대표키워드")),
        ],
    )

    render_notice_comments(row, section_key=f"summary_{clean(row.get('공고ID'))}")


def render_notice_page(notice_df: pd.DataFrame, opportunity_df: pd.DataFrame) -> None:
    render_notice_page_with_scope(
        notice_df,
        opportunity_df,
        page_key="notice",
        title="진행 공고",
        default_status_scope="접수중",
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
    if "공고ID" in combined.columns:
        combined = combined.drop_duplicates(subset=["공고ID"], keep="first")
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


def _legacy_render_notice_page_with_scope(
    source_df: pd.DataFrame,
    opportunity_df: pd.DataFrame,
    *,
    page_key: str,
    title: str,
    default_status_scope: str,
    current_only_default: bool,
    archive: bool = False,
) -> None:
    subtitle = "수집된 공고를 상태와 기관 기준으로 정리해 봅니다."
    if archive:
        subtitle = "종료되었거나 보관 대상으로 분류된 공고를 모아 봅니다."
    elif default_status_scope == "예정":
        subtitle = "예정 공고와 접수 예정 건을 먼저 확인합니다."
    current_view, selected_notice_id = get_route_state(page_key)
    if current_view == "detail":
        selected_row = get_row_by_column_value(source_df, "공고ID", selected_notice_id)
        action_col, info_col = st.columns([1, 5])
        with action_col:
            if st.button("RFP Dashboard로", key=f"{page_key}_back_to_dashboard", use_container_width=True):
                navigate_to_route("dashboard", "dashboard")
        with info_col:
            st.markdown('<div class="page-note">RFP 추천 화면에서 연결된 공고 상세를 확인하는 화면입니다.</div>', unsafe_allow_html=True)
        render_notice_detail_from_row(selected_row, opportunity_df)
        return

    render_page_header(title, subtitle, eyebrow="Notice")

    filtered = source_df.copy()
    filtered = filter_archived_notice_rows(filtered) if archive else filter_current_notice_rows(filtered)
    filtered["사업비"] = series_from_candidates(filtered, ["사업비", "대표예산"]).apply(extract_budget_summary)
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
    filtered = apply_multiselect_filter(filtered, "전문기관", "전문기관", f"{page_key}_agency")
    filtered = apply_multiselect_filter(filtered, "소관부처", "소관부처", f"{page_key}_ministry")
    filtered = apply_multiselect_filter(filtered, "검토 여부", "검토 여부", f"{page_key}_review")

    filtered = filtered[
        build_contains_mask(
            filtered,
            ["공고명", "공고번호", "전문기관", "소관부처", "공고ID", "대표과제명"],
            search_text,
        )
    ]

    render_metrics(
        [
            ("공고 수", str(len(filtered))),
            ("현재 공고", str(int((filtered["is_current"] == "Y").sum()) if "is_current" in filtered.columns else 0)),
            ("전문기관 수", str(filtered["전문기관"].nunique() if "전문기관" in filtered.columns else 0)),
            ("검토 완료", str(int(filtered["검토 여부"].fillna("").ne("").sum()) if "검토 여부" in filtered.columns else 0)),
        ]
    )

    render_section_label("Notice List")
    st.markdown(
        f'<div class="page-note">공고명 또는 과제명을 클릭하면 상세 공고와 연결 RFP를 함께 확인할 수 있습니다. 현재 {len(filtered)}건</div>',
        unsafe_allow_html=True,
    )
    render_clickable_table(
        filtered,
        NOTICE_PREFERRED_COLUMNS,
        page_key=page_key,
        id_column="공고ID",
    )


def _legacy_render_opportunity_page_aligned(
    df: pd.DataFrame,
    *,
    page_key: str | None = None,
    title: str | None = None,
    archive: bool = False,
    all_df: pd.DataFrame | None = None,
) -> None:
    page_key = page_key or ("opportunity_archive" if archive else "opportunity")
    title = title or ("RFP Archive" if archive else "RFP Queue")
    subtitle = "사업공고 내 지원 가능한 RFP를 추천합니다."
    if archive:
        subtitle = "보관 대상으로 분류된 RFP 분석 결과를 가볍게 탐색할 수 있습니다."
    render_page_header(title, subtitle, eyebrow="RFP")

    source_df = ensure_opportunity_row_ids(df)
    working_source_df = ensure_opportunity_row_ids(all_df) if all_df is not None and not all_df.empty else source_df
    if archive:
        working_source_df = filter_archived_opportunity_rows(working_source_df)
    if working_source_df.empty:
        st.info("표시할 RFP가 없습니다.")
        return

    working = _build_queue_filter_frame(working_source_df)
    recommendation_options = build_queue_recommendation_options(working["_queue_recommendation"])
    status_options = build_queue_status_options(working["_queue_status"])

    filter_cols = st.columns(2)
    with filter_cols[0]:
        selected_recommendation = st.multiselect(
            "추천 상태",
            options=recommendation_options,
            default=[],
            key=f"{page_key}_filter_recommendation_aligned",
            placeholder="전체",
        )
    with filter_cols[1]:
        selected_status = st.multiselect(
            "공고 상태",
            options=status_options,
            default=[],
            key=f"{page_key}_filter_status_aligned",
            placeholder="전체",
        )

    filtered = filter_queue_working_frame(
        working,
        selected_recommendation=selected_recommendation,
        selected_status=selected_status,
        archive=archive,
    )
    if filtered.empty:
        st.info("검색 조건에 맞는 RFP가 없습니다.")
        return

    filtered = _sort_opportunity_rows_for_display(filtered)

    render_metrics(
        [
            ("RFP Count", str(len(filtered))),
            ("Recommended", str(int((filtered["recommendation"] == "추천").sum()) if "recommendation" in filtered.columns else 0)),
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
            if st.button("목록으로", key=f"{page_key}_back_to_table_aligned", use_container_width=True):
                switch_to_table(page_key)
        with info_col:
            st.markdown('<div class="page-note">브라우저 뒤로가기로도 이전 화면으로 돌아갈 수 있습니다.</div>', unsafe_allow_html=True)
        render_opportunity_detail_from_row(selected_row)
        return

    render_section_label("RFP Analysis List")
    st.markdown(
        f'<div class="page-note">공고명이나 과제명을 누르면 상세 공고와 RFP 분석 페이지로 이동합니다. 현재 {len(filtered)}건</div>',
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
    subtitle = "사업공고 내 지원 가능한 RFP를 추천합니다."
    if archive:
        subtitle = "보관 대상으로 분류된 RFP 분석 결과를 가볍게 탐색할 수 있습니다."
    render_page_header(title, subtitle, eyebrow="RFP")

    source_df = ensure_opportunity_row_ids(df)
    filtered = filter_archived_opportunity_rows(source_df) if archive else filter_current_opportunity_rows(source_df)
    filtered = filter_rankable_opportunity_rows(filtered)
    if filtered.empty:
        st.info("표시할 RFP가 없습니다.")
        return

    working = filtered.copy()
    working["_queue_recommendation"] = series_from_candidates(working, ["추천여부", "recommendation"]).fillna("").astype(str).str.strip()
    working["_queue_status"] = series_from_candidates(working, ["공고상태", "status", "rcve_status"]).fillna("").astype(str).apply(normalize_notice_status_label)
    working["_queue_deadline_sort"] = series_from_candidates(working, ["접수기간", "period"]).apply(extract_period_end)
    working["_queue_project_sort"] = series_from_candidates(working, ["해당 과제명", "project_name", "llm_project_name"]).fillna("").astype(str).str.strip()

    recommendation_options = sorted(
        [value for value in working["_queue_recommendation"].unique().tolist() if clean(value)]
    )
    status_options = sorted(
        [value for value in working["_queue_status"].unique().tolist() if clean(value)]
    )

    st.markdown('<div class="queue-shell-note">추천 상태와 공고 상태만 빠르게 좁히고, 결과 행을 눌러 상세 공고와 RFP 내용을 바로 확인할 수 있게 구성했습니다.</div>', unsafe_allow_html=True)
    st.markdown('<div class="queue-filter-label">요건 / 필터</div>', unsafe_allow_html=True)
    st.markdown(
        '<div class="queue-filter-help">추천 상태와 공고 상태만 빠르게 좁혀서 분석할 RFP를 확인합니다.</div>',
        unsafe_allow_html=True,
    )
    filter_cols = st.columns(2)
    with filter_cols[0]:
        selected_recommendation = st.multiselect(
            "추천 상태",
            options=recommendation_options,
            default=[],
            key=f"{page_key}_filter_recommendation_aligned",
            placeholder="전체",
        )
    with filter_cols[1]:
        selected_status = st.multiselect(
            "공고 상태",
            options=status_options,
            default=[],
            key=f"{page_key}_filter_status_aligned",
            placeholder="전체",
        )

    filtered = working.copy()
    if selected_recommendation:
        filtered = filtered[filtered["_queue_recommendation"].isin(selected_recommendation)]
    if selected_status:
        filtered = filtered[filtered["_queue_status"].isin(selected_status)]

    filtered = _sort_opportunity_rows_for_display(filtered)

    render_metrics(
        [
            ("RFP 분석 건수", str(len(filtered))),
            ("추천 건수", str(int((filtered["recommendation"] == "추천").sum()) if "recommendation" in filtered.columns else 0)),
            ("평균 점수", safe_mean(filtered["rfp_score"]) if "rfp_score" in filtered.columns and len(filtered) > 0 else "-"),
            ("공고 수", str(filtered["notice_id"].nunique() if "notice_id" in filtered.columns else 0)),
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
            if st.button("테이블로 돌아가기", key=f"{page_key}_back_to_table_aligned", use_container_width=True):
                switch_to_table(page_key)
        with info_col:
            st.markdown('<div class="page-note">브라우저 뒤로가기로도 표 화면으로 돌아갈 수 있습니다.</div>', unsafe_allow_html=True)
        render_opportunity_detail_from_row(selected_row)
        return

    render_section_label("RFP Analysis List")
    st.markdown(
        f'<div class="page-note">공고명이나 과제명을 클릭하면 상세 공고와 RFP 분석 페이지로 이동합니다. 현재 {len(filtered)}건</div>',
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
    status_default: str = "전체",
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
    filtered = apply_multiselect_filter(filtered, "전문기관", "전문기관", f"{page_key}_agency")
    filtered = apply_multiselect_filter(filtered, "소관부처", "소관부처", f"{page_key}_ministry")
    filtered = apply_multiselect_filter(filtered, "검토 여부", "검토 여부", f"{page_key}_review")
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
    render_page_header("Pending Notice", "예정 공고와 접수 예정 건을 먼저 점검합니다.", eyebrow="Pending")
    page_key = "pending"

    source_df = df.copy()
    filtered = prepare_notice_collection_rows(
        source_df,
        page_key=page_key,
        search_columns=["공고명", "공고번호", "전문기관", "소관부처", "공고ID"],
        status_default="예정",
        current_only_default=True,
        extra_multiselects=[("공고상태", "공고상태", "status")],
    )

    render_metrics(
        [
            ("접수예정 공고 수", str(len(filtered))),
            ("현재 표시 공고", str(int((filtered["is_current"] == "Y").sum()) if "is_current" in filtered.columns else 0)),
            ("전문기관 수", str(filtered["전문기관"].nunique() if "전문기관" in filtered.columns else 0)),
            ("검토 완료", str(int(filtered["검토 여부"].fillna("").ne("").sum()) if "검토 여부" in filtered.columns else 0)),
        ]
    )

    current_view, selected_notice_id = get_route_state(page_key)

    if current_view == "detail":
        selected_row = get_row_by_column_value(source_df, "공고ID", selected_notice_id)
        action_col, info_col = st.columns([1, 5])
        with action_col:
            if st.button("테이블로 돌아가기", key="pending_back_to_table", use_container_width=True):
                switch_to_table(page_key)
        with info_col:
            st.markdown('<div class="page-note">브라우저 뒤로가기로도 표 화면으로 돌아갈 수 있습니다.</div>', unsafe_allow_html=True)
        render_pending_detail_from_row(selected_row)
        return

    render_section_label("Pending List")
    st.markdown(
        f'<div class="page-note">공고명 또는 과제명을 클릭하면 상세 페이지로 이동합니다. 현재 {len(filtered)}건</div>',
        unsafe_allow_html=True,
    )
    render_clickable_table(
        filtered,
        PENDING_PREFERRED_COLUMNS,
        page_key=page_key,
        id_column="공고ID",
    )


def _legacy_render_summary_page(df: pd.DataFrame, opportunity_df: pd.DataFrame) -> None:
    render_page_header("Summary", "공고별 대표 과제와 추천 요약을 한눈에 봅니다.", eyebrow="Summary")
    page_key = "summary"

    source_df = df.copy()
    filtered = filter_current_summary_rows(source_df)
    filtered = prepare_notice_collection_rows(
        filtered,
        page_key=page_key,
        search_columns=["공고명", "공고번호", "해당 과제명", "예산", "공고ID"],
        status_default="전체",
        current_only_default=True,
        extra_multiselects=[("대표추천도", "대표추천도", "recommendation")],
    )

    if "대표점수" in filtered.columns and len(filtered) > 0:
        min_score = int(filtered["대표점수"].min())
        max_score = int(filtered["대표점수"].max())
        if min_score < max_score:
            score_range = st.sidebar.slider(
                "대표점수 범위",
                min_value=min_score,
                max_value=max_score,
                value=(min_score, max_score),
                key="summary_score_range",
            )
            filtered = filtered[
                (filtered["대표점수"] >= score_range[0]) &
                (filtered["대표점수"] <= score_range[1])
            ]
        else:
            st.sidebar.caption(f"대표점수 고정값: {min_score}")

    render_metrics(
        [
            ("요약 공고 수", str(len(filtered))),
            ("추천 공고", str(int((filtered["대표추천도"] == "추천").sum()) if "대표추천도" in filtered.columns else 0)),
            ("평균 대표점수", safe_mean(filtered["대표점수"]) if "대표점수" in filtered.columns and len(filtered) > 0 else "-"),
            ("평균 과제수", safe_mean(filtered["과제수"]) if "과제수" in filtered.columns and len(filtered) > 0 else "-"),
        ]
    )

    current_view, selected_notice_id = get_route_state(page_key)

    if current_view == "detail":
        selected_row = get_row_by_column_value(source_df, "공고ID", selected_notice_id)
        action_col, info_col = st.columns([1, 5])
        with action_col:
            if st.button("목록으로", key="summary_back_to_table", use_container_width=True):
                switch_to_table(page_key)
        with info_col:
            st.markdown('<div class="page-note">브라우저 뒤로가기로도 요약 리스트 화면으로 돌아갈 수 있습니다.</div>', unsafe_allow_html=True)
        render_summary_detail_from_row(selected_row, opportunity_df)
        return

    render_section_label("Summary List")
    st.markdown(
        f'<div class="page-note">공고명 또는 과제명을 클릭하면 대표 분석 요약과 연결된 RFP 상세를 함께 확인할 수 있습니다. 현재 {len(filtered)}건</div>',
        unsafe_allow_html=True,
    )
    render_clickable_table(
        filtered,
        SUMMARY_PREFERRED_COLUMNS,
        page_key=page_key,
        id_column="공고ID",
    )


def _legacy_render_summary_page_v2(df: pd.DataFrame, opportunity_df: pd.DataFrame) -> None:
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


def render_errors_page(df: pd.DataFrame) -> None:
    st.subheader("Errors")
    page_key = "errors"

    filtered = enrich_error_df(df)
    if filtered.empty:
        st.info("현재 적재된 오류 행이 없습니다.")
        return

    search_text = render_sidebar_search()
    filtered = apply_multiselect_filter(filtered, "출처사이트", "source_site", "errors_source")
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
            ("오류 수", str(len(filtered))),
            ("공고 수", str(filtered["notice_id"].nunique() if "notice_id" in filtered.columns else 0)),
            ("출처 수", str(filtered["source_site"].nunique() if "source_site" in filtered.columns else 0)),
        ]
    )

    st.caption(f"검증/파싱/LLM 오류 행입니다. 현재 {len(filtered)}건")
    visible_columns = [column for column in ERROR_PREFERRED_COLUMNS if column in filtered.columns]
    st.dataframe(filtered[visible_columns] if visible_columns else filtered, use_container_width=True, hide_index=True)


def render_source_notice_page(
    df: pd.DataFrame,
    data_origin: str,
    *,
    prefix: str,
    title: str,
    source_label: str = "중소기업벤처부",
    view_columns: list[str] | None = None,
    archive: bool = False,
) -> None:
    st.markdown(f"### {title}")
    st.caption(f"{source_label} 연계 공고 목록입니다. 데이터 소스: {data_origin}")

    if df.empty:
        st.info(f"{source_label} 공고 데이터를 아직 불러오지 못했습니다.")
        return

    current_view, selected_notice_id = get_route_state(prefix)
    if current_view == "detail":
        selected_row = get_row_by_column_value(df, "공고ID", selected_notice_id)
        action_col, info_col = st.columns([1, 5])
        with action_col:
            if st.button("목록으로 돌아가기", key=f"{prefix}_back_to_table", use_container_width=True):
                switch_to_table(prefix)
        with info_col:
            st.caption("브라우저 뒤로가기를 눌러도 목록으로 돌아갈 수 있습니다.")
        render_notice_detail_from_row(selected_row, pd.DataFrame())
        return

    filtered = df.copy()
    filtered = filter_archived_notice_rows(filtered) if archive else filter_current_notice_rows(filtered)
    default_status = "전체" if archive else "예정" if "scheduled" in prefix else "접수중"
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
    filtered = apply_multiselect_filter(filtered, "전문기관", "전문기관", f"{prefix}_agency")
    filtered = apply_multiselect_filter(filtered, "소관부처", "소관부처", f"{prefix}_ministry")
    filtered = apply_multiselect_filter(filtered, "검토 여부", "검토 여부", f"{prefix}_review")

    if search_text:
        filtered = filtered[build_contains_mask(filtered, ["공고명", "공고번호", "전문기관", "담당부서", "소관부처", "사업명"], search_text)]

    open_count = int(filtered["상태"].fillna("").astype(str).str.strip().eq("접수중").sum()) if "상태" in filtered.columns else 0
    metric_cols = st.columns(3)
    metric_cols[0].metric("공고 수", str(len(filtered)))
    metric_cols[1].metric("접수중", str(open_count))
    metric_cols[2].metric("담당부서 수", str(filtered["담당부서"].nunique() if "담당부서" in filtered.columns else 0))

    st.caption(f"공고명 또는 과제명을 클릭하면 상세 페이지로 이동합니다. 현재 {len(filtered)}건")
    render_clickable_table(
        filtered,
        view_columns or MSS_VIEW_COLUMNS,
        page_key=prefix,
        id_column="공고ID",
    )


def normalize_favorite_notice_df(df: pd.DataFrame, *, source_key: str, source_label: str) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame()

    working = df.copy()
    working["매체"] = source_label
    working["_source_key"] = source_key
    working["공고ID"] = series_from_candidates(working, ["공고ID", "notice_id"])
    working["공고명"] = series_from_candidates(working, ["공고명", "notice_title", "title"])
    working["공고번호"] = series_from_candidates(working, ["공고번호", "notice_no", "ancm_no"])
    working["공고일자"] = series_from_candidates(working, ["공고일자", "등록일", "registered_at", "ancm_de"])
    working["접수기간"] = series_from_candidates(working, ["접수기간", "신청기간", "period"])
    working["전문기관"] = series_from_candidates(working, ["전문기관", "전문기관명", "agency"])
    working["담당부서"] = series_from_candidates(working, ["담당부서", "department", "agency"])
    working["소관부처"] = series_from_candidates(working, ["소관부처", "ministry"])
    working["공고상태"] = series_from_candidates(working, ["공고상태", "상태", "status", "rcve_status"])
    working["검토 여부"] = series_from_candidates(working, ["검토 여부", "검토여부", "review_status"])
    working["상세링크"] = series_from_candidates(working, ["상세링크", "detail_link"])
    working["상세링크"] = working.apply(
        lambda row: resolve_external_detail_link(row, source_key=source_key),
        axis=1,
    )
    working["_favorite_id"] = working.apply(
        lambda row: f"{source_key}::{clean(row.get('공고ID'))}",
        axis=1,
    )
    if "대표점수" not in working.columns:
        working["대표점수"] = 0
    working["_sort_date"] = parse_date_column(working["공고일자"])
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
        mss_df = mss_df.drop_duplicates(subset=["공고ID"], keep="first")
        frames.append(normalize_favorite_notice_df(mss_df, source_key="tipa", source_label="중소기업벤처부"))

    nipa_current_df = source_datasets["nipa_current"]
    nipa_past_df = source_datasets["nipa_past"]
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






def render_other_crawlers_source_page() -> None:
    st.subheader("Other Crawlers")
    st.info("다른 크롤러 소스는 여기에 확장할 수 있습니다.")


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
        st.error(f"시트 로딩 실패: {exc}")
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
            ("favorites", "관심공고"),
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
            default_status_scope="전체",
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


def _legacy_main(app_mode: str = "viewer"):
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
        st.error(f"시트 로딩 실패: {exc}")
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
            label="세부 페이지",
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


if __name__ == "__main__":
    main()

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
    if label in {"사업 규모", "지원금", "총예산", "과제별 예산"}:
        budget_summary = extract_budget_summary(raw_text)
        if budget_summary:
            raw_text = budget_summary
    return detail_display_text(label, raw_text)

def _notice_detail_deadline_parts(value: object) -> tuple[str, str]:
    period_text = clean(value)
    if not period_text:
        return "", MISSING_DISPLAY_TEXT
    period_end = extract_period_end(period_text)
    if pd.isna(period_end):
        return "", period_text
    d_day = int((period_end.normalize() - pd.Timestamp.now().normalize()).days)
    if d_day > 0:
        return f"D-{d_day}", period_text
    if d_day == 0:
        return "D-Day", period_text
    return "마감", period_text

def _notice_detail_value_html(label: str, value: object, *, kind: str = "text") -> str:
    if kind == "chips":
        chips = parse_detail_tag_items(value)
        if not chips:
            return f'<span class="notice-detail-empty">{MISSING_DISPLAY_TEXT}</span>'
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
            '<div class="notice-detail-empty-block">표시할 정보가 없습니다.</div>'
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
        body = "표시할 요약 정보가 없습니다."

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
        '<div class="notice-detail-sidebar-title">공고 한눈에 보기</div>'
        '<div class="notice-detail-sidebar-meta">'
        '<div class="notice-detail-sidebar-label">핵심 키워드</div>'
        f'<div class="notice-detail-sidebar-tags">{keyword_html}</div>'
        '</div>'
        '<div class="notice-detail-sidebar-grid">'
        '<div class="notice-detail-sidebar-item">'
        '<div class="notice-detail-sidebar-label">사업 규모</div>'
        f'<div class="notice-detail-sidebar-value">{escape(_notice_detail_scalar_text("사업 규모", total_budget))}</div>'
        '</div>'
        '<div class="notice-detail-sidebar-item">'
        '<div class="notice-detail-sidebar-label">지원금</div>'
        f'<div class="notice-detail-sidebar-value">{escape(_notice_detail_scalar_text("지원금", per_project_budget))}</div>'
        '</div>'
        '<div class="notice-detail-sidebar-item">'
        '<div class="notice-detail-sidebar-label">전문기관</div>'
        f'<div class="notice-detail-sidebar-value">{escape(_notice_detail_scalar_text("전문기관", agency))}</div>'
        '</div>'
        '<div class="notice-detail-sidebar-item">'
        '<div class="notice-detail-sidebar-label">소관부처</div>'
        f'<div class="notice-detail-sidebar-value">{escape(_notice_detail_scalar_text("소관부처", ministry))}</div>'
        '</div>'
        '</div>'
        '<div class="notice-detail-sidebar-period">'
        '<div class="notice-detail-sidebar-label">신청 기간</div>'
        '<div class="notice-detail-deadline-wrap">'
        f'{deadline_html}'
        f'<span class="notice-detail-period-text">{escape(period_text)}</span>'
        '</div>'
        '</div>'
        '<div class="notice-detail-sidebar-grid compact">'
        '<div class="notice-detail-sidebar-item">'
        '<div class="notice-detail-sidebar-label">추천 상태</div>'
        f'<div class="notice-detail-sidebar-value">{escape(_notice_detail_scalar_text("추천 상태", recommendation))}</div>'
        '</div>'
        '<div class="notice-detail-sidebar-item">'
        '<div class="notice-detail-sidebar-label">적합 점수</div>'
        f'<div class="notice-detail-sidebar-value">{escape(_notice_detail_scalar_text("적합 점수", score))}</div>'
        '</div>'
        '<div class="notice-detail-sidebar-item">'
        '<div class="notice-detail-sidebar-label">연결 RFP</div>'
        f'<div class="notice-detail-sidebar-value">{related_count}건</div>'
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
    parts = re.split(r"\n{2,}|(?<=[.!?다요음])\s+(?=[A-Z가-힣0-9□■○●])", normalized)
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
        benefit_parts.append(f"사업비 규모는 {total_budget}입니다.")
    if per_project_budget:
        benefit_parts.append(f"과제별 지원 조건은 {per_project_budget} 기준으로 정리됩니다.")
    if eligibility:
        benefit_parts.append(f"지원 가능 기관은 {eligibility}입니다.")
    if period:
        benefit_parts.append(f"사업 기간 및 접수 일정은 {period} 기준으로 운영됩니다.")
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
    base_row = normalize_row_dict(row)
    notice_row = normalize_row_dict(notice_row)

    summary_text = first_non_empty(base_row, "llm_reason", "reason", "summary", "analysis_summary")
    background_text = first_non_empty(
        base_row,
        "llm_support_necessity",
        "support_necessity",
        "llm_technical_background",
        "technical_background",
        "project_overview",
        "project_summary",
        "사업개요",
        "과제개요",
    )
    objective_text = first_non_empty(
        base_row,
        "llm_concept_and_development",
        "concept_and_development",
        "llm_application_field",
        "application_field",
        "활용분야",
        "과제 목표",
    )
    detail_text = first_non_empty(
        base_row,
        "llm_development_content",
        "development_content",
        "상세내용",
        "지원내용",
        "상세 내역",
    )
    support_need_text = first_non_empty(base_row, "llm_support_need", "support_need")
    support_plan_text = first_non_empty(base_row, "llm_support_plan", "support_plan")
    eligibility_text = first_non_empty(base_row, "llm_eligibility", "eligibility", "지원대상")
    total_budget_text = first_non_empty(base_row, "llm_total_budget_text", "total_budget_text", "budget")
    per_project_budget_text = first_non_empty(base_row, "llm_per_project_budget_text", "per_project_budget_text")
    period_value = clean(period_text) or first_non_empty(
        base_row,
        "rfp_period",
        "project_period",
        "llm_project_period",
        "notice_period",
        "period",
    ) or first_non_empty(notice_row, "접수기간", "신청기간", "period")

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
        {"title": "사업 개요 및 배경", "body": _join_display_blocks(background_text, support_need_text, max_items=3)},
        {"title": "과제 목표", "body": objective_text},
        {"title": "과제 내용", "body": _join_display_blocks(detail_text, support_plan_text, max_items=3)},
        {"title": "지원 내용 및 혜택", "body": benefit_text},
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
    from pages.notice_detail import render_page

    render_page(st, row, opportunity_df, api=PAGE_API)

def render_summary_detail_from_row(row: dict, opportunity_df: pd.DataFrame) -> None:
    row = normalize_row_dict(row)
    if not row:
        st.info("표시할 요약 공고가 없습니다.")
        return

    render_detail_header(
        title=clean(row.get("공고명")),
        kicker="Summary Detail",
        chips=[
            (clean(row.get("대표추천도")), "accent"),
            (clean(row.get("추천도 및 점수")), "neutral"),
            (clean(row.get("전문기관")), "neutral"),
            (clean(row.get("공고일자")), "neutral"),
        ],
    )

    top_left, top_right = st.columns([2, 1])
    with top_left:
        render_detail_card(
            "대표 과제 분석",
            [
                ("해당 과제명", row.get("해당 과제명")),
                ("추천도 및 점수", row.get("추천도 및 점수")),
                ("예산", row.get("예산")),
                ("과제수", row.get("과제수")),
                ("문서수", row.get("문서수")),
            ],
        )
    with top_right:
        render_detail_card(
            "공고 식별 정보",
            [
                ("공고ID", row.get("공고ID")),
                ("공고번호", row.get("공고번호")),
                ("전문기관", row.get("전문기관")),
                ("소관부처", row.get("소관부처")),
                ("검토 여부", row.get("검토 여부")),
            ],
        )

    action_left, action_right = st.columns([1, 2])
    with action_left:
        detail_link = resolve_external_detail_link(row)
        if detail_link:
            st.link_button("IRIS 상세 바로가기", detail_link, use_container_width=True)
    with action_right:
        st.caption("Summary는 대표 과제 기준으로 공고를 요약해서 보여줍니다.")

    related = pd.DataFrame()
    if not opportunity_df.empty and "notice_id" in opportunity_df.columns:
        notice_key = normalize_notice_id_for_match(row.get("공고ID"))
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

    st.markdown('<div class="detail-section-title">검토 상태</div>', unsafe_allow_html=True)
    left, right = st.columns(2)
    with left:
        render_detail_card(
            "공고 정보",
            [
                ("공고일자", row.get("공고일자")),
                ("공고상태", row.get("공고상태")),
                ("접수기간", row.get("접수기간")),
                ("is_current", row.get("is_current")),
            ],
        )
    with right:
        render_review_editor(
            notice_id=clean(row.get("공고ID")),
            current_value=clean(row.get("검토 여부")),
            form_key=f"summary_review_form_{clean(row.get('공고ID'))}",
        )

    st.markdown('<div class="detail-section-title">대표 분석 요약</div>', unsafe_allow_html=True)
    render_detail_card(
        "과제 분석",
        [
            ("추천 이유", first_non_empty(top_related, "llm_reason", "reason", "대표추천이유")),
            (
                "개념 및 개발 내용",
                first_non_empty(
                    top_related,
                    "llm_concept_and_development",
                    "concept_and_development",
                    "개념 및 개발 내용",
                ),
            ),
            (
                "지원필요성(과제 배경)",
                first_non_empty(
                    top_related,
                    "llm_support_necessity",
                    "support_necessity",
                    "지원필요성(과제 배경)",
                    "llm_technical_background",
                    "technical_background",
                ),
            ),
            (
                "활용분야",
                first_non_empty(
                    top_related,
                    "llm_application_field",
                    "application_field",
                    "활용분야",
                ),
            ),
            (
                "지원기간 및 예산·추진체계",
                first_non_empty(
                    top_related,
                    "llm_support_plan",
                    "support_plan",
                    "지원기간 및 예산·추진체계",
                ),
            ),
            ("대표과제명", first_non_empty(top_related, "llm_project_name", "project_name", "대표과제명")),
            ("대표예산", first_non_empty(top_related, "llm_total_budget_text", "total_budget_text", "budget", "대표예산")),
            ("대표키워드", first_non_empty(top_related, "llm_keywords", "keywords", "대표키워드")),
            ("대표관심영역", first_non_empty(top_related, "target_market", "대표관심영역")),
        ],
    )

    render_notice_comments(row, section_key=f"summary_{clean(row.get('공고ID'))}")

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
        selected_row = get_row_by_column_value(source_df, "공고ID", selected_notice_id)
        action_col, info_col = st.columns([1, 5])
        with action_col:
            if st.button("RFP Dashboard로", key=f"{page_key}_back_to_dashboard", use_container_width=True):
                navigate_to_route("dashboard", "dashboard")
        with info_col:
            st.markdown('<div class="page-note">RFP 추천 화면에서 연결된 공고 상세를 확인하는 화면입니다.</div>', unsafe_allow_html=True)
        render_notice_detail_from_row(selected_row, opportunity_df)
        return

    subtitle = "수집된 공고를 상태와 기관 기준으로 정리해 봅니다."
    if archive:
        subtitle = "종료되었거나 보관 대상으로 분류된 공고를 모아 봅니다."
    elif default_status_scope == "예정":
        subtitle = "예정 공고와 접수 예정 건을 먼저 확인합니다."
    render_page_header(title, subtitle, eyebrow="Notice")

    filtered = source_df.copy()
    if not already_scoped:
        filtered = filter_archived_notice_rows(filtered) if archive else filter_current_notice_rows(filtered)
    filtered["사업비"] = series_from_candidates(filtered, ["사업비", "대표예산"]).apply(extract_budget_summary)
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
    filtered = apply_multiselect_filter(filtered, "전문기관", "전문기관", f"{page_key}_agency")
    filtered = apply_multiselect_filter(filtered, "소관부처", "소관부처", f"{page_key}_ministry")
    filtered = apply_multiselect_filter(filtered, "검토 여부", "검토 여부", f"{page_key}_review")

    filtered = filtered[
        build_contains_mask(
            filtered,
            ["공고명", "공고번호", "전문기관", "소관부처", "공고ID", "대표과제명"],
            search_text,
        )
    ]

    render_metrics(
        [
            ("공고 수", str(len(filtered))),
            ("현재 공고", str(int((filtered["is_current"] == "Y").sum()) if "is_current" in filtered.columns else 0)),
            ("전문기관 수", str(filtered["전문기관"].nunique() if "전문기관" in filtered.columns else 0)),
            ("검토 완료", str(int(filtered["검토 여부"].fillna("").ne("").sum()) if "검토 여부" in filtered.columns else 0)),
        ]
    )

    render_section_label("Notice List")
    st.markdown(
        f'<div class="page-note">공고명 또는 과제명을 클릭하면 상세 페이지로 이동합니다. 현재 {len(filtered)}건</div>',
        unsafe_allow_html=True,
    )
    render_clickable_table(
        filtered,
        NOTICE_PREFERRED_COLUMNS,
        page_key=page_key,
        id_column="공고ID",
    )

def render_opportunity_detail_from_row(row: dict) -> None:
    from pages.rfp_detail import render_page

    render_page(st, row, api=PAGE_API)


def render_summary_page(df: pd.DataFrame, opportunity_df: pd.DataFrame) -> None:
    del df

    working = ensure_opportunity_row_ids(filter_rankable_opportunity_rows(filter_current_opportunity_rows(opportunity_df.copy())))
    if working.empty:
        st.info("표시할 분석 대상이 없습니다.")
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
            lambda row: f"{source_key}::{scope}::{clean(first_non_empty(row, '공고ID', 'notice_id'))}",
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
        by=["_sort_date", "매체", "공고명"],
        ascending=[False, True, True],
        na_position="last",
    )




def filter_notice_queue_rows(rows: pd.DataFrame, *, search_text: str) -> pd.DataFrame:
    if rows.empty or not clean(search_text):
        return rows.copy()
    return rows[
        build_contains_mask(
            rows,
            ["매체", "공고명", "공고번호", "전문기관", "담당부서", "소관부처", "공고ID", "공고상태", "접수기간"],
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
        ("전체 공고", str(len(rows)), "all"),
        ("IRIS", str(len(iris_rows)), "iris"),
        ("MSS", str(len(mss_rows)), "mss"),
        ("NIPA", str(len(nipa_rows)), "nipa"),
        ("마감·보관", str(len(archive_rows)), "archive"),
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


def _render_notice_queue_table(rows: pd.DataFrame, *, key_prefix: str) -> None:
    if rows.empty:
        st.markdown('<div class="notice-mailbox-empty">표시할 공고가 없습니다.</div>', unsafe_allow_html=True)
        return
    del key_prefix

    def _deadline_sort_value(value: object) -> int | None:
        text = clean(value)
        if not text or text in {"-", "예정", "마감"}:
            return None
        if text == "D-Day":
            return 0
        match = re.fullmatch(r"D-(\d+)", text)
        if not match:
            return None
        return int(match.group(1))

    def _detail_href(notice_id: str) -> str:
        return build_route_href("notice_queue", notice_id, source_key="notices")

    row_html: list[str] = []
    for _, row in rows.iterrows():
        notice_id = _workspace_notice_id(row)
        detail_href = _detail_href(notice_id)
        dday_label = clean(row.get("D-Day")) or "-"
        dday_sort = _deadline_sort_value(dday_label)
        tone_class = "is-calm"
        if dday_sort is not None and dday_sort <= 3:
            tone_class = "is-critical"
        elif dday_sort is not None and dday_sort <= 14:
            tone_class = "is-warning"
        subtitle = " / ".join(
            part for part in [clean(row.get("Source")), clean(row.get("Notice No"))] if part and part != "-"
        )
        review_value = clean(row.get("Review"))
        favorite_href = build_favorite_toggle_href(
            page_key="notice_queue",
            notice_id=notice_id,
            current_value=review_value,
            source_key=clean(row.get("source_key")) or "iris",
        )
        favorite_label = "해제" if review_value == FAVORITE_REVIEW_STATUS else "등록"
        favorite_class = " is-active" if review_value == FAVORITE_REVIEW_STATUS else ""
        rfp_count_value = _score_value(row.get("RFP Count"))
        rfp_count = escape(str(rfp_count_value))
        agency_value = clean(row.get("Agency")) or ""
        keyword_values = _extract_dashboard_keywords(row, limit=3)
        keyword_chips = "".join(
            f'<span class="queue-grid-chip">{escape(keyword)}</span>'
            for keyword in keyword_values
        )
        if not keyword_values:
            keyword_chips = '<span class="queue-grid-chip is-empty">키워드 없음</span>'
        keyword_class = "queue-grid-keywords is-single" if len(keyword_values) <= 1 else "queue-grid-keywords"
        keywords_html = f'<div class="{keyword_class}">{keyword_chips}</div>'
        period_value = clean(row.get("Period")) or "미정"
        status_badge = notice_status_badge_html(row.get("Status"))
        row_html.append(
            "".join(
                [
                    '<div class="queue-grid-row">',
                    f'<div class="queue-grid-cell">{status_badge}</div>',
                    '<div class="queue-grid-cell is-title">',
                    f'<a class="queue-grid-title" href="{escape(detail_href, quote=True)}" target="_self">{escape(truncate_text(row.get("Title"), max_chars=106))}</a>',
                    (f'<div class="queue-grid-subtitle">{escape(truncate_text(subtitle, max_chars=74))}</div>' if subtitle else ""),
                    '</div>',
                    f'<div class="queue-grid-cell is-keywords">{keywords_html}</div>',
                    f'<div class="queue-grid-cell">{escape(truncate_text(agency_value or "미정", max_chars=20))}</div>',
                    f'<div class="queue-grid-cell queue-grid-period">{escape(truncate_text(period_value, max_chars=28))}</div>',
                    f'<div class="queue-grid-cell"><span class="queue-grid-dday {tone_class}">{escape(dday_label)}</span></div>',
                    f'<div class="queue-grid-cell queue-grid-rfp-count is-center">{rfp_count}</div>',
                    f'<div class="queue-grid-cell is-center"><a class="queue-grid-favorite{favorite_class}" href="{escape(favorite_href, quote=True)}" target="_self">{favorite_label}</a></div>',
                    '</div>',
                ]
            )
        )
    st.markdown(
        (
            '<div class="queue-grid-scroll"><div class="queue-grid">'
            '<div class="queue-grid-head">'
            '<div>상태</div><div class="queue-grid-col-title">공고명</div><div>키워드</div><div>기관</div><div>기간</div><div>D-day</div><div>RFP 수</div><div>관심</div>'
            '</div>'
            + "".join(row_html)
            + '</div></div>'
        ),
        unsafe_allow_html=True,
    )


def _build_notice_rfp_count_lookup(opportunity_rows: pd.DataFrame) -> dict[tuple[str, str], int]:
    if opportunity_rows is None or opportunity_rows.empty:
        return {}
    working = opportunity_rows.copy()
    working["source_key"] = series_from_candidates(working, ["source_key", "_source_key"]).fillna("").astype(str).str.strip().str.lower()
    working["Notice ID"] = series_from_candidates(working, ["Notice ID", "notice_id", "공고ID"]).fillna("").astype(str).str.strip()
    working = working[(working["source_key"] != "") & (working["Notice ID"] != "")]
    if working.empty:
        return {}
    counts = (
        working.groupby(["source_key", "Notice ID"], dropna=False)
        .size()
        .reset_index(name="rfp_count")
    )
    return {
        (clean(row["source_key"]), clean(row["Notice ID"])): int(row["rfp_count"])
        for _, row in counts.iterrows()
    }


def _render_rfp_queue_pagination_controls(
    *,
    route: dict[str, object] | None = None,
    current_page: int,
    total_pages: int,
) -> str:
    base_route = route_core.normalize_route(route or route_core.build_rfp_queue_route())

    def _page_href(page_number: int) -> str:
        next_route = route_core.normalize_route(
            {
                **base_route,
                "view": "list",
                "item_type": "",
                "item_id": "",
                "page_no": max(1, min(int(page_number), total_pages)),
            }
        )
        params = with_auth_params(route_core.serialize_route(next_route))
        return f"?{urlencode(params)}"

    page_numbers = _notice_queue_pagination_window(current_page, total_pages)
    prev_class = " is-disabled" if current_page <= 1 else ""
    next_class = " is-disabled" if current_page >= total_pages else ""
    number_links: list[str] = []
    for page_number in page_numbers:
        active_class = " is-active" if page_number == current_page else ""
        number_links.append(
            f'<a class="queue-page-link{active_class}" href="{escape(_page_href(page_number), quote=True)}" target="_self">{page_number}</a>'
        )
    if total_pages > page_numbers[-1]:
        if total_pages > page_numbers[-1] + 1:
            number_links.append('<span class="queue-page-ellipsis">…</span>')
        number_links.append(
            f'<a class="queue-page-link" href="{escape(_page_href(total_pages), quote=True)}" target="_self">{total_pages}</a>'
        )

    nav_html = (
        f'<div class="queue-pagination-row">'
        f'<a class="queue-page-nav{prev_class}" href="{escape(_page_href(current_page - 1), quote=True)}" target="_self">&lt; 이전</a>'
        f'<a class="queue-page-nav{next_class}" href="{escape(_page_href(current_page + 1), quote=True)}" target="_self">다음 &gt;</a>'
        '</div>'
    )
    number_html = f'<div class="queue-pagination-row">{"".join(number_links)}</div>'
    return f'<nav class="queue-pagination-wrap" aria-label="RFP Queue pagination">{number_html}{nav_html}</nav>'


def _render_notice_queue_pagination_controls(
    *,
    page_state_key: str,
    control_key_prefix: str,
    current_page: int,
    total_pages: int,
    total_rows: int,
    page_size: int,
) -> str:
    del page_state_key, control_key_prefix, total_rows, page_size

    def _page_href(page_number: int) -> str:
        params = get_query_params_dict()
        params.update({
            "source": "notices",
            "page": "notice_queue",
            "view": "table",
            "page_no": str(max(1, min(page_number, total_pages))),
        })
        params.update(_current_notice_queue_filter_params())
        return f"?{urlencode(params)}"

    page_numbers = _notice_queue_pagination_window(current_page, total_pages)
    prev_class = " is-disabled" if current_page <= 1 else ""
    next_class = " is-disabled" if current_page >= total_pages else ""
    number_links = []
    for page_number in page_numbers:
        active_class = " is-active" if page_number == current_page else ""
        number_links.append(
            f'<a class="notice-queue-page-link{active_class}" href="{escape(_page_href(page_number), quote=True)}" target="_self">{page_number}</a>'
        )
    if total_pages > page_numbers[-1]:
        if total_pages > page_numbers[-1] + 1:
            number_links.append('<span class="notice-queue-page-ellipsis">…</span>')
        number_links.append(
            f'<a class="notice-queue-page-link" href="{escape(_page_href(total_pages), quote=True)}" target="_self">{total_pages}</a>'
        )

    nav_html = (
        f'<div class="notice-queue-pagination">'
        f'<a class="notice-queue-page-nav{prev_class}" href="{escape(_page_href(current_page - 1), quote=True)}" target="_self">‹ 이전</a>'
        f'<a class="notice-queue-page-nav{next_class}" href="{escape(_page_href(current_page + 1), quote=True)}" target="_self">다음 ›</a>'
        '</div>'
    )
    number_html = f'<div class="notice-queue-pagination">{"".join(number_links)}</div>'
    return f'<nav class="notice-queue-pagination-wrap" aria-label="Notice Queue pagination">{number_html}{nav_html}</nav>'






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
    page_state_key = f"{page_key}_page_index"
    filter_signature_key = f"{page_key}_filter_signature"
    page_size = 10

    current_view, selected_document_id = get_route_state(page_key)
    if current_view == "detail":
        selected_row = get_row_by_column_value(source_df, "_row_id", selected_document_id)
        if selected_row is None and not all_source_df.empty:
            selected_row = get_row_by_column_value(all_source_df, "_row_id", selected_document_id)
        render_opportunity_detail_from_row(selected_row)
        return

    @st.fragment
    def _render_opportunity_queue_fragment() -> None:
        # `source_df` is already scoped to current opportunity sheets for the active queue.
        # Re-applying the current mask here can incorrectly drop rows when some source sheets
        # have sparse period/status fields despite belonging to the current sheet.
        base_rows = filter_archived_opportunity_rows(all_source_df) if archive else source_df.copy()
        base_rows = filter_rankable_opportunity_rows(base_rows)
        working = _build_queue_filter_frame(base_rows)
        option_rows = filter_archived_opportunity_rows(all_source_df) if archive else all_source_df
        option_working = _build_queue_filter_frame(option_rows)
        if working.empty and option_working.empty:
            st.info("표시할 RFP가 없습니다.")
            return

        recommendation_options = build_queue_recommendation_options(working["_queue_recommendation"]) if not working.empty else []
        status_options = build_queue_status_options(option_working["_queue_status"]) if not option_working.empty else ["마감"]
        recommendation_widget_key = f"{page_key}_filter_recommendation"
        recommendation_init_key = f"{page_key}_filter_recommendation_initialized"
        if recommendation_widget_key not in st.session_state and not archive:
            st.session_state[recommendation_widget_key] = [
                value for value in recommendation_options if is_positive_recommendation(value)
            ]
        st.session_state[recommendation_init_key] = True
        archive_reason_options = sorted(
            [
                value
                for value in working["_queue_archive_reason"].dropna().astype(str).unique().tolist()
                if clean(value) and value != "-"
            ]
        ) if not working.empty else []

        filter_cols = st.columns(3 if archive else 2)
        with filter_cols[0]:
            selected_recommendation = st.multiselect(
                "추천 상태",
                options=recommendation_options,
                default=st.session_state.get(recommendation_widget_key, []),
                key=recommendation_widget_key,
                placeholder="전체",
            )
        with filter_cols[1]:
            selected_status = st.multiselect(
                "공고 상태",
                options=status_options,
                default=[],
                key=f"{page_key}_filter_status",
                placeholder="전체",
            )

        selected_archive_reason: list[str] = []
        if archive:
            with filter_cols[2]:
                selected_archive_reason = st.multiselect(
                    "보관 사유",
                    options=archive_reason_options,
                    default=[],
                    key=f"{page_key}_filter_archive_reason",
                    placeholder="전체",
                )

        current_filter_signature = (
            tuple(sorted(clean(value) for value in selected_recommendation if clean(value))),
            tuple(sorted(clean(value) for value in selected_status if clean(value))),
            tuple(sorted(clean(value) for value in selected_archive_reason if clean(value))),
        )
        previous_filter_signature = st.session_state.get(filter_signature_key)
        filters_changed = previous_filter_signature != current_filter_signature
        if filters_changed:
            st.session_state[page_state_key] = 1
            st.session_state[filter_signature_key] = current_filter_signature

        include_closed = archive or ("마감" in selected_status)
        filter_source = (
            filter_archived_opportunity_rows(all_source_df)
            if archive
            else (all_source_df if include_closed else source_df.copy())
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

        if filtered.empty:
            st.info("검색 조건에 맞는 RFP가 없습니다.")
            return

        filtered = _sort_opportunity_rows_for_display(filtered)

        total_rows = len(filtered)
        total_pages = max(1, math.ceil(total_rows / page_size)) if page_size else 1
        current_route = route_core.get_current_route()
        query_page_no = safe_int(get_query_param("page_no"), 0)
        legacy_query_page_no = safe_int(get_query_param("rfp_archive_page_no" if archive else "rfp_page_no"), 0)
        if filters_changed:
            current_page = 1
        else:
            current_page = (
                query_page_no
                or legacy_query_page_no
                or int(current_route.get("page_no") or st.session_state.get(page_state_key, 1) or 1)
            )
        current_page = max(1, min(current_page, total_pages))
        st.session_state[page_state_key] = current_page
        start_idx = (current_page - 1) * page_size
        page_rows = filtered.iloc[start_idx:start_idx + page_size].copy()

        st.markdown(
            f'<div class="notice-queue-toolbar-meta">검색 결과 {total_rows:,}건</div>',
            unsafe_allow_html=True,
        )
        _render_rfp_queue_list(page_rows, page_key=page_key)
        pagination_route = route_core.build_rfp_queue_route(
            page_no=current_page,
            page_size=page_size,
            view="list",
            item_id="",
            source_key=clean(current_route.get("source_key")) or "iris",
        )
        pagination_html = _render_rfp_queue_pagination_controls(
            route=pagination_route,
            current_page=current_page,
            total_pages=total_pages,
        )
        st.markdown(pagination_html, unsafe_allow_html=True)

    _render_opportunity_queue_fragment()

# END VIEWER LAYOUT OVERRIDES


def render_notice_queue_ui_styles() -> None:
    st.markdown(
        """
        <style>
        .notice-queue-toolbar-meta {
          margin: 0 0 0.95rem;
          color: #6b7280;
          font-size: 0.83rem;
          font-weight: 700;
        }
        [class*="st-key-notice_queue_workspace_search"],
        [class*="st-key-notice_queue_workspace_status_inline"],
        [class*="st-key-notice_queue_workspace_recommendation"],
        [class*="st-key-notice_queue_workspace_sort"],
        [class*="st-key-notice_queue_reset_top"] {
          margin: 0;
          overflow: visible !important;
        }
        [class*="st-key-notice_queue_workspace_search"] [data-testid="stTextInput"],
        [class*="st-key-notice_queue_workspace_search"] [data-testid="stTextInput"] > div,
        [class*="st-key-notice_queue_workspace_search"] [data-testid="stTextInputRootElement"] {
          background: transparent !important;
          border: none !important;
          box-shadow: none !important;
          overflow: visible !important;
        }
        [class*="st-key-notice_queue_workspace_search"] [data-testid="stTextInputRootElement"] > div,
        [class*="st-key-notice_queue_workspace_status_inline"] [data-baseweb="select"] > div,
        [class*="st-key-notice_queue_workspace_recommendation"] [data-baseweb="select"] > div,
        [class*="st-key-notice_queue_workspace_sort"] [data-baseweb="select"] > div {
          min-height: 54px;
          height: auto !important;
          border: 1px solid #cfd8e3 !important;
          border-radius: 999px !important;
          background: #fbfdff !important;
          color: #0f172a !important;
          font-size: 0.92rem !important;
          font-weight: 500 !important;
          box-shadow: none !important;
          transition: border-color 0.16s ease, box-shadow 0.16s ease, background-color 0.16s ease;
        }
        [class*="st-key-notice_queue_workspace_search"] [data-testid="stTextInputRootElement"][data-baseweb="input"] {
          background: transparent !important;
          border: none !important;
          box-shadow: none !important;
          padding: 0 !important;
          overflow: visible !important;
        }
        [class*="st-key-notice_queue_workspace_search"] [data-testid="stTextInputRootElement"] [data-baseweb="base-input"] {
          display: flex !important;
          align-items: center !important;
          min-height: 54px;
          height: auto !important;
          border: 1px solid #cfd8e3 !important;
          border-radius: 999px !important;
          background: #fbfdff !important;
          box-shadow: none !important;
          padding-top: 0 !important;
          padding-bottom: 0 !important;
          overflow: visible !important;
        }
        [class*="st-key-notice_queue_workspace_status_inline"] [data-baseweb="select"] > div,
        [class*="st-key-notice_queue_workspace_recommendation"] [data-baseweb="select"] > div,
        [class*="st-key-notice_queue_workspace_sort"] [data-baseweb="select"] > div {
          display: flex !important;
          align-items: center !important;
          justify-content: flex-start !important;
          padding: 0 1rem !important;
        }
        [class*="st-key-notice_queue_workspace_status_inline"] [data-baseweb="select"] > div > div,
        [class*="st-key-notice_queue_workspace_recommendation"] [data-baseweb="select"] > div > div,
        [class*="st-key-notice_queue_workspace_sort"] [data-baseweb="select"] > div > div {
          display: flex !important;
          align-items: center !important;
          min-height: 52px !important;
          height: auto !important;
          padding-top: 0 !important;
          padding-bottom: 0 !important;
        }
        [class*="st-key-notice_queue_workspace_search"] input {
          display: block !important;
          min-height: 52px !important;
          height: auto !important;
          padding: 0 1.4rem !important;
          background: transparent !important;
          box-shadow: none !important;
          color: #0f172a !important;
          font-size: 0.98rem !important;
          font-weight: 500 !important;
          line-height: 1.35 !important;
          align-self: center !important;
        }
        [class*="st-key-notice_queue_workspace_search"] input::placeholder {
          color: #9aa8ba !important;
          font-size: 0.98rem !important;
          font-weight: 400 !important;
          line-height: 1.35 !important;
        }
        [class*="st-key-notice_queue_workspace_status_inline"] input,
        [class*="st-key-notice_queue_workspace_recommendation"] input,
        [class*="st-key-notice_queue_workspace_sort"] input {
          display: block !important;
          min-height: 0 !important;
          height: 1.4rem !important;
          padding: 0 !important;
          margin: 0 !important;
          align-self: center !important;
          text-align: left !important;
          color: #0f172a !important;
          font-size: 0.98rem !important;
          font-weight: 600 !important;
          line-height: 1.35 !important;
          opacity: 1 !important;
        }
        [class*="st-key-notice_queue_workspace_status_inline"] input::placeholder,
        [class*="st-key-notice_queue_workspace_recommendation"] input::placeholder,
        [class*="st-key-notice_queue_workspace_sort"] input::placeholder {
          color: #64748b !important;
          font-size: 0.98rem !important;
          font-weight: 600 !important;
          line-height: 1.35 !important;
          opacity: 1 !important;
        }
        [class*="st-key-notice_queue_workspace_status_inline"] [data-baseweb="select"] span,
        [class*="st-key-notice_queue_workspace_recommendation"] [data-baseweb="select"] span,
        [class*="st-key-notice_queue_workspace_sort"] [data-baseweb="select"] span {
          display: block !important;
          line-height: 1.35 !important;
          padding-top: 0 !important;
          padding-bottom: 0 !important;
          color: #0f172a !important;
          font-size: 0.98rem !important;
          font-weight: 600 !important;
        }
        [class*="st-key-notice_queue_workspace_status_inline"] [data-baseweb="select"] > div:hover,
        [class*="st-key-notice_queue_workspace_recommendation"] [data-baseweb="select"] > div:hover,
        [class*="st-key-notice_queue_workspace_sort"] [data-baseweb="select"] > div:hover,
        [class*="st-key-notice_queue_workspace_search"] [data-testid="stTextInputRootElement"] [data-baseweb="base-input"]:hover {
          border-color: #b9c6d8 !important;
          background: #ffffff !important;
        }
        [class*="st-key-notice_queue_workspace_status_inline"] [data-baseweb="select"] > div:focus-within,
        [class*="st-key-notice_queue_workspace_recommendation"] [data-baseweb="select"] > div:focus-within,
        [class*="st-key-notice_queue_workspace_sort"] [data-baseweb="select"] > div:focus-within,
        [class*="st-key-notice_queue_workspace_search"] [data-testid="stTextInputRootElement"] [data-baseweb="base-input"]:focus-within {
          border-color: #7fb1f9 !important;
          background: #ffffff !important;
          box-shadow: 0 0 0 3px rgba(37, 99, 235, 0.1) !important;
        }
        [class*="st-key-notice_queue_workspace_status_inline"] [data-baseweb="tag"],
        [class*="st-key-notice_queue_workspace_recommendation"] [data-baseweb="tag"] {
          margin-top: 0 !important;
          margin-bottom: 0 !important;
        }
        [class*="st-key-notice_queue_reset_top"] button {
          min-height: 44px;
          height: 44px !important;
          border: 1px solid #cfd8e3 !important;
          border-radius: 999px !important;
          background: #ffffff !important;
          color: #475569 !important;
          font-size: 0.88rem !important;
          font-weight: 600 !important;
          text-align: center !important;
          box-shadow: none !important;
          padding: 0 0.95rem !important;
          transition: border-color 0.16s ease, box-shadow 0.16s ease, background-color 0.16s ease, color 0.16s ease;
        }
        [class*="st-key-favorites_toolbar_shell"] {
          margin: 0.15rem 0 0.85rem;
        }
        [class*="st-key-favorites_toolbar_shell"] [data-testid="stHorizontalBlock"] {
          align-items: center;
          gap: 0.75rem;
        }
        [class*="st-key-favorites_workspace_type"],
        [class*="st-key-favorites_workspace_sort"],
        [class*="st-key-favorites_workspace_search"],
        [class*="st-key-favorites_workspace_reset"] {
          margin: 0;
          overflow: visible !important;
        }
        [class*="st-key-favorites_workspace_search"] [data-testid="stTextInput"],
        [class*="st-key-favorites_workspace_search"] [data-testid="stTextInput"] > div,
        [class*="st-key-favorites_workspace_search"] [data-testid="stTextInputRootElement"] {
          background: transparent !important;
          border: none !important;
          box-shadow: none !important;
          overflow: visible !important;
        }
        [class*="st-key-favorites_workspace_type"] [data-baseweb="select"] > div,
        [class*="st-key-favorites_workspace_sort"] [data-baseweb="select"] > div,
        [class*="st-key-favorites_workspace_search"] [data-testid="stTextInputRootElement"] > div,
        [class*="st-key-favorites_workspace_search"] [data-testid="stTextInputRootElement"] [data-baseweb="base-input"] {
          min-height: 54px;
          height: auto !important;
          border: 1px solid #cfd8e3 !important;
          border-radius: 999px !important;
          background: #fbfdff !important;
          color: #0f172a !important;
          font-size: 0.98rem !important;
          font-weight: 600 !important;
          box-shadow: none !important;
          transition: border-color 0.16s ease, box-shadow 0.16s ease, background-color 0.16s ease;
          overflow: visible !important;
        }
        [class*="st-key-favorites_workspace_type"] [data-baseweb="select"] > div,
        [class*="st-key-favorites_workspace_sort"] [data-baseweb="select"] > div {
          display: flex !important;
          align-items: center !important;
          justify-content: flex-start !important;
          padding: 0 1rem !important;
        }
        [class*="st-key-favorites_workspace_type"] [data-baseweb="select"] > div > div,
        [class*="st-key-favorites_workspace_sort"] [data-baseweb="select"] > div > div {
          display: flex !important;
          align-items: center !important;
          min-height: 52px !important;
          height: auto !important;
          padding-top: 0 !important;
          padding-bottom: 0 !important;
        }
        [class*="st-key-favorites_workspace_search"] [data-testid="stTextInputRootElement"][data-baseweb="input"] {
          background: transparent !important;
          border: none !important;
          box-shadow: none !important;
          padding: 0 !important;
          overflow: visible !important;
        }
        [class*="st-key-favorites_workspace_type"] [data-baseweb="select"] > div:hover,
        [class*="st-key-favorites_workspace_sort"] [data-baseweb="select"] > div:hover,
        [class*="st-key-favorites_workspace_search"] [data-testid="stTextInputRootElement"] [data-baseweb="base-input"]:hover {
          border-color: #b9c6d8 !important;
          background: #ffffff !important;
        }
        [class*="st-key-favorites_workspace_type"] [data-baseweb="select"] > div:focus-within,
        [class*="st-key-favorites_workspace_sort"] [data-baseweb="select"] > div:focus-within,
        [class*="st-key-favorites_workspace_search"] [data-testid="stTextInputRootElement"] [data-baseweb="base-input"]:focus-within {
          border-color: #7fb1f9 !important;
          background: #ffffff !important;
          box-shadow: 0 0 0 3px rgba(37, 99, 235, 0.1) !important;
        }
        [class*="st-key-favorites_workspace_search"] input {
          display: block !important;
          min-height: 52px !important;
          height: auto !important;
          padding: 0 1.4rem !important;
          background: transparent !important;
          box-shadow: none !important;
          color: #0f172a !important;
          font-size: 0.98rem !important;
          font-weight: 500 !important;
          line-height: 1.35 !important;
          align-self: center !important;
        }
        [class*="st-key-favorites_workspace_search"] input::placeholder {
          color: #9aa8ba !important;
          font-size: 0.98rem !important;
          font-weight: 400 !important;
          line-height: 1.35 !important;
        }
        [class*="st-key-favorites_workspace_type"] input,
        [class*="st-key-favorites_workspace_sort"] input {
          display: block !important;
          min-height: 0 !important;
          height: 1.4rem !important;
          padding: 0 !important;
          margin: 0 !important;
          align-self: center !important;
          text-align: left !important;
          color: #0f172a !important;
          font-size: 0.98rem !important;
          font-weight: 600 !important;
          line-height: 1.35 !important;
          opacity: 1 !important;
        }
        [class*="st-key-favorites_workspace_type"] input::placeholder,
        [class*="st-key-favorites_workspace_sort"] input::placeholder {
          color: #64748b !important;
          font-size: 0.98rem !important;
          font-weight: 600 !important;
          line-height: 1.35 !important;
          opacity: 1 !important;
        }
        [class*="st-key-favorites_workspace_type"] [data-baseweb="select"] span,
        [class*="st-key-favorites_workspace_sort"] [data-baseweb="select"] span {
          display: block !important;
          line-height: 1.35 !important;
          padding-top: 0 !important;
          padding-bottom: 0 !important;
          color: #0f172a !important;
          font-size: 0.98rem !important;
          font-weight: 600 !important;
        }
        [class*="st-key-favorites_workspace_reset"] button {
          min-height: 40px;
          height: 40px !important;
          border: 1px solid #cfd8e3 !important;
          border-radius: 999px !important;
          background: #ffffff !important;
          color: #475569 !important;
          font-size: 0.84rem !important;
          font-weight: 600 !important;
          text-align: center !important;
          box-shadow: none !important;
          padding: 0 0.8rem !important;
          transition: border-color 0.16s ease, box-shadow 0.16s ease, background-color 0.16s ease, color 0.16s ease;
        }
        .notice-queue-summary-wrap {
          margin: 0 0 1rem;
        }
        .queue-grid-scroll {
          width: 100%;
          overflow-x: auto;
        }
        .favorites-grid-scroll {
          width: 100%;
          overflow-x: auto;
        }
        .queue-grid {
          min-width: 1280px;
          border: 1px solid #e5e7eb;
          border-radius: 18px;
          background: #ffffff;
          overflow: hidden;
        }
        .favorites-grid {
          min-width: 1340px;
          border: 1px solid #e5e7eb;
          border-radius: 16px;
          background: #ffffff;
          overflow: hidden;
        }
        .queue-grid-head,
        .queue-grid-row {
          display: grid;
          grid-template-columns: 96px minmax(320px, 1.55fr) minmax(220px, 1fr) 156px 184px 88px 72px 80px;
          gap: 0.8rem;
          align-items: center;
          padding: 1rem 1.05rem;
        }
        .queue-grid-head {
          background: #fbfcfe;
          border-bottom: 1px solid #e7edf5;
          color: #5f7189;
          font-size: 0.92rem;
          font-weight: 800;
        }
        .queue-grid-head > div {
          display: flex;
          align-items: center;
          justify-content: center;
          text-align: center;
        }
        .queue-grid-row {
          border-top: 1px solid #eef3f8;
          min-height: 88px;
        }
        .queue-grid-row:first-of-type {
          border-top: none;
        }
        .queue-grid-row > .queue-grid-cell:first-child {
          display: flex;
          align-items: center;
          justify-content: center;
        }
        .queue-grid-row.is-selected {
          background: #f8fbff;
        }
        .favorites-grid-head,
        .favorites-grid-row {
          display: grid;
          grid-template-columns: 72px 92px minmax(420px, 3.2fr) 240px 150px 174px 78px 56px 76px;
          gap: 0.8rem;
          align-items: center;
          padding: 0.8rem 0.9rem;
        }
        .favorites-grid-head {
          background: #f8fafc;
          border-bottom: 1px solid #e5e7eb;
          color: #64748b;
          font-size: 0.79rem;
          font-weight: 800;
        }
        .favorites-grid-head > div {
          display: flex;
          align-items: center;
          justify-content: center;
          text-align: center;
        }
        .favorites-grid-row {
          border-top: 1px solid #eef2f7;
          min-height: 78px;
        }
        .favorites-grid-row:first-of-type {
          border-top: none;
        }
        .favorites-grid-row.is-selected {
          background: #f8fbff;
        }
        .favorites-grid-kind {
          display: inline-flex;
          align-items: center;
          justify-content: center;
          min-height: 28px;
          padding: 0 0.72rem;
          border-radius: 999px;
          background: #f3f4f6;
          color: #111827;
          font-size: 0.79rem;
          font-weight: 800;
          white-space: nowrap;
        }
        .favorites-grid-summary {
          color: #6b7280;
          font-size: 0.84rem;
          line-height: 1.42;
          margin-top: 0.22rem;
        }
        .favorites-grid-memo {
          color: #6b7280;
          font-size: 0.84rem;
          line-height: 1.42;
        }
        .favorites-grid-recommend {
          color: #111827;
          font-weight: 700;
        }
        .favorites-grid-empty {
          color: #9ca3af;
          font-size: 0.84rem;
          font-weight: 600;
        }
        .queue-grid-cell {
          min-width: 0;
          display: flex;
          flex-direction: column;
          align-items: center;
          justify-content: center;
          text-align: center;
          color: #111827;
          font-size: 0.95rem;
          line-height: 1.48;
        }
        .queue-grid-cell.is-center {
          text-align: center;
        }
        .queue-grid-head > .queue-grid-col-title {
          justify-content: center;
          text-align: center;
        }
        .queue-grid-cell.is-title {
          align-items: flex-start;
          justify-content: center;
          text-align: left;
          justify-self: start;
          max-width: 100%;
          width: 100%;
          padding-left: 0.4rem;
        }
        .queue-grid-cell.is-keywords {
          align-items: center;
          justify-content: center;
          text-align: center;
          width: 100%;
        }
        .queue-grid-title {
          display: block;
          color: #111827 !important;
          font-size: 1rem;
          font-weight: 800;
          line-height: 1.42;
          text-align: center;
          width: 100%;
          white-space: normal;
          overflow: visible;
          text-decoration: none !important;
          display: -webkit-box;
          -webkit-line-clamp: 2;
          -webkit-box-orient: vertical;
        }
        .queue-grid-cell.is-title .queue-grid-title,
        .queue-grid-cell.is-title .queue-grid-subtitle {
          text-align: left;
        }
        .queue-grid-title:hover {
          color: #111827 !important;
        }
        .queue-grid-subtitle {
          margin-top: 0.38rem;
          color: #94a3b8;
          font-size: 0.8rem;
          line-height: 1.45;
          text-align: center;
          width: 100%;
          white-space: normal;
          overflow: visible;
          display: -webkit-box;
          -webkit-line-clamp: 2;
          -webkit-box-orient: vertical;
        }
        .queue-grid-period {
          line-height: 1.45;
        }
        .queue-grid-dday {
          font-size: 1.02rem;
          font-weight: 800;
        }
        .queue-grid-dday.is-calm {
          color: #111827;
        }
        .queue-grid-dday.is-warning {
          color: #d97706;
        }
        .queue-grid-dday.is-critical {
          color: #dc2626;
        }
        .queue-grid-budget,
        .queue-grid-rfp-count {
          color: #111827;
          font-weight: 850;
        }
        .queue-grid-keywords {
          display: flex;
          flex-wrap: wrap;
          gap: 0.42rem;
          width: 100%;
          max-width: 100%;
          justify-content: center;
        }
        .queue-grid-chip {
          flex: 0 0 auto;
          display: inline-flex;
          align-items: center;
          min-height: 28px;
          padding: 0 0.78rem;
          border-radius: 999px;
          border: 1px solid #edf2f7;
          background: #f8fafc;
          color: #475569;
          white-space: nowrap;
          font-size: 0.78rem;
          font-weight: 700;
        }
        .queue-grid-chip.is-empty {
          border-color: #dbe3ef;
          color: #64748b;
        }
        .queue-grid-select {
          width: 18px;
          height: 18px;
          display: inline-flex;
          align-items: center;
          justify-content: center;
          border: 1.5px solid #cbd5e1;
          border-radius: 5px;
          background: #ffffff;
          color: transparent;
          text-decoration: none !important;
          font-size: 0.72rem;
          font-weight: 900;
        }
        .queue-grid-select.is-selected {
          border-color: #2563eb;
          background: #2563eb;
          color: #ffffff;
        }
        .queue-grid-favorite {
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
        .queue-grid-favorite.is-active {
          border-color: #bfdbfe;
          background: #eff6ff;
          color: #1d4ed8 !important;
        }
        .queue-grid-empty {
          color: #9ca3af;
          font-size: 0.84rem;
          font-weight: 600;
        }
        .favorites-inline-head {
          margin-bottom: 0.35rem;
          color: #64748b;
          font-size: 0.75rem;
          font-weight: 800;
        }
        .favorites-inline-row {
          padding: 0.2rem 0 0.7rem;
          border-bottom: 1px solid #e5e7eb;
        }
        .favorites-inline-cell {
          min-height: 100%;
          display: flex;
          align-items: center;
        }
        .favorites-inline-cell.is-center {
          justify-content: center;
        }
        @media (max-width: 1100px) {
          .queue-grid {
            min-width: 1080px;
          }
          .favorites-grid {
            min-width: 1140px;
          }
        }
        .notice-mailbox {
          display: flex;
          flex-direction: column;
          gap: 12px;
          background: transparent;
          overflow: visible;
        }
        .notice-mailbox-head,
        .notice-mailbox-item {
          display: grid;
          grid-template-columns: minmax(0, 1.55fr) 138px 212px 232px 178px;
          gap: 0.8rem;
          align-items: start;
          padding: 0.85rem 0.95rem;
        }
        .notice-mailbox-head {
          display: none;
        }
        .notice-mailbox-item {
          border: 1px solid #e5e7eb;
          border-radius: 16px;
          background: #ffffff;
          box-shadow: 0 1px 2px rgba(15, 23, 42, 0.03);
        }
        .notice-mailbox-item.is-selected {
          background: #f8fbff;
          border-color: #bfdbfe;
          box-shadow: 0 0 0 1px rgba(37, 99, 235, 0.08);
        }
        .notice-mailbox-main,
        .notice-mailbox-status-col,
        .notice-mailbox-agency-col,
        .notice-mailbox-period-col,
        .notice-mailbox-pick-col {
          color: #111827;
          font-size: 0.88rem;
          line-height: 1.5;
          min-width: 0;
          word-break: break-word;
        }
        .notice-mailbox-status-col,
        .notice-mailbox-agency-col,
        .notice-mailbox-period-col,
        .notice-mailbox-pick-col {
          padding-left: 0.95rem;
          border-left: 1px solid #eef2f7;
          display: flex;
          flex-direction: column;
          justify-content: flex-start;
          gap: 0.55rem;
        }
        .notice-mailbox-title {
          color: #111827 !important;
          font-size: 1.12rem;
          font-weight: 850;
          line-height: 1.4;
          text-decoration: none !important;
        }
        .notice-mailbox-title:hover {
          color: #111827 !important;
        }
        .notice-mailbox-subtitle {
          margin-top: 0.2rem;
          color: #6b7280;
          font-size: 0.88rem;
          line-height: 1.4;
        }
        .notice-mailbox-summary {
          margin-top: 0.4rem;
          color: #475569;
          font-size: 0.9rem;
          line-height: 1.5;
        }
        .notice-mailbox-inline-pills {
          display: flex;
          flex-wrap: wrap;
          gap: 0.4rem;
          margin-top: 0.72rem;
        }
        .notice-mailbox-inline-pill {
          display: inline-flex;
          align-items: center;
          min-height: 24px;
          padding: 0 0.58rem;
          border-radius: 999px;
          border: 1px solid #e5e7eb;
          background: #f8fafc;
          color: #475569;
          font-size: 0.73rem;
          font-weight: 700;
          white-space: nowrap;
        }
        .notice-mailbox-keywords {
          display: flex;
          flex-wrap: wrap;
          gap: 0.35rem;
          margin-top: 0.72rem;
        }
        .notice-mailbox-keyword {
          display: inline-flex;
          align-items: center;
          min-height: 24px;
          padding: 0 0.58rem;
          border-radius: 999px;
          background: #f3f4f6;
          color: #374151;
          font-size: 0.73rem;
          font-weight: 700;
          white-space: nowrap;
        }
        .notice-mailbox-meta-inline {
          margin-top: 0.5rem;
          color: #64748b;
          font-size: 0.87rem;
          line-height: 1.4;
        }
        .notice-mailbox-status {
          display: inline-flex;
          align-items: center;
          justify-content: center;
          min-height: 32px;
          padding: 0 0.82rem;
          border-radius: 999px;
          border: 1px solid transparent;
          font-size: 0.82rem;
          font-weight: 800;
          letter-spacing: -0.01em;
          white-space: nowrap;
        }
        .notice-mailbox-status.is-open {
          background: #dcfce7;
          border-color: #86efac;
          color: #15803d;
        }
        .notice-mailbox-status.is-pending {
          background: #eff6ff;
          border-color: #bfdbfe;
          color: #2563eb;
        }
        .notice-mailbox-status.is-deadline,
        .notice-mailbox-status.is-closed {
          background: #fee2e2;
          border-color: #fca5a5;
          color: #dc2626;
        }
        .notice-mailbox-side-block {
          padding: 0.72rem 0.8rem;
          border: 1px solid #eef2f7;
          border-radius: 12px;
          background: #fbfdff;
        }
        .notice-mailbox-dday {
          display: inline-flex;
          align-items: center;
          justify-content: center;
          min-width: 56px;
          min-height: 28px;
          padding: 0 0.6rem;
          border-radius: 999px;
          font-size: 0.74rem;
          font-weight: 700;
          color: #ffffff;
          background: #2563eb;
          margin-top: 0.55rem;
        }
        .notice-mailbox-dday.is-critical {
          background: #cb0000;
        }
        .notice-mailbox-dday.is-warning {
          background: #f89321;
        }
        .notice-mailbox-dday.is-calm {
          background: #2563eb;
        }
        .notice-mailbox-meta-label {
          color: #94a3b8;
          font-size: 0.72rem;
          font-weight: 800;
          text-transform: uppercase;
          letter-spacing: 0.05em;
          margin-bottom: 0.18rem;
        }
        .notice-mailbox-meta-value {
          color: #111827;
          font-size: 0.9rem;
          font-weight: 700;
          line-height: 1.4;
        }
        .notice-mailbox-side-row {
          display: grid;
          grid-template-columns: 1fr 1fr;
          gap: 0.5rem;
        }
        .notice-mailbox-side-empty {
          color: #9ca3af;
          font-size: 0.8rem;
          font-weight: 600;
          line-height: 1.4;
        }
        .notice-mailbox-pick-col {
          justify-content: center;
        }
        .notice-mailbox-pick-hint {
          color: #94a3b8;
          font-size: 0.72rem;
          font-weight: 700;
        }
        .notice-mailbox-pick-link {
          display: inline-flex;
          align-items: center;
          justify-content: center;
          min-height: 38px;
          padding: 0 0.78rem;
          border-radius: 999px;
          border: 1px solid #cbd5e1;
          background: #ffffff;
          color: #111827 !important;
          font-size: 0.84rem;
          font-weight: 800;
          text-decoration: none !important;
          text-align: center;
          transition: border-color 140ms ease, background-color 140ms ease, color 140ms ease;
          width: 100%;
        }
        .notice-mailbox-pick-link.is-active {
          border-color: #86efac;
          background: #dcfce7;
          color: #15803d !important;
        }
        .notice-mailbox-empty {
          padding: 1.2rem;
          color: #6b7280;
          font-size: 0.88rem;
        }
        .queue-pagination-wrap {
          display: flex;
          flex-direction: column;
          align-items: center;
          gap: 0.55rem;
          margin-top: 1.05rem;
        }
        .queue-pagination-row {
          display: inline-flex;
          align-items: center;
          justify-content: center;
          gap: 0.7rem;
        }
        .queue-page-nav {
          display: inline-flex;
          align-items: center;
          justify-content: center;
          gap: 2.1rem;
          color: #4b5563 !important;
          font-size: 1rem;
          font-weight: 800;
          text-decoration: none !important;
        }
        .queue-page-nav.is-disabled {
          color: #cbd5e1 !important;
          pointer-events: none;
        }
        .queue-page-link {
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
          text-decoration: none !important;
        }
        .queue-page-link.is-active {
          border-color: #2563eb;
          background: #2563eb;
          color: #ffffff !important;
        }
        .queue-page-link:hover {
          border-color: #93c5fd;
        }
        .queue-page-ellipsis {
          display: inline-flex;
          align-items: center;
          justify-content: center;
          min-width: 34px;
          height: 38px;
          color: #4b5563;
          font-size: 1.05rem;
          font-weight: 900;
        }
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
          gap: 0.55rem;
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
          gap: 2.1rem;
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
        .notice-queue-page-link.is-disabled {
          color: #cbd5e1 !important;
          pointer-events: none;
        }
        .notice-queue-page-link:hover {
          border-color: #93c5fd;
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
        html, body, [class*="css"], .stApp,
        [data-testid="stAppViewContainer"],
        [data-testid="stAppViewBlockContainer"],
        [data-testid="stHeader"] {
          background: #ffffff !important;
        }
        .app-shell {
          min-height: 108px;
          display: grid;
          grid-template-columns: auto minmax(320px, 1fr) auto;
          align-items: center;
          gap: 1.85rem;
          margin: -0.15rem 0 0.9rem;
          padding: 0;
          background: #ffffff;
          border-bottom: 1px solid #e5e7eb;
        }
        .app-brand {
          display: flex;
          align-items: center;
          gap: 0;
          color: #111827;
          font-size: 1.28rem;
          font-weight: 700;
          white-space: nowrap;
        }
        .app-brand-copy {
          display: block;
        }
        .app-brand-title {
          color: #111827;
          font-size: 1.5rem;
          font-weight: 700;
          font-family: "Pretendard", "Noto Sans KR", "Apple SD Gothic Neo", "Segoe UI", sans-serif;
          line-height: 1.15;
          letter-spacing: -0.05em;
        }
        .app-nav {
          display: flex;
          align-items: center;
          height: 100%;
          gap: 2.55rem;
          min-width: 0;
          overflow-x: auto;
          overflow-y: hidden;
          scrollbar-width: none;
        }
        .app-nav::-webkit-scrollbar {
          display: none;
        }
        .app-nav-item {
          display: inline-flex;
          align-items: center;
          height: 108px;
          color: #1f2937;
          border-bottom: 3px solid transparent;
          font-size: 1.38rem;
          font-weight: 700;
          font-family: "Pretendard", "Noto Sans KR", "Apple SD Gothic Neo", "Segoe UI", sans-serif;
          line-height: 1;
          letter-spacing: -0.035em;
          text-decoration: none !important;
          white-space: nowrap;
          flex: 0 0 auto;
        }
        .app-nav-item-active {
          color: #2563eb;
          border-bottom-color: #2563eb;
          font-weight: 700;
        }
        .app-nav-item:hover {
          color: #1f2937;
        }
        .app-actions {
          display: flex;
          align-items: center;
          justify-content: flex-end;
          gap: 0.55rem;
          min-width: 0;
        }
        .app-user-link,
        .app-user-menu,
        .app-user-chip {
          min-height: 46px;
          display: inline-flex;
          align-items: center;
          justify-content: center;
          padding: 0 1rem;
          color: #374151;
          background: #ffffff;
          border: 1px solid #e5e7eb;
          border-radius: 12px;
          font-size: 0.94rem;
          font-weight: 700;
          text-decoration: none !important;
          white-space: nowrap;
          box-shadow: none;
        }
        .app-user-chip {
          color: #374151;
        }
        .app-user-link {
          color: #374151 !important;
        }
        .app-user-menu {
          align-items: flex-start;
          flex-direction: column;
          gap: 0.16rem;
          min-width: 118px;
        }
        .app-user-name {
          color: #111827;
          font-size: 0.94rem;
          font-weight: 800;
          line-height: 1.1;
        }
        .app-user-role {
          color: #6b7280;
          font-size: 0.78rem;
          font-weight: 600;
          line-height: 1.1;
        }
        @media (max-width: 900px) {
          .app-shell {
            grid-template-columns: 1fr;
            align-items: flex-start;
            gap: 0.45rem;
          }
          .app-actions {
            justify-content: flex-start;
            flex-wrap: wrap;
          }
          .app-nav {
            width: 100%;
          }
        }
        </style>
        """,
        unsafe_allow_html=True,
    )


def render_public_workspace_navigation(mode_config: AppModeConfig, current_source: str, current_page: str) -> None:
    _inject_public_workspace_shell_styles()
    nav_items = [
        (item.label, item.source_key, item.page_key)
        for group in mode_config.nav_groups
        if group.key == "workspace"
        for item in group.items
    ]
    if not nav_items:
        nav_items = [
            (item.label, item.source_key, item.page_key)
            for group in mode_config.nav_groups
            for item in group.items
        ]
    nav_links: list[str] = []
    for label, source_key, page_key in nav_items:
        active_class = " app-nav-item-active" if current_source == source_key and current_page == page_key else ""
        href = _build_public_workspace_route_href(source_key, page_key)
        nav_links.append(
            f'<a class="app-nav-item{active_class}" href="{escape(href, quote=True)}" target="_self">{escape(label)}</a>'
        )

    logout_params = with_auth_params(get_query_params_dict())
    logout_params["logout"] = "1"
    logout_href = f"?{urlencode(logout_params)}"
    affiliation_label = escape(get_current_user_affiliation() or get_current_user_domain() or "Researcher")
    user_label = escape(get_current_user_label() or get_current_user_id() or "User")
    st.markdown(
        (
            '<div class="app-shell">'
            '<div class="app-brand">'
            '<span class="app-brand-copy">'
            '<span class="app-brand-title">R&amp;D Opportunity</span>'
            '</span>'
            '</div>'
            f'<nav class="app-nav">{"".join(nav_links)}</nav>'
            '<div class="app-actions">'
            f'<div class="app-user-chip app-user-affiliation-chip">{affiliation_label}</div>'
            f'<div class="app-user-chip app-user-name-chip">{user_label}</div>'
            f'<a class="app-user-link app-user-link-logout" href="{escape(logout_href, quote=True)}" target="_self">로그아웃</a>'
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
          max-width: 1360px;
          padding-left: 28px;
          padding-right: 28px;
          padding-top: 0.2rem;
        }
        .app-shell {
          min-height: 94px;
          gap: 1.55rem;
          margin: -0.1rem 0 0.14rem;
          grid-template-columns: auto minmax(320px, 1fr) auto;
        }
        .dashboard-section,
        .queue-table-card,
        .summary-panel {
          border-radius: 20px;
          padding: 1.08rem 1.15rem;
        }
        .dashboard-kpi-grid {
          display: grid;
          grid-template-columns: repeat(3, minmax(0, 1fr));
          gap: 1rem;
          margin: 0.35rem 0 1.25rem;
        }
        .dashboard-kpi-card {
          position: relative;
          display: block;
          min-height: 108px;
          padding: 1.02rem 1.08rem;
          background: linear-gradient(180deg, #ffffff 0%, #fbfdff 100%);
          border: 1px solid #dbe4f0;
          border-radius: 20px;
          color: #15233b;
          text-decoration: none !important;
          box-shadow: 0 10px 24px rgba(15, 23, 42, 0.04);
          transition: transform 0.16s ease, border-color 0.16s ease, box-shadow 0.16s ease, background-color 0.16s ease;
        }
        .dashboard-kpi-card:hover {
          transform: translateY(-1px);
          border-color: rgba(15, 23, 42, 0.18);
          background: #fbfdff;
          box-shadow: 0 14px 30px rgba(15, 23, 42, 0.06);
        }
        .dashboard-kpi-label {
          font-size: 0.86rem;
          margin-bottom: 0.48rem;
          color: #6c7f9d;
          font-weight: 700;
        }
        .dashboard-kpi-value {
          font-size: 2rem;
          color: #15233b;
          font-weight: 800;
        }
        .dashboard-kpi-caption {
          font-size: 0.82rem;
          color: #94a3b8;
          font-weight: 600;
        }
        .dashboard-kpi-symbol {
          position: absolute;
          top: 1rem;
          right: 1rem;
          width: 56px;
          height: 46px;
          display: inline-flex;
          align-items: center;
          justify-content: center;
          border-radius: 12px;
          font-size: 0.9rem;
          font-weight: 800;
          border: 1px solid #dbe4f0;
          background: #f8fbff;
          color: #2563eb;
        }
        .dashboard-kpi-card.is-blue .dashboard-kpi-symbol {
          background: #eef4ff;
          border-color: #c7d8ff;
          color: #2563eb;
        }
        .dashboard-kpi-card.is-red .dashboard-kpi-symbol {
          background: #fff3f2;
          border-color: #fecaca;
          color: #dc2626;
        }
        .dashboard-kpi-card.is-green .dashboard-kpi-symbol {
          background: #eefbf3;
          border-color: #ccebd7;
          color: #0f9f6e;
        }
        .dashboard-section {
          margin-top: 1.2rem;
          background: #ffffff;
          border: 1px solid #dbe4f0;
          box-shadow: 0 10px 24px rgba(15, 23, 42, 0.04);
        }
        .dashboard-section + .dashboard-section {
          margin-top: 1.28rem;
        }
        .dashboard-section-header {
          display: flex;
          align-items: center;
          justify-content: space-between;
          gap: 0.75rem;
          margin-bottom: 1.05rem;
          padding-bottom: 0.95rem;
          border-bottom: 1px solid rgba(15, 23, 42, 0.06);
        }
        .dashboard-section-title {
          color: #15233b;
          font-size: 1.02rem;
          font-weight: 800;
          letter-spacing: -0.02em;
        }
        .dashboard-section-title::before {
          display: none;
        }
        .dashboard-section-link {
          color: #2563eb !important;
          font-size: 0.82rem;
          font-weight: 700;
          text-decoration: none !important;
        }
        .dashboard-carousel-note {
          margin: -0.05rem 0 1.05rem;
          color: #6c7f9d;
          font-size: 0.82rem;
          font-weight: 600;
        }
        [class*="st-key-dashboard_search_shell"],
        [class*="st-key-public_dashboard_search_shell"] {
          margin: 0 0 1.15rem;
          padding: 0.14rem 0 0.92rem;
          border-bottom: 1px solid rgba(15, 23, 42, 0.06);
        }
        [class*="st-key-dashboard_compact_search_text"],
        [class*="st-key-public_dashboard_compact_search_text"] {
          margin: 0;
          overflow: visible !important;
        }
        [class*="st-key-dashboard_compact_search_text"] [data-testid="stTextInput"],
        [class*="st-key-dashboard_compact_search_text"] [data-testid="stTextInput"] > div,
        [class*="st-key-dashboard_compact_search_text"] [data-testid="stTextInputRootElement"],
        [class*="st-key-public_dashboard_compact_search_text"] [data-testid="stTextInput"],
        [class*="st-key-public_dashboard_compact_search_text"] [data-testid="stTextInput"] > div,
        [class*="st-key-public_dashboard_compact_search_text"] [data-testid="stTextInputRootElement"] {
          background: transparent !important;
          border: none !important;
          box-shadow: none !important;
          overflow: visible !important;
        }
        [class*="st-key-dashboard_compact_search_text"] [data-testid="stTextInputRootElement"][data-baseweb="input"],
        [class*="st-key-public_dashboard_compact_search_text"] [data-testid="stTextInputRootElement"][data-baseweb="input"] {
          background: transparent !important;
          border: none !important;
          box-shadow: none !important;
          padding: 0 !important;
          overflow: visible !important;
        }
        [class*="st-key-dashboard_compact_search_text"] [data-testid="stTextInputRootElement"] [data-baseweb="base-input"],
        [class*="st-key-public_dashboard_compact_search_text"] [data-testid="stTextInputRootElement"] [data-baseweb="base-input"] {
          display: flex !important;
          align-items: center !important;
          min-height: 54px;
          height: auto !important;
          border: 1px solid #dbe4f0 !important;
          border-radius: 999px !important;
          background: #fbfdff !important;
          box-shadow: none !important;
          padding-top: 0 !important;
          padding-bottom: 0 !important;
          overflow: visible !important;
          transition: border-color 0.16s ease, box-shadow 0.16s ease, background-color 0.16s ease;
        }
        [class*="st-key-dashboard_compact_search_text"] [data-testid="stTextInputRootElement"] [data-baseweb="base-input"]:hover,
        [class*="st-key-public_dashboard_compact_search_text"] [data-testid="stTextInputRootElement"] [data-baseweb="base-input"]:hover {
          border-color: rgba(15, 23, 42, 0.14) !important;
          background: #ffffff !important;
        }
        [class*="st-key-dashboard_compact_search_text"] [data-testid="stTextInputRootElement"] [data-baseweb="base-input"]:focus-within,
        [class*="st-key-public_dashboard_compact_search_text"] [data-testid="stTextInputRootElement"] [data-baseweb="base-input"]:focus-within {
          border-color: #93c5fd !important;
          background: #ffffff !important;
          box-shadow: 0 0 0 4px rgba(37, 99, 235, 0.08) !important;
        }
        [class*="st-key-dashboard_compact_search_text"] input,
        [class*="st-key-public_dashboard_compact_search_text"] input {
          display: block !important;
          min-height: 52px !important;
          height: auto !important;
          padding: 0 1.35rem !important;
          background: transparent !important;
          box-shadow: none !important;
          color: #15233b !important;
          font-size: 0.96rem !important;
          font-weight: 600 !important;
          line-height: 1.35 !important;
          align-self: center !important;
        }
        [class*="st-key-dashboard_compact_search_text"] input::placeholder,
        [class*="st-key-public_dashboard_compact_search_text"] input::placeholder {
          color: #94a3b8 !important;
          font-size: 0.96rem !important;
          font-weight: 500 !important;
          line-height: 1.35 !important;
        }
        .rfp-card-row {
          display: flex;
          gap: 1rem;
          overflow-x: auto;
          overflow-y: hidden;
          padding: 0.05rem 0.05rem 0.55rem;
          scroll-snap-type: x proximity;
          scrollbar-width: thin;
          scrollbar-color: #cbd5e1 #f8fafc;
        }
        .rfp-card-row::-webkit-scrollbar {
          height: 10px;
        }
        .rfp-card-row::-webkit-scrollbar-track {
          background: #f8fafc;
          border-radius: 999px;
        }
        .rfp-card-row::-webkit-scrollbar-thumb {
          background: #cbd5e1;
          border-radius: 999px;
        }
        .rfp-card {
          flex: 0 0 360px;
          min-height: 236px;
          padding: 1.15rem 1.2rem;
          display: flex;
          flex-direction: column;
          background: #ffffff;
          border: 1px solid #dbe4f0;
          border-radius: 20px;
          box-shadow: 0 10px 24px rgba(15, 23, 42, 0.04);
          text-decoration: none !important;
          color: #15233b;
          scroll-snap-align: start;
          transition: transform 0.16s ease, box-shadow 0.16s ease, border-color 0.16s ease;
        }
        .rfp-card:hover {
          transform: translateY(-1px);
          border-color: rgba(15, 23, 42, 0.18);
          background: #fbfdff;
          box-shadow: 0 14px 30px rgba(15, 23, 42, 0.06);
        }
        .rfp-card-title {
          color: #15233b;
          font-size: 1.04rem;
          font-weight: 800;
          line-height: 1.4;
          margin-bottom: 0.45rem;
        }
        .rfp-card-notice,
        .rfp-card-agency,
        .rfp-card-meta,
        .notice-row-meta,
        .notice-row-summary {
          font-size: 0.8rem;
          color: #6c7f9d;
        }
        .rfp-card-topline,
        .rfp-card-topline-left,
        .rfp-card-meta {
          display: flex;
          align-items: center;
        }
        .rfp-card-topline {
          justify-content: space-between;
          gap: 0.7rem;
          margin-bottom: 0.6rem;
        }
        .rfp-card-topline-left {
          gap: 0.4rem;
        }
        .rfp-card-rank {
          display: inline-flex;
          align-items: center;
          justify-content: center;
          min-width: 28px;
          height: 28px;
          padding: 0 0.5rem;
          border-radius: 8px;
          background: #2563eb;
          color: #ffffff;
          font-size: 0.75rem;
          font-weight: 800;
        }
        .dashboard-pill {
          display: inline-flex;
          align-items: center;
          justify-content: center;
          min-height: 24px;
          padding: 0 0.6rem;
          border-radius: 999px;
          font-size: 0.7rem;
          font-weight: 700;
          border: 1px solid transparent;
        }
        .dashboard-pill.is-recommend {
          background: #eef4ff;
          border-color: #c7d8ff;
          color: #2563eb;
        }
        .dashboard-pill.is-review {
          background: #fff7ed;
          border-color: #fed7aa;
          color: #d97706;
        }
        .rfp-card-score {
          font-size: 0.98rem;
          color: #2563eb;
          font-weight: 800;
        }
        .rfp-card-keywords {
          display: flex;
          flex-wrap: wrap;
          gap: 0.35rem;
          margin-top: 0.75rem;
        }
        .rfp-card-keyword {
          display: inline-flex;
          align-items: center;
          font-size: 0.72rem;
          padding: 0.22rem 0.56rem;
          border-radius: 999px;
          border: 1px solid rgba(15, 23, 42, 0.06);
          background: rgba(15, 23, 42, 0.03);
          color: #21314d;
          font-weight: 700;
        }
        .rfp-card-agency {
          margin: 0.7rem 0 0.75rem;
          color: #21314d;
          font-weight: 650;
        }
        .rfp-card-meta {
          margin-top: auto;
          justify-content: space-between;
          gap: 0.5rem;
          border-top: 1px solid rgba(15, 23, 42, 0.06);
          padding-top: 0.7rem;
        }
        .dashboard-search-meta {
          color: #6c7f9d;
          font-size: 0.78rem;
          font-weight: 700;
          display: flex;
          align-items: center;
          justify-content: flex-end;
          min-height: 52px;
          min-width: 184px;
          white-space: nowrap;
          text-align: right;
        }
        .dashboard-bottom-grid {
          display: grid;
          grid-template-columns: repeat(2, minmax(0, 1fr));
          gap: 1rem;
          margin-top: 1.2rem;
        }
        .dashboard-list-panel {
          min-height: 320px;
        }
        .dashboard-list {
          display: flex;
          flex-direction: column;
          gap: 0.88rem;
        }
        .dashboard-list-item {
          display: grid;
          grid-template-columns: auto minmax(0, 1fr) auto;
          align-items: center;
          gap: 0.9rem;
          padding: 1rem 1.05rem;
          color: #15233b;
          text-decoration: none !important;
          border: 1px solid #dbe4f0;
          border-radius: 20px;
          background: #ffffff;
          box-shadow: 0 10px 24px rgba(15, 23, 42, 0.04);
          transition: transform 0.16s ease, box-shadow 0.16s ease, border-color 0.16s ease, background-color 0.16s ease;
        }
        .dashboard-list-item:hover {
          transform: translateY(-1px);
          border-color: rgba(15, 23, 42, 0.18);
          background: #fbfdff;
          box-shadow: 0 14px 30px rgba(15, 23, 42, 0.06);
        }
        .dashboard-list-main {
          min-width: 0;
        }
        .dashboard-list-title {
          color: #15233b;
          font-size: 0.94rem;
          font-weight: 800;
          line-height: 1.38;
        }
        .dashboard-list-meta {
          margin-top: 0.22rem;
          color: #6c7f9d;
          font-size: 0.8rem;
          line-height: 1.4;
        }
        .dashboard-list-side {
          display: flex;
          flex-direction: column;
          align-items: flex-end;
          justify-content: center;
          gap: 0.3rem;
          color: #21314d;
          font-size: 0.78rem;
          font-weight: 700;
          line-height: 1.2;
          white-space: nowrap;
          min-width: 78px;
          text-align: right;
        }
        .dashboard-dday-pill {
          display: inline-flex;
          align-items: center;
          justify-content: center;
          min-width: 56px;
          height: 28px;
          padding: 0 0.7rem;
          border-radius: 999px;
          font-size: 0.8rem;
          font-weight: 850;
          letter-spacing: 0.01em;
          white-space: nowrap;
        }
        .dashboard-dday-pill.is-critical {
          color: #ffffff;
          background: #cb0000;
        }
        .dashboard-dday-pill.is-warning {
          color: #ffffff;
          background: #f89321;
        }
        .dashboard-dday-pill.is-calm {
          color: #ffffff;
          background: #0050a9;
        }
        .dashboard-activity-icon {
          width: 38px;
          height: 38px;
          display: inline-flex;
          align-items: center;
          justify-content: center;
          color: #2563eb;
          background: #f8fbff;
          border: 1px solid #dbe4f0;
          border-radius: 12px;
          font-size: 0.76rem;
          font-weight: 850;
        }
        .dashboard-list-date {
          color: #6c7f9d;
          font-size: 0.76rem;
          font-weight: 600;
          line-height: 1.2;
        }
        .dashboard-list-empty,
        .dashboard-empty {
          color: #6c7f9d;
          font-size: 0.92rem;
          padding: 0.4rem 0 0.1rem;
        }
        .notice-row-head,
        .notice-row-body {
          grid-template-columns: minmax(0, 0.95fr) minmax(0, 1.05fr) minmax(0, 3fr) minmax(0, 1.55fr) minmax(0, 1fr) minmax(0, 0.9fr) minmax(0, 1.15fr) minmax(0, 2.2fr) minmax(0, 0.85fr);
        }
        @media (max-width: 1180px) {
          .dashboard-bottom-grid {
            grid-template-columns: 1fr;
          }
        }
        @media (max-width: 900px) {
          .dashboard-kpi-grid {
            grid-template-columns: 1fr;
          }
          .dashboard-search-meta {
            justify-content: flex-start;
            text-align: left;
          }
          .rfp-card {
            flex-basis: min(86vw, 340px);
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
                ["Title", "Agency", "Ministry", "Notice No", "Notice ID", "Summary", "Project"],
                search_text,
            )
        ].copy()

    return filtered_opportunities, filtered_notices


def _render_dashboard_kpi_cards(recommended_rows: pd.DataFrame, notice_rows: pd.DataFrame) -> None:
    recommended_count = len(recommended_rows) if recommended_rows is not None and not recommended_rows.empty else 0
    favorite_count = 0
    if notice_rows is not None and not notice_rows.empty:
        favorite_series = notice_rows.apply(_dashboard_review_value, axis=1)
        favorite_count = int(favorite_series.eq(FAVORITE_REVIEW_STATUS).sum())
    urgent_count = _count_dashboard_urgent_notices(notice_rows)

    cards = [
        ("recommended_rfp", "추천 RFP", str(recommended_count), "추천 RFP Queue로 이동", "RFP"),
        ("urgent_notice", "마감 임박", str(urgent_count), "30일 이내 공고 보기", "D-30"),
        ("favorite_notice", "관심공고", str(favorite_count), "즐겨찾기 모아보기", "SAVE"),
    ]
    cols = st.columns(3, gap="medium")
    for column, (card_key, label, value, copy, icon) in zip(cols, cards):
        safe_key = _css_safe_key(f"dashboard_kpi_{card_key}")
        style_block = """
            <style>
            .st-key-__SAFE_KEY__ button {{
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
            .st-key-__SAFE_KEY__ button:hover {{
              border-color: #93c5fd !important;
              background: #f8fbff !important;
              color: #1d4ed8 !important;
              box-shadow: 0 14px 28px rgba(37, 99, 235, 0.10) !important;
            }}
            </style>
            """.replace("__SAFE_KEY__", safe_key)
        st.markdown(
            style_block,
            unsafe_allow_html=True,
        )
        with column:
            if st.button(
                f"{label}  {icon}\n{value}\n{copy}\n바로가기 >",
                key=f"dashboard_kpi_{card_key}",
                use_container_width=True,
                type="secondary",
            ):
                _navigate_from_dashboard_kpi(card_key)


def _render_dashboard_workspace(
    datasets: dict[str, pd.DataFrame],
    source_datasets: dict[str, object] | None,
) -> None:
    _inject_compact_public_dashboard_styles()
    opportunity_index = build_dashboard_opportunity_index(datasets, source_datasets)
    if not opportunity_index.empty:
        opportunity_index = _sort_opportunity_rows_for_display(opportunity_index)
    recommended_rows = (
        opportunity_index[build_positive_recommendation_mask(opportunity_index)].copy()
        if not opportunity_index.empty
        else pd.DataFrame()
    )
    notice_rows = _build_workspace_notice_rows(datasets, source_datasets, opportunity_index)
    if not notice_rows.empty:
        notice_rows = notice_rows.sort_values(
            by=["_sort_date", "Source", "Title"],
            ascending=[False, True, True],
            na_position="last",
        )
    favorite_count = len(build_favorite_notice_df(datasets["notice_view"], source_datasets or {}))
    urgent_count = int(
        len(
            build_dashboard_deadline_table(
                notice_rows if not notice_rows.empty else build_dashboard_notice_index(datasets, source_datasets, archived=False),
                limit=None,
            )
        )
    )

    search_cols = st.columns([10.8, 2.2], gap="small")
    with search_cols[0]:
        dashboard_search = clean(
            st.text_input(
                "Dashboard Search",
                key="public_dashboard_compact_search_text",
                placeholder="공고명, 과제명, 키워드, 기관명 검색",
                label_visibility="collapsed",
            )
        )
    with search_cols[1]:
        updated_at = pd.Timestamp.now(tz="Asia/Seoul").strftime("%Y-%m-%d %H:%M")
        st.markdown(
            f'<div class="dashboard-search-meta">업데이트 {escape(updated_at)}</div>',
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
    top_rows = recommended_filtered.head(10).copy() if not recommended_filtered.empty else pd.DataFrame()

    def _deadline_sort_key(value: object) -> int | None:
        text = clean(value)
        if not text or text in {"-", "마감"}:
            return None
        if text == "D-Day":
            return 0
        match = re.fullmatch(r"D-(\d+)", text)
        if not match:
            return None
        return int(match.group(1))

    deadline_preview_rows = pd.DataFrame()
    if not filtered_notice_rows.empty:
        deadline_preview_rows = filtered_notice_rows.copy()
        deadline_preview_rows["_deadline_sort"] = deadline_preview_rows["D-Day"].apply(_deadline_sort_key)
        deadline_preview_rows = deadline_preview_rows[deadline_preview_rows["_deadline_sort"].notna()].copy()
        if not deadline_preview_rows.empty:
            deadline_preview_rows["_deadline_sort"] = deadline_preview_rows["_deadline_sort"].astype(int)
            deadline_preview_rows = deadline_preview_rows.sort_values(
                by=["_deadline_sort", "_sort_date", "Title"],
                ascending=[True, False, True],
                na_position="last",
            ).head(5)

    recent_notice_rows = pd.DataFrame()
    if not filtered_notice_rows.empty:
        recent_notice_rows = filtered_notice_rows.sort_values(
            by=["_sort_date", "Title"],
            ascending=[False, True],
            na_position="last",
        ).head(5).copy()

    if dashboard_search:
        st.caption(f"검색 결과: 추천 RFP {len(top_rows)}건, 마감 임박 {len(deadline_preview_rows)}건")

    kpi_cards = [
        ("추천 RFP", f"{len(recommended_rows)}건", "AI 추천 기준", "iris", "rfp_queue", "blue", "RFP"),
        ("마감 임박", f"{urgent_count}건", "D-30 이내", "notices", "notice_queue", "red", "D-30"),
        ("관심공고", f"{favorite_count}건", "저장된 공고", "favorites", "favorites", "green", "SAVE"),
    ]
    st.markdown(
        '<section class="dashboard-kpi-grid">'
        + "".join(
            (
                f'<a class="dashboard-kpi-card is-{tone}" href="?source={source}&page={page}&view=table" target="_self">'
                f'<div class="dashboard-kpi-label">{escape(label)}</div>'
                f'<div class="dashboard-kpi-value">{escape(value)}</div>'
                f'<div class="dashboard-kpi-caption">{escape(caption)}</div>'
                f'<div class="dashboard-kpi-symbol">{escape(symbol)}</div>'
                '</a>'
            )
            for label, value, caption, source, page, tone, symbol in kpi_cards
        )
        + '</section>',
        unsafe_allow_html=True,
    )

    card_html: list[str] = []
    for rank, (_, row) in enumerate(top_rows.iterrows(), start=1):
        ctx = _queue_row_context(row)
        row_id = clean(row.get("Row ID"))
        href = build_route_href("rfp_queue", row_id, source_key=resolve_route_source_key_for_row(row))
        recommendation_text = clean(ctx["recommendation"]) or "검토필요"
        recommendation_class = "is-recommend" if is_positive_recommendation(recommendation_text) else "is-review"
        keywords_html = "".join(
            f'<span class="rfp-card-keyword">{escape(keyword)}</span>'
            for keyword in _extract_dashboard_keywords(row, limit=3)
        )
        keyword_markup = keywords_html or '<span class="rfp-card-keyword">RFP</span>'
        agency_line = " / ".join(
            part for part in [ctx["ministry"], ctx["agency"]] if clean(part) and part != "-"
        ) or ctx["agency"]
        score_text = clean(ctx["score"])
        score_label = f"{score_text}점" if score_text and score_text != "-" else "-"
        card_html.append(
            (
                f'<a class="rfp-card" href="{escape(href, quote=True)}" target="_self">'
                '<div class="rfp-card-topline">'
                '<div class="rfp-card-topline-left">'
                f'<span class="rfp-card-rank">{rank}</span>'
                f'<span class="dashboard-pill {recommendation_class}">{escape(recommendation_text)}</span>'
                '</div>'
                f'<span class="rfp-card-score">{escape(score_label)}</span>'
                '</div>'
                f'<div class="rfp-card-title">{escape(truncate_text(ctx["project"], max_chars=56))}</div>'
                f'<div class="rfp-card-notice">{escape(truncate_text(ctx["notice"], max_chars=72))}</div>'
                f'<div class="rfp-card-keywords">{keyword_markup}</div>'
                f'<div class="rfp-card-agency">{escape(truncate_text(agency_line, max_chars=40))}</div>'
                '<div class="rfp-card-meta">'
                f'<span>{escape(ctx["period"])}</span>'
                f'<strong>{escape(ctx["deadline"])}</strong>'
                f'<span>{escape(ctx["budget"])}</span>'
                '</div>'
                '</a>'
            )
        )
    st.markdown(
        (
            '<section class="dashboard-section">'
            '<div class="dashboard-section-header">'
            '<div class="dashboard-section-title">추천 RFP Top 10</div>'
            '<a class="dashboard-section-link" href="?source=iris&page=rfp_queue&view=table" target="_self">전체 RFP Queue 보기</a>'
            '</div>'
            '<div class="dashboard-carousel-note">좌우로 넘겨서 추천 RFP 10개를 확인할 수 있습니다.</div>'
            f"""<div class="rfp-card-row">{"".join(card_html) if card_html else '<div class="dashboard-empty">표시할 추천 RFP가 없습니다.</div>'}</div>"""
            '</section>'
        ),
        unsafe_allow_html=True,
    )

    deadline_body: list[str] = []
    for _, row in deadline_preview_rows.iterrows():
        source_key = _workspace_source_key(row)
        notice_id = clean(row.get("Notice ID"))
        href = build_dashboard_notice_route(source_key, notice_id)
        dday_label = clean(row.get("D-Day")) or "-"
        dday_sort = _deadline_sort_key(dday_label)
        tone = "is-calm"
        if dday_sort is not None and dday_sort <= 3:
            tone = "is-critical"
        elif dday_sort is not None and dday_sort <= 14:
            tone = "is-warning"
        agency_line = " / ".join(
            part for part in [clean(row.get("Ministry")), clean(row.get("Agency"))]
            if part and part != "-"
        ) or clean(row.get("Agency")) or clean(row.get("Source")) or "-"
        budget_text = extract_budget_summary(clean(row.get("Budget"))) or clean(row.get("Budget")) or "-"
        deadline_body.append(
            (
                f'<a class="dashboard-list-item" href="{escape(href, quote=True)}" target="_self">'
                f'<span class="dashboard-dday-pill {tone}">{escape(dday_label)}</span>'
                '<div class="dashboard-list-main">'
                f'<div class="dashboard-list-title">{escape(truncate_text(clean(row.get("Title")), max_chars=70))}</div>'
                f'<div class="dashboard-list-meta">{escape(truncate_text(agency_line, max_chars=56))}</div>'
                '</div>'
                f'<div class="dashboard-list-side">{escape(truncate_text(budget_text, max_chars=14))}</div>'
                '</a>'
            )
        )

    recent_notice_body: list[str] = []
    for _, row in recent_notice_rows.iterrows():
        source_key = _workspace_source_key(row)
        notice_id = clean(row.get("Notice ID"))
        href = build_dashboard_notice_route(source_key, notice_id)
        source_label = clean(row.get("Source")) or "-"
        status_badge = notice_status_badge_html(row.get("Status"))
        date_text = clean(row.get("Date")) or "-"
        period_text = clean(row.get("Period")) or "-"
        recent_notice_body.append(
            (
                f'<a class="dashboard-list-item" href="{escape(href, quote=True)}" target="_self">'
                f'<span class="dashboard-activity-icon">{escape(source_label[:4] or "공고")}</span>'
                '<div class="dashboard-list-main">'
                f'<div class="dashboard-list-title">{escape(truncate_text(clean(row.get("Title")), max_chars=70))}</div>'
                f'<div class="dashboard-list-meta">{escape(truncate_text(period_text, max_chars=52))}</div>'
                '</div>'
                f'<div class="dashboard-list-side">{status_badge}<div class="dashboard-list-date">{escape(date_text)}</div></div>'
                '</a>'
            )
        )

    st.markdown(
        (
            '<div class="dashboard-bottom-grid">'
            '<section class="dashboard-section dashboard-list-panel">'
            '<div class="dashboard-section-header">'
            '<div class="dashboard-section-title">최근 공고</div>'
            '<a class="dashboard-section-link" href="?source=notices&page=notice_queue&view=table" target="_self">전체 보기</a>'
            '</div>'
            f"""<div class="dashboard-list">{"".join(recent_notice_body) if recent_notice_body else '<div class="dashboard-list-empty">표시할 최근 공고가 없습니다.</div>'}</div>"""
            '</section>'
            '<section class="dashboard-section dashboard-list-panel">'
            '<div class="dashboard-section-header">'
            '<div class="dashboard-section-title">마감 임박 공고</div>'
            '<a class="dashboard-section-link" href="?source=notices&page=notice_queue&view=table" target="_self">전체 보기</a>'
            '</div>'
            f"""<div class="dashboard-list">{"".join(deadline_body) if deadline_body else '<div class="dashboard-list-empty">표시할 마감 임박 공고가 없습니다.</div>'}</div>"""
            '</section>'
            '</div>'
        ),
        unsafe_allow_html=True,
    )


def main(app_mode: str = "viewer"):
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
    consume_workspace_logout_query_action()
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
        st.error(f"시트 로딩 실패: {exc}")
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
