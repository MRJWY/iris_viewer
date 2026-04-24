import json
import os
import re
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
    get_default_page_for_source,
    get_source_config_map,
    get_source_key_map,
    get_source_label_map,
)
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


NOTICE_PREFERRED_COLUMNS = [
    "공고일자",
    "접수기간",
    "전문기관",
    "공고명",
    "상세링크",
    "사업비",
    "공고상태",
    "검토 여부",
    "공고ID",
    "소관부처",
    "공고번호",
    "상태키",
    "is_current",
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
    "공고번호",
    "전문기관명",
    "공고명",
    "상세링크",
    "추천여부",
    "공고상태",
    "접수기간",
    "검토여부",
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
    "created_at",
    "source",
    "notice_id",
    "notice_title",
    "author",
    "comment",
]


def clean(value) -> str:
    return str(value or "").strip()


def render_iris_page(page_key: str, datasets: dict[str, pd.DataFrame]) -> None:
    if page_key == "opportunity":
        render_opportunity_page(datasets["opportunity"])
    elif page_key == "summary":
        render_summary_page(datasets["summary"], datasets["opportunity"])
    elif page_key == "notice":
        render_notice_page(datasets["notice_view"], datasets["opportunity"])
    elif page_key == "notice_scheduled":
        render_notice_page_with_scope(
            datasets["notice_view"],
            datasets["opportunity"],
            page_key="notice_scheduled",
            title="예정 공고",
            default_status_scope="예정",
            current_only_default=True,
        )
    elif page_key == "notice_archive":
        render_notice_page_with_scope(
            datasets["notice_view"],
            datasets["opportunity"],
            page_key="notice_archive",
            title="Archive",
            default_status_scope="전체",
            current_only_default=False,
            archive=True,
        )


def render_iris_source(
    source_config: SourceRouteConfig,
    mode_config: AppModeConfig,
    datasets: dict[str, pd.DataFrame],
    source_datasets: dict[str, object] | None = None,
) -> None:
    del source_config
    del source_datasets
    raw_page_key = get_query_param("page")
    current_page_key = raw_page_key or mode_config.default_iris_page
    current_view = get_query_param("view") or "table"

    if current_page_key not in mode_config.valid_iris_pages:
        st.query_params.clear()
        st.query_params.update({
            "source": "iris",
            "page": mode_config.default_iris_page,
            "view": "table",
        })
        st.rerun()

    if not st.session_state.get("_initial_page_redirect_done"):
        st.session_state["_initial_page_redirect_done"] = True
        if current_view == "table" and not raw_page_key:
            st.query_params.clear()
            st.query_params.update({
                "source": "iris",
                "page": mode_config.default_iris_page,
                "view": "table",
            })
            st.rerun()

    current_page_key = render_page_tabs(
        current_page_key,
        list(mode_config.iris_tabs),
        key=mode_config.iris_tab_key,
    )

    render_iris_page(current_page_key, datasets)


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
) -> None:
    if not source_datasets:
        st.error(f"{source_config.label} 데이터를 불러오지 못했습니다.")
        return

    st.subheader(source_config.label)
    raw_page_key = get_query_param("page")
    current_page_key = raw_page_key or source_config.default_page
    valid_page_keys = {page.key for page in source_config.page_configs}

    if current_page_key not in valid_page_keys:
        st.query_params.clear()
        st.query_params.update({
            "source": source_config.key,
            "page": source_config.default_page,
            "view": "table",
        })
        st.rerun()

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
) -> None:
    del mode_config, datasets
    render_external_source(source_config, source_datasets)


def render_nipa_source(
    source_config: SourceRouteConfig,
    mode_config: AppModeConfig,
    datasets: dict[str, pd.DataFrame],
    source_datasets: dict[str, object] | None,
) -> None:
    del mode_config, datasets
    render_external_source(source_config, source_datasets)


def render_favorites_source(
    source_config: SourceRouteConfig,
    mode_config: AppModeConfig,
    datasets: dict[str, pd.DataFrame],
    source_datasets: dict[str, object] | None,
) -> None:
    del source_config, mode_config
    render_favorite_notice_page(
        datasets["notice_view"],
        datasets["opportunity"],
        source_datasets or {},
    )


SOURCE_RENDERERS = {
    "iris": render_iris_source,
    "tipa": render_tipa_source,
    "nipa": render_nipa_source,
    "favorites": render_favorites_source,
}


def render_selected_source(
    source_key: str,
    *,
    source_config: SourceRouteConfig | None,
    mode_config: AppModeConfig,
    datasets: dict[str, pd.DataFrame],
    source_datasets: dict[str, object] | None,
) -> None:
    renderer = SOURCE_RENDERERS.get(source_config.renderer_key if source_config else source_key)
    if renderer is None:
        fallback_config = source_config or SourceRouteConfig("iris", "IRIS", mode_config.default_iris_page, False, "iris")
        render_iris_source(fallback_config, mode_config, datasets)
        return
    active_config = source_config or SourceRouteConfig(source_key, source_key, mode_config.default_iris_page, False, source_key)
    renderer(active_config, mode_config, datasets, source_datasets)


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
    link = row_first_non_empty(row, "상세링크", "detail_link")
    normalized_source = clean(
        source_key
        or row.get("_source_key")
        or row.get("source_site")
        or row.get("출처사이트")
    ).lower()
    if normalized_source in {"tipa", "mss", "nipa"}:
        return link

    notice_id = row_first_non_empty(row, "공고ID", "notice_id")
    if not notice_id:
        return link

    status_key = row_first_non_empty(row, "상태키", "status_key")
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
    sh = gc.open_by_key(sheet_id)
    return sh.worksheet(sheet_name)


def get_spreadsheet():
    gc = get_gspread_client()
    sheet_id = get_env("GOOGLE_SHEET_ID")
    if not sheet_id:
        raise RuntimeError("GOOGLE_SHEET_ID is not set.")
    return gc.open_by_key(sheet_id)


def get_or_create_worksheet(sheet_name: str, headers: list[str], rows: int = 1000, cols: int | None = None):
    sh = get_spreadsheet()
    try:
        ws = sh.worksheet(sheet_name)
    except gspread.WorksheetNotFound:
        ws = sh.add_worksheet(title=sheet_name, rows=rows, cols=cols or len(headers))
        ws.update([headers])
        return ws

    values = ws.get_all_values()
    if not values:
        ws.update([headers])
        return ws

    header = [clean(x) for x in values[0]]
    missing_headers = [column for column in headers if column not in header]
    if missing_headers:
        ws.update(
            range_name=f"A1:{chr(64 + len(header) + len(missing_headers))}1",
            values=[header + missing_headers],
        )
    return ws


@st.cache_data(ttl=300, show_spinner=False)
def load_sheet_as_dataframe(sheet_name: str) -> pd.DataFrame:
    ws = get_worksheet(sheet_name)
    values = ws.get_all_values()

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


def get_comment_sheet_name() -> str:
    return get_env("NOTICE_COMMENT_SHEET", "NOTICE_COMMENTS")


def load_notice_comments() -> pd.DataFrame:
    df = load_optional_sheet_as_dataframe(get_comment_sheet_name())
    if df.empty:
        return pd.DataFrame(columns=COMMENT_COLUMNS)

    working = df.copy()
    for column in COMMENT_COLUMNS:
        if column not in working.columns:
            working[column] = ""
    working["created_at_sort"] = pd.to_datetime(working["created_at"], errors="coerce")
    return working.sort_values(by=["created_at_sort"], ascending=False, na_position="last")


def filter_notice_comments(comments_df: pd.DataFrame, *, source_key: str, notice_id: str) -> pd.DataFrame:
    if comments_df.empty:
        return pd.DataFrame(columns=COMMENT_COLUMNS)

    working = comments_df.copy()
    for column in COMMENT_COLUMNS:
        if column not in working.columns:
            working[column] = ""

    comment_notice_keys = working["notice_id"].apply(normalize_notice_id_for_match)
    current_notice_key = normalize_notice_id_for_match(notice_id)
    return working[
        working["source"].fillna("").astype(str).str.strip().eq(clean(source_key))
        & comment_notice_keys.eq(current_notice_key)
    ].copy()


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
    load_sheet_as_dataframe.clear()


def delete_notice_comment(comment_id: str) -> None:
    comment_id = clean(comment_id)
    if not comment_id:
        raise RuntimeError("삭제할 댓글 ID가 없습니다.")

    ws = get_worksheet(get_comment_sheet_name())
    values = ws.get_all_values()
    if not values:
        raise RuntimeError("댓글 이력 시트가 비어 있습니다.")

    header = [clean(x) for x in values[0]]
    if "comment_id" not in header:
        raise RuntimeError("댓글 이력 시트에 comment_id 컬럼이 없습니다.")

    comment_id_col = header.index("comment_id")
    for row_index, sheet_row in enumerate(values[1:], start=2):
        current_comment_id = clean(sheet_row[comment_id_col] if comment_id_col < len(sheet_row) else "")
        if current_comment_id == comment_id:
            ws.delete_rows(row_index)
            load_sheet_as_dataframe.clear()
            return

    raise RuntimeError("삭제할 댓글을 찾지 못했습니다.")


def resolve_notice_source_key(row: dict | None) -> str:
    if row:
        source_key = clean(row.get("_source_key"))
        if source_key and source_key != "favorites":
            return source_key
    current_source = get_query_param("source") or "iris"
    if current_source in {"tipa", "nipa"}:
        return current_source
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
    sheet_name = get_env("MSS_OPPORTUNITY_MASTER_SHEET", "MSS_OPPORTUNITY_MASTER")
    return enrich_opportunity_df(load_optional_sheet_as_dataframe(sheet_name))


@st.cache_data(ttl=1800, show_spinner=False)
def load_nipa_opportunity_df() -> pd.DataFrame:
    sheet_name = get_env("NIPA_OPPORTUNITY_MASTER_SHEET", "NIPA_OPPORTUNITY_MASTER")
    return enrich_opportunity_df(load_optional_sheet_as_dataframe(sheet_name))


def find_header_column(header: list[str], candidates: list[str]) -> int | None:
    for candidate in candidates:
        if candidate in header:
            return header.index(candidate) + 1
    return None


def update_notice_review_status(notice_id: str, review_status: str) -> None:
    notice_id = clean(notice_id)
    if not notice_id:
        raise RuntimeError("공고ID가 없어 검토 여부를 저장할 수 없습니다.")

    notice_master_sheet = get_env("NOTICE_MASTER_SHEET", "IRIS_NOTICE_MASTER")
    ws = get_worksheet(notice_master_sheet)
    values = ws.get_all_values()
    if not values:
        raise RuntimeError("IRIS_NOTICE_MASTER 시트가 비어 있습니다.")

    header = [clean(x) for x in values[0]]
    notice_id_col = find_header_column(header, ["공고ID", "notice_id"])
    review_col = find_header_column(header, ["검토 여부", "검토여부", "review_status"])
    if not notice_id_col:
        raise RuntimeError("필수 컬럼이 없습니다: 공고ID/notice_id")
    if not review_col:
        review_col = len(header) + 1
        ws.update_cell(1, review_col, "review_status")

    for row_index, row in enumerate(values[1:], start=2):
        current_notice_id = clean(row[notice_id_col - 1] if notice_id_col - 1 < len(row) else "")
        if current_notice_id == notice_id:
            ws.update_cell(row_index, review_col, clean(review_status))
            load_sheet_as_dataframe.clear()
            return

    raise RuntimeError(f"IRIS_NOTICE_MASTER에서 공고ID {notice_id}를 찾지 못했습니다.")


def update_mss_review_status(notice_id: str, review_status: str) -> None:
    notice_id = clean(notice_id)
    if not notice_id:
        raise RuntimeError("공고ID가 없어 검토 여부를 저장할 수 없습니다.")

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

        values = ws.get_all_values()
        if not values:
            continue

        header = [clean(x) for x in values[0]]
        notice_id_col = find_header_column(header, ["공고ID", "notice_id"])
        if not notice_id_col:
            continue

        review_col = find_header_column(header, ["검토 여부", "검토여부", "review_status"])
        if not review_col:
            review_col = len(header) + 1
            ws.update_cell(1, review_col, "review_status")

        for row_index, row in enumerate(values[1:], start=2):
            current_notice_id = clean(row[notice_id_col - 1] if notice_id_col - 1 < len(row) else "")
            if current_notice_id == notice_id:
                ws.update_cell(row_index, review_col, clean(review_status))
                load_sheet_as_dataframe.clear()
                return

    raise RuntimeError(f"중소기업벤처부 시트({', '.join(checked_sheets)})에서 공고ID {notice_id}를 찾지 못했습니다.")


def update_nipa_review_status(notice_id: str, review_status: str) -> None:
    notice_id = clean(notice_id)
    if not notice_id:
        raise RuntimeError("공고ID가 없어 검토 여부를 저장할 수 없습니다.")

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

        values = ws.get_all_values()
        if not values:
            continue

        header = [clean(x) for x in values[0]]
        notice_id_col = find_header_column(header, ["공고ID", "notice_id"])
        if not notice_id_col:
            continue

        review_col = find_header_column(header, ["검토 여부", "검토여부", "review_status"])
        if not review_col:
            review_col = len(header) + 1
            ws.update_cell(1, review_col, "review_status")

        for row_index, row in enumerate(values[1:], start=2):
            current_notice_id = clean(row[notice_id_col - 1] if notice_id_col - 1 < len(row) else "")
            if current_notice_id == notice_id:
                ws.update_cell(row_index, review_col, clean(review_status))
                load_sheet_as_dataframe.clear()
                return

    raise RuntimeError(f"NIPA 시트({', '.join(checked_sheets)})에서 공고ID {notice_id}를 찾지 못했습니다.")


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


def normalize_review_status_label(value: object) -> str:
    return clean(value).replace(" ", "")


def is_archived_review_status_value(value: object) -> bool:
    return normalize_review_status_label(value) in {
        status.replace(" ", "")
        for status in ARCHIVE_REVIEW_STATUS_VALUES
    }


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

    for status_source in ["notice_status", "status", "rcve_status", "공고상태", "怨듦퀬?곹깭"]:
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

    working = working.sort_values(
        by=["notice_id", "rfp_score", "_recommendation_rank", "_project_name"],
        ascending=[True, False, False, True],
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
    return st.sidebar.text_input("통합 검색", "", key=unified_sidebar_filter_key(key))


def render_notice_filter_sidebar(
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


def render_metrics(items: list[tuple[str, str]]) -> None:
    cols = st.columns(len(items))
    for column, (label, value) in zip(cols, items):
        with column:
            st.metric(label, value)


def inject_page_styles() -> None:
    st.markdown(
        """
        <style>
        .detail-hero {
          padding: 20px 22px;
          border: 1px solid #e5e7eb;
          border-radius: 18px;
          background:
            radial-gradient(circle at top right, rgba(252, 211, 77, 0.18), transparent 28%),
            linear-gradient(180deg, #ffffff 0%, #fbfdff 100%);
          margin: 8px 0 18px 0;
        }
        .detail-kicker {
          font-size: 13px;
          font-weight: 700;
          letter-spacing: 0.04em;
          text-transform: uppercase;
          color: #64748b;
          margin-bottom: 8px;
        }
        .detail-title {
          font-size: 34px;
          font-weight: 800;
          line-height: 1.18;
          color: #0f172a;
          margin-bottom: 14px;
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
          font-size: 13px;
          font-weight: 700;
          background: #eef2ff;
          color: #3730a3;
        }
        .detail-chip.neutral {
          background: #f1f5f9;
          color: #334155;
        }
        .detail-card {
          border: 1px solid #e5e7eb;
          border-radius: 16px;
          padding: 16px 18px;
          background: white;
          height: 100%;
          box-shadow: 0 1px 2px rgba(15, 23, 42, 0.04);
        }
        .detail-card-title {
          font-size: 14px;
          font-weight: 800;
          color: #0f172a;
          margin-bottom: 12px;
        }
        .detail-field {
          padding: 10px 0;
          border-bottom: 1px solid #f1f5f9;
        }
        .detail-field:last-child {
          border-bottom: none;
          padding-bottom: 0;
        }
        .detail-label {
          font-size: 12px;
          font-weight: 700;
          text-transform: uppercase;
          letter-spacing: 0.03em;
          color: #64748b;
          margin-bottom: 4px;
        }
        .detail-value {
          font-size: 17px;
          font-weight: 500;
          line-height: 1.6;
          color: #111827;
          word-break: break-word;
          font-family: "Apple SD Gothic Neo", "Noto Sans KR", "Segoe UI", sans-serif;
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
          font-weight: 500;
        }
        .detail-more summary .detail-toggle-text {
          margin-left: 6px;
          font-size: 13px;
          font-weight: 700;
          color: #2563eb;
        }
        .detail-more summary::-webkit-details-marker {
          display: none;
        }
        .detail-more[open] summary {
          display: none;
        }
        .detail-more-body {
          margin-top: 2px;
          font-size: 17px;
          font-weight: 500;
          line-height: 1.6;
          color: #111827;
          white-space: pre-wrap;
          word-break: break-word;
          font-family: "Apple SD Gothic Neo", "Noto Sans KR", "Segoe UI", sans-serif;
        }
        .detail-section-title {
          font-size: 22px;
          font-weight: 800;
          color: #0f172a;
          margin: 24px 0 12px 0;
        }
        .list-table-wrap {
          width: 100%;
          overflow-x: auto;
          border: 1px solid #e5e7eb;
          border-radius: 16px;
          background: #ffffff;
        }
        .list-table {
          width: max-content;
          min-width: 100%;
          border-collapse: collapse;
          table-layout: auto;
        }
        .list-table thead th {
          background: #f8fafc;
          color: #334155;
          font-size: 13px;
          font-weight: 800;
          text-align: left;
          padding: 12px 14px;
          border-bottom: 1px solid #e5e7eb;
          white-space: nowrap;
        }
        .list-table tbody td {
          padding: 10px 14px;
          border-bottom: 1px solid #f1f5f9;
          vertical-align: middle;
          height: 52px;
          white-space: nowrap;
          overflow: hidden;
          text-overflow: ellipsis;
        }
        .list-table tbody tr:hover {
          background: #f8fbff;
        }
        .list-table tbody td a {
          color: #0f172a;
          text-decoration: none;
        }
        .list-table tbody td a:hover {
          color: #2563eb;
        }
        .list-action-col {
          width: 112px;
          min-width: 112px;
          max-width: 112px;
          text-align: center !important;
        }
        .list-action-cell,
        .list-link-cell {
          text-align: center;
        }
        .list-action-link,
        .list-link-out {
          display: inline-flex;
          align-items: center;
          justify-content: center;
          min-width: 84px;
          min-height: 34px;
          padding: 0 12px;
          border: 1px solid #d1d5db;
          border-radius: 8px;
          background: #ffffff;
          font-size: 13px;
          font-weight: 700;
          white-space: nowrap;
        }
        .list-action-link:hover,
        .list-link-out:hover {
          border-color: #93c5fd;
          background: #eff6ff;
        }
        .list-cell-text,
        .list-cell-empty {
          display: block;
          color: #0f172a;
          white-space: nowrap;
          overflow: hidden;
          text-overflow: ellipsis;
        }
        .list-cell-empty {
          color: #94a3b8;
          text-align: center;
        }
        .faux-tabs-wrap {
          display: flex;
          gap: 8px;
          margin: 8px 0 14px 0;
          border-bottom: 1px solid #e5e7eb;
          padding-bottom: 8px;
          flex-wrap: wrap;
        }
        .faux-tab {
          display: inline-flex;
          align-items: center;
          justify-content: center;
          padding: 10px 14px;
          border: 1px solid #d1d5db;
          border-radius: 10px 10px 0 0;
          background: #f8fafc;
          color: #334155 !important;
          text-decoration: none !important;
          font-weight: 600;
          min-width: 112px;
        }
        .faux-tab:hover {
          background: #eef2ff;
          color: #0f172a !important;
        }
        .faux-tab-active {
          background: #ffffff;
          border-color: #cbd5e1;
          border-bottom: 2px solid #0f766e;
          color: #0f172a !important;
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


def switch_to_detail(page_key: str, identifier: str) -> None:
    current_source = get_query_param("source")
    st.query_params.clear()
    params = {
        "page": page_key,
        "view": "detail",
        "id": clean(identifier),
    }
    if current_source:
        params["source"] = current_source
    st.query_params.update(params)
    st.session_state[f"{page_key}_view"] = "detail"
    st.session_state[f"{page_key}_selected_id"] = clean(identifier)
    st.rerun()


def switch_to_table(page_key: str) -> None:
    current_source = get_query_param("source")
    st.query_params.clear()
    params = {
        "page": page_key,
        "view": "table",
    }
    if current_source:
        params["source"] = current_source
    st.query_params.update(params)
    st.session_state[f"{page_key}_view"] = "table"
    st.rerun()


def get_query_param(name: str) -> str:
    value = st.query_params.get(name, "")
    if isinstance(value, list):
        return clean(value[0]) if value else ""
    return clean(value)


def get_route_state(page_key: str) -> tuple[str, str]:
    current_page = get_query_param("page") or "notice"
    if current_page != page_key:
        return "table", ""

    current_view = get_query_param("view") or "table"
    selected_id = get_query_param("id")
    return current_view, selected_id


def build_page_href(page_key: str) -> str:
    params = {"page": page_key, "view": "table"}
    current_source = get_query_param("source")
    if current_source and not (current_source == "favorites" and page_key != "favorites"):
        params["source"] = current_source
    return f"?{urlencode(params)}"


def render_page_tabs(current_page_key: str, tabs: list[tuple[str, str]], *, key: str) -> str:
    page_options = {page_key: label for page_key, label in tabs}
    if current_page_key not in page_options:
        current_page_key = next(iter(page_options))

    selected_label = st.radio(
        "Page",
        list(page_options.values()),
        horizontal=True,
        index=list(page_options.keys()).index(current_page_key),
        key=key,
        label_visibility="collapsed",
    )
    selected_page_key = next(
        page_key for page_key, label in page_options.items() if label == selected_label
    )
    if selected_page_key != current_page_key:
        current_source = get_query_param("source")
        st.query_params.clear()
        params = {
            "page": selected_page_key,
            "view": "table",
        }
        if current_source:
            params["source"] = current_source
        st.query_params.update(params)
        st.rerun()
    return selected_page_key


def build_route_href(page_key: str, identifier: str) -> str:
    params = {"page": page_key, "view": "detail", "id": clean(identifier)}
    current_source = get_query_param("source")
    if current_source and not (current_source == "favorites" and page_key != "favorites"):
        params["source"] = current_source
    return f"?{urlencode(params)}"


def render_clickable_table(
    df: pd.DataFrame,
    preferred_columns: list[str],
    page_key: str,
    id_column: str,
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
        "공고일자": "110px",
        "공고번호": "190px",
        "전문기관": "180px",
        "전문기관명": "180px",
        "공고명": "720px",
        "notice_title": "720px",
        "해당 과제명": "520px",
        "project_name": "520px",
        "공고상태": "120px",
        "접수기간": "240px",
        "추천여부": "110px",
        "추천도 및 점수": "140px",
        "예산": "180px",
        "검토 여부": "110px",
        "검토여부": "110px",
    }
    compact_limits = {
        "공고명": 120,
        "notice_title": 120,
        "해당 과제명": 90,
        "project_name": 90,
        "접수기간": 48,
        "예산": 40,
    }

    header_cells = ['<th class="list-action-col">상세</th>']
    header_cells.extend(f"<th>{escape(column)}</th>" for column in display_columns)
    header_html = "".join(header_cells)

    body_rows = []
    for _, row in df.iterrows():
        identifier = clean(row.get(id_column))
        if not identifier:
            continue

        href = build_route_href(page_key, identifier)
        cell_html = [
            (
                '<td class="list-action-cell">'
                '<a class="list-action-link" href="{href}" target="_self">상세 보기</a>'
                "</td>"
            ).format(href=escape(href, quote=True))
        ]
        for column in display_columns:
            value = compact_table_value(row.get(column), max_chars=compact_limits.get(column, 70))
            width_style = ""
            if column in column_widths:
                width_style = (
                    f" style=\"min-width:{column_widths[column]};max-width:{column_widths[column]};\""
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


def resolve_local_file_path(row: dict) -> Path | None:
    if not row:
        return None

    for key in ["file_path", "파일경로"]:
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


def render_review_editor(notice_id: str, current_value: str, form_key: str, source_key: str = "iris") -> None:
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
                if clean(source_key) == "tipa":
                    update_mss_review_status(notice_id, review_value)
                elif clean(source_key) == "nipa":
                    update_nipa_review_status(notice_id, review_value)
                else:
                    update_notice_review_status(notice_id, review_value)
                st.success("검토 여부를 저장했습니다.")
                st.rerun()
            except Exception as exc:
                st.error(f"저장 실패: {exc}")


def render_notice_comments(row: dict, section_key: str) -> None:
    notice_id = clean(row.get("공고ID") or row.get("notice_id"))
    notice_title = clean(row.get("공고명") or row.get("notice_title"))
    source_key = resolve_notice_source_key(row)

    st.markdown('<div class="detail-section-title">댓글</div>', unsafe_allow_html=True)
    if not notice_id:
        st.info("공고ID가 없어 댓글을 연결할 수 없습니다.")
        return

    saved_comment = False
    with st.form(f"{section_key}_comment_form"):
        author = st.text_input("작성자", value=get_env("DEFAULT_COMMENT_AUTHOR", ""), key=f"{section_key}_comment_author")
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
                saved_comment = True
                st.success("댓글을 저장했습니다.")
            except Exception as exc:
                st.error(f"댓글 저장 실패: {exc}")

    try:
        comments_df = load_notice_comments()
    except Exception as exc:
        st.warning(f"댓글 이력을 불러오지 못했습니다: {exc}")
        comments_df = pd.DataFrame(columns=COMMENT_COLUMNS)

    matched = filter_notice_comments(comments_df, source_key=source_key, notice_id=notice_id)

    if matched.empty:
        if not saved_comment:
            st.info("아직 등록된 댓글이 없습니다.")
        return

    st.caption(f"댓글 이력 {len(matched)}건")
    for _, comment_row in matched.iterrows():
        comment_id = clean(comment_row.get("comment_id"))
        created_at = clean(comment_row.get("created_at"))
        author = clean(comment_row.get("author")) or "익명"
        comment_text = clean(comment_row.get("comment"))
        delete_key = f"{section_key}_delete_comment_{comment_id}"
        with st.container(border=True):
            st.caption(" · ".join([value for value in [created_at, author] if value]))
            st.write(comment_text)
            if comment_id and st.button("댓글 삭제", key=delete_key):
                try:
                    delete_notice_comment(comment_id)
                    st.success("댓글을 삭제했습니다.")
                    st.rerun()
                except Exception as exc:
                    st.error(f"댓글 삭제 실패: {exc}")


def render_notice_detail_from_row(row: dict, opportunity_df: pd.DataFrame) -> None:
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

    render_detail_header(
        title=clean(row.get("공고명")),
        kicker=detail_kicker,
        chips=[
            (clean(row.get("대표추천도")), "accent"),
            (f"점수 {clean(row.get('대표점수'))}" if clean(row.get("대표점수")) else "", "neutral"),
            (clean(row.get("공고상태")), "accent"),
            (clean(row.get("전문기관") or row.get("담당부서")), "neutral"),
            (clean(row.get("공고일자")), "neutral"),
            (f"검토: {clean(row.get('검토 여부') or '미지정')}", "neutral"),
        ],
    )

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

    action_left, action_right = st.columns([1, 2])
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
            "분석 정보",
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
        page_key="opportunity",
        id_column="_row_id",
    )


def render_pending_detail_from_row(row: dict) -> None:
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


def render_opportunity_detail_from_row(row: dict) -> None:
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

    render_detail_header(
        title=clean(
            row.get("project_name")
            or row.get("llm_project_name")
            or row.get("llm_rfp_title")
            or row.get("rfp_title")
            or row.get("notice_title")
            or row.get("\uacf5\uace0\uba85")
        ),
        kicker="Opportunity Master Detail",
        chips=[
            (first_non_empty(row, "llm_recommendation", "recommendation"), "accent"),
            (f"점수 {clean(row.get('llm_fit_score') or row.get('rfp_score'))}", "neutral"),
            (clean(row.get("agency")), "neutral"),
            (clean(row.get("ancm_de")), "neutral"),
        ],
    )

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

    action_left, action_right = st.columns([1, 2])
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
            "분석 정보",
            [
                ("추천 이유", first_non_empty(row, "llm_reason", "reason")),
                ("전략적합도", first_non_empty(row, "llm_score_strategic_fit_score", "strategic_fit_score", "전략적합도")),
                ("전략적합도 사유", first_non_empty(row, "llm_score_strategic_fit_reason", "strategic_fit_reason", "전략적합도사유")),
                ("기술관련도", first_non_empty(row, "llm_score_tech_relevance_score", "tech_relevance_score", "기술관련도")),
                ("기술관련도 사유", first_non_empty(row, "llm_score_tech_relevance_reason", "tech_relevance_reason", "기술관련도사유")),
                ("긴급도", first_non_empty(row, "llm_score_urgency_score", "urgency_score", "긴급도")),
                ("긴급도 사유", first_non_empty(row, "llm_score_urgency_reason", "urgency_reason", "긴급도사유")),
                (
                    "시장정합도",
                    first_non_empty(
                        row,
                        "llm_score_market_alignment_score",
                        "market_alignment_score",
                        "시장정합도",
                    ),
                ),
                (
                    "시장정합도 사유",
                    first_non_empty(
                        row,
                        "llm_score_market_alignment_reason",
                        "market_alignment_reason",
                        "시장정합도사유",
                    ),
                ),
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



def render_summary_detail_from_row(row: dict, opportunity_df: pd.DataFrame) -> None:
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
            "대표 과제 요약",
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

    render_notice_comments(row, section_key=f"summary_{clean(row.get('공고ID'))}")

    st.markdown('<div class="detail-section-title">대표 분석 요약</div>', unsafe_allow_html=True)
    render_detail_card(
        "대표 RFP 분석",
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
    notice_df: pd.DataFrame,
    pending_df: pd.DataFrame,
    opportunity_df: pd.DataFrame,
    summary_df: pd.DataFrame,
    errors_df: pd.DataFrame,
) -> dict[str, pd.DataFrame]:
    notice_view_df = merge_notice_with_analysis(notice_df, opportunity_df)
    return {
        "notice": notice_df,
        "notice_view": notice_view_df,
        "pending": pending_df,
        "opportunity": opportunity_df,
        "summary": summary_df,
        "errors": errors_df,
    }


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
        "nipa_current": nipa_current_df,
        "nipa_current_origin": nipa_current_origin,
        "nipa_past": nipa_past_df,
        "nipa_past_origin": nipa_past_origin,
        "nipa_opportunity": load_nipa_opportunity_df(),
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
    st.subheader(title)

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

    current_view, selected_notice_id = get_route_state(page_key)

    if current_view == "detail":
        st.caption(f"{title} / 상세")
        selected_row = get_row_by_column_value(source_df, "공고ID", selected_notice_id)
        action_col, info_col = st.columns([1, 5])
        with action_col:
            if st.button("테이블로 돌아가기", key=f"{page_key}_back_to_table", use_container_width=True):
                switch_to_table(page_key)
        with info_col:
            st.caption("브라우저 뒤로가기로도 표 화면으로 돌아갈 수 있습니다.")
        render_notice_detail_from_row(selected_row, opportunity_df)
        return

    st.caption(f"왼쪽 상세 보기 버튼으로 상세 페이지로 이동합니다. 현재 {len(filtered)}건")
    render_clickable_table(
        filtered,
        NOTICE_PREFERRED_COLUMNS,
        page_key=page_key,
        id_column="공고ID",
    )


def render_opportunity_page(
    df: pd.DataFrame,
    *,
    page_key: str | None = None,
    title: str | None = None,
    archive: bool = False,
) -> None:
    page_key = page_key or ("opportunity_archive" if archive else "opportunity")
    title = title or ("Opportunity Archive" if archive else "Opportunity")
    st.subheader(title)

    source_df = ensure_opportunity_row_ids(df)
    filtered = filter_archived_opportunity_rows(source_df) if archive else filter_current_opportunity_rows(source_df)
    filter_prefix = "oppty_archive" if archive else "oppty"
    search_text, current_only, status_scope = render_notice_filter_sidebar(
        filter_prefix,
        current_only_default=False if archive else True,
        show_current_only=not archive,
    )
    current_field = "notice_is_current" if "notice_is_current" in filtered.columns else "is_current"
    if current_only and current_field in filtered.columns:
        current_mask = filtered[current_field].fillna("").eq("Y")
        project_mask = filtered["project_name"].fillna("").astype(str).str.strip().ne("") if "project_name" in filtered.columns else False
        incomplete_mask = filtered[current_field].fillna("").astype(str).str.strip().eq("") & project_mask
        filtered = filtered[current_mask | incomplete_mask]
    filtered = filtered[build_opportunity_status_scope_mask(filtered, status_scope)]
    filtered = apply_multiselect_filter(filtered, "전문기관명", "전문기관", f"{filter_prefix}_agency")
    filtered = apply_multiselect_filter(filtered, "소관부처", "소관부처", f"{filter_prefix}_ministry")
    filtered = apply_multiselect_filter(filtered, "검토여부", "검토 여부", f"{filter_prefix}_review")
    filtered = apply_multiselect_filter(filtered, "추천여부", "추천도", f"{filter_prefix}_recommendation")

    if "rfp_score" in filtered.columns and len(filtered) > 0:
        min_score = int(filtered["rfp_score"].min())
        max_score = int(filtered["rfp_score"].max())
        if min_score < max_score:
            score_range = st.sidebar.slider(
                "점수 범위",
                min_value=min_score,
                max_value=max_score,
                value=(min_score, max_score),
                key=f"{filter_prefix}_score_range",
            )
            filtered = filtered[
                (filtered["rfp_score"] >= score_range[0]) &
                (filtered["rfp_score"] <= score_range[1])
            ]
        else:
            st.sidebar.caption(f"점수 고정값: {min_score}")

    filtered = filtered[
        build_contains_mask(
            filtered,
            [
                "notice_title",
                "공고명",
                "project_name",
                "rfp_title",
                "keywords",
                "budget",
                "notice_id",
                "공고번호",
                "file_name",
            ],
            search_text,
        )
    ]

    render_metrics(
        [
            ("Opportunity 수", str(len(filtered))),
            ("추천 건수", str(int((filtered["recommendation"] == "추천").sum()) if "recommendation" in filtered.columns else 0)),
            ("평균 점수", safe_mean(filtered["rfp_score"]) if "rfp_score" in filtered.columns and len(filtered) > 0 else "-"),
            ("공고 수", str(filtered["notice_id"].nunique() if "notice_id" in filtered.columns else 0)),
        ]
    )

    current_view, selected_document_id = get_route_state(page_key)

    if current_view == "detail":
        st.caption(f"{title} / 상세")
        selected_row = get_row_by_column_value(source_df, "_row_id", selected_document_id)
        action_col, info_col = st.columns([1, 5])
        with action_col:
            if st.button("테이블로 돌아가기", key=f"{page_key}_back_to_table", use_container_width=True):
                switch_to_table(page_key)
        with info_col:
            st.caption("브라우저 뒤로가기로도 표 화면으로 돌아갈 수 있습니다.")
        render_opportunity_detail_from_row(selected_row)
        return

    st.caption(f"왼쪽 상세 보기 버튼으로 상세 페이지로 이동합니다. 현재 {len(filtered)}건")
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
    st.subheader("Pending Notice")
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
        st.caption("Pending Notice / 상세")
        selected_row = get_row_by_column_value(source_df, "공고ID", selected_notice_id)
        action_col, info_col = st.columns([1, 5])
        with action_col:
            if st.button("테이블로 돌아가기", key="pending_back_to_table", use_container_width=True):
                switch_to_table(page_key)
        with info_col:
            st.caption("브라우저 뒤로가기로도 표 화면으로 돌아갈 수 있습니다.")
        render_pending_detail_from_row(selected_row)
        return

    st.caption(f"왼쪽 상세 보기 버튼으로 상세 페이지로 이동합니다. 현재 {len(filtered)}건")
    render_clickable_table(
        filtered,
        PENDING_PREFERRED_COLUMNS,
        page_key=page_key,
        id_column="공고ID",
    )


def render_summary_page(df: pd.DataFrame, opportunity_df: pd.DataFrame) -> None:
    st.subheader("Summary")
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
        st.caption("Summary / 상세")
        selected_row = get_row_by_column_value(source_df, "공고ID", selected_notice_id)
        action_col, info_col = st.columns([1, 5])
        with action_col:
            if st.button("테이블로 돌아가기", key="summary_back_to_table", use_container_width=True):
                switch_to_table(page_key)
        with info_col:
            st.caption("브라우저 뒤로가기로도 표 화면으로 돌아갈 수 있습니다.")
        render_summary_detail_from_row(selected_row, opportunity_df)
        return

    st.caption(f"왼쪽 상세 보기 버튼으로 상세 페이지로 이동합니다. 현재 {len(filtered)}건")
    render_clickable_table(
        filtered,
        SUMMARY_PREFERRED_COLUMNS,
        page_key=page_key,
        id_column="공고ID",
    )


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

    st.caption(f"왼쪽 상세 보기 버튼으로 상세 페이지로 이동합니다. 현재 {len(filtered)}건")
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


def render_favorite_notice_page(
    notice_view_df: pd.DataFrame,
    opportunity_df: pd.DataFrame,
    source_datasets: dict[str, object],
) -> None:
    st.subheader("관심 공고")
    page_key = "favorites"
    source_df = build_favorite_notice_df(notice_view_df, source_datasets)

    current_view, selected_id = get_route_state(page_key)
    if current_view == "detail":
        selected_row = get_row_by_column_value(source_df, "_favorite_id", selected_id)
        action_col, info_col = st.columns([1, 5])
        with action_col:
            if st.button("목록으로 돌아가기", key="favorites_back_to_table", use_container_width=True):
                switch_to_table(page_key)
        with info_col:
            st.caption("'관심공고'로 지정된 공고만 모아보는 화면입니다.")
        render_notice_detail_from_row(selected_row, opportunity_df)
        return

    if source_df.empty:
        st.info("'관심공고'로 지정된 공고가 아직 없습니다.")
        return

    filtered = prepare_notice_collection_rows(
        source_df,
        page_key=page_key,
        search_columns=["매체", "공고명", "공고번호", "전문기관", "담당부서", "소관부처", "공고ID"],
        status_default="전체",
        current_only_default=False,
        extra_multiselects=[
            ("공고상태", "공고상태", "status"),
            ("매체", "매체", "source"),
        ],
    )

    render_metrics(
        [
            ("관심 공고 수", str(len(filtered))),
            ("매체 수", str(filtered["매체"].nunique() if "매체" in filtered.columns else 0)),
            ("IRIS", str(int(filtered["매체"].fillna("").astype(str).str.strip().eq("IRIS").sum()))),
            ("중소기업벤처부", str(int(filtered["매체"].fillna("").astype(str).str.strip().eq("중소기업벤처부").sum()))),
            ("NIPA", str(int(filtered["매체"].fillna("").astype(str).str.strip().eq("NIPA").sum()))),
        ]
    )

    st.caption(f"왼쪽 상세 보기 버튼으로 공고 상세 페이지로 이동합니다. 현재 {len(filtered)}건")
    render_clickable_table(
        filtered,
        FAVORITE_NOTICE_COLUMNS,
        page_key=page_key,
        id_column="_favorite_id",
    )


def render_other_crawlers_source_page() -> None:
    st.subheader("Other Crawlers")
    st.info("다른 크롤러 소스는 여기에 확장할 수 있습니다.")


def main(app_mode: str = "admin"):
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
    st.title(mode_config.header_title)
    st.caption(mode_config.header_caption)

    sheet_names = {
        "notice": get_env("NOTICE_MASTER_SHEET", "IRIS_NOTICE_MASTER"),
        "pending": get_env("PENDING_MASTER_SHEET", "IRIS_PENDING_MASTER"),
        "opportunity": (
            get_env("IRIS_OPPORTUNITY_MASTER_SHEET")
            or get_env("OPPORTUNITY_MASTER_SHEET")
            or get_env("MASTER_SHEET", "OPPORTUNITY_MASTER")
        ),
        "summary": get_env("SUMMARY_SHEET", "SUMMARY"),
        "errors": get_env("ERROR_SHEET", "OPPORTUNITY_ERRORS"),
    }

    try:
        notice_df = enrich_notice_df(load_sheet_as_dataframe(sheet_names["notice"]))
        pending_df = enrich_notice_df(load_optional_sheet_as_dataframe(sheet_names["pending"]))
        opportunity_df = enrich_opportunity_df(load_optional_sheet_as_dataframe(sheet_names["opportunity"]))
        opportunity_df = enrich_opportunity_with_notice_meta(opportunity_df, notice_df)
        summary_df = enrich_summary_df(load_optional_sheet_as_dataframe(sheet_names["summary"]))
        summary_df = enrich_summary_with_notice_meta(summary_df, notice_df)
        errors_df = enrich_error_df(load_optional_sheet_as_dataframe(sheet_names["errors"]))
    except Exception as exc:
        st.error(f"시트 로딩 실패: {exc}")
        st.stop()

    datasets = build_app_datasets(
        notice_df=notice_df,
        pending_df=pending_df,
        opportunity_df=opportunity_df,
        summary_df=summary_df,
        errors_df=errors_df,
    )

    source_label_map = get_source_label_map(mode_config)
    source_key_map = get_source_key_map(mode_config)
    source_config_map = get_source_config_map(mode_config)
    current_source = get_query_param("source") or mode_config.default_source
    if current_source not in source_label_map:
        current_source = mode_config.default_source
    source_keys = list(source_label_map.keys())
    source_labels = list(source_label_map.values())
    source_index = source_keys.index(current_source)
    selected_source = st.radio(
        "Source",
        source_labels,
        horizontal=True,
        index=source_index,
    )
    selected_source_key = source_key_map.get(selected_source, mode_config.default_source)
    selected_source_config = source_config_map.get(selected_source_key)

    if selected_source_key != current_source:
        default_page = get_default_page_for_source(mode_config, selected_source_key)
        st.query_params.clear()
        st.query_params.update({
            "source": selected_source_key,
            "page": default_page,
            "view": "table",
        })
        st.rerun()

    source_datasets = None
    if selected_source_config and selected_source_config.requires_source_datasets:
        source_datasets = build_source_datasets()

    render_selected_source(
        selected_source_key,
        source_config=selected_source_config,
        mode_config=mode_config,
        datasets=datasets,
        source_datasets=source_datasets,
    )


if __name__ == "__main__":
    main()
