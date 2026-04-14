from __future__ import annotations

import json
import os
import re
from html import escape
from pathlib import Path
from urllib.parse import urlencode

import gspread
import pandas as pd
import streamlit as st
from dotenv import load_dotenv
from google.oauth2.service_account import Credentials


BASE_DIR = Path(__file__).resolve().parent
SCOPE = ["https://www.googleapis.com/auth/spreadsheets"]


def clean(value) -> str:
    return str(value or "").strip()


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


def parse_date_column(series: pd.Series) -> pd.Series:
    return pd.to_datetime(series.fillna("").astype(str).str.strip(), errors="coerce")


def to_numeric_column(series: pd.Series) -> pd.Series:
    return pd.to_numeric(series, errors="coerce").fillna(0)


def get_env(name: str, default: str = "") -> str:
    try:
        if name in st.secrets:
            return clean(st.secrets[name])
    except Exception:
        pass
    return clean(os.getenv(name, default))


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
        return json.loads(raw_json)
    return None


def get_gspread_client():
    service_account_info = get_service_account_info()
    if service_account_info:
        creds = Credentials.from_service_account_info(service_account_info, scopes=SCOPE)
        return gspread.authorize(creds)

    credentials_path = get_env("GOOGLE_CREDENTIALS_JSON")
    if not credentials_path:
        raise RuntimeError(
            "Google credentials are not set. Provide gcp_service_account in st.secrets or GOOGLE_CREDENTIALS_JSON."
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
    return gc.open_by_key(sheet_id).worksheet(sheet_name)


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
    for col in df.columns:
        df[col] = df[col].fillna("").astype(str).str.strip()
    return df


def load_optional_sheet_as_dataframe(sheet_name: str) -> pd.DataFrame:
    try:
        return load_sheet_as_dataframe(sheet_name)
    except Exception as exc:
        if "WorksheetNotFound" in str(exc) or "not found" in str(exc).lower():
            return pd.DataFrame()
        raise


def extract_budget_summary(value: str, max_items: int = 3) -> str:
    text = clean(value)
    if not text:
        return ""

    matches = re.findall(r"\d[\d,]*(?:\.\d+)?\s*(?:조원|억원|천만원|백만원|만원|원)", text)
    unique_matches: list[str] = []
    for match in matches:
        normalized = re.sub(r"\s+", "", clean(match))
        if normalized and normalized not in unique_matches:
            unique_matches.append(normalized)
    if unique_matches:
        return ", ".join(unique_matches[:max_items])

    budget_keywords = [
        "정부지원연구개발비",
        "총 정부지원연구개발비",
        "사업비",
        "총사업비",
        "국비",
    ]
    normalized_text = re.sub(r"\s+", " ", text)
    for keyword in budget_keywords:
        index = normalized_text.find(keyword)
        if index >= 0:
            snippet = normalized_text[index:index + 120].strip()
            return snippet

    return normalized_text[:120].strip()


def enrich_notice_df(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    enriched = df.copy()
    if "대표점수" in enriched.columns:
        enriched["대표점수"] = to_numeric_column(enriched["대표점수"])
    else:
        enriched["대표점수"] = 0
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
    enriched["budget"] = series_from_candidates(enriched, ["예산", "budget"]).apply(extract_budget_summary)
    enriched["공고일자"] = series_from_candidates(enriched, ["공고일자", "ancm_de"])
    enriched["공고번호"] = series_from_candidates(enriched, ["공고번호", "ancm_no"])
    enriched["전문기관명"] = series_from_candidates(enriched, ["전문기관명", "전문기관", "agency"])
    enriched["공고명"] = series_from_candidates(enriched, ["공고명", "notice_title"])
    enriched["공고상태"] = series_from_candidates(enriched, ["공고상태", "rcve_status"])
    enriched["접수기간"] = series_from_candidates(enriched, ["접수기간", "period"])
    enriched["상세링크"] = series_from_candidates(enriched, ["상세링크", "detail_link"])
    enriched["review_status"] = series_from_candidates(enriched, ["검토 여부", "검토여부", "review_status"])
    enriched["notice_title"] = series_from_candidates(enriched, ["공고명", "notice_title"])
    enriched["project_name"] = series_from_candidates(enriched, ["과제명", "project_name"])
    enriched["rfp_title"] = series_from_candidates(enriched, ["RFP 제목", "rfp_title"])
    enriched["recommendation"] = series_from_candidates(enriched, ["추천여부", "추천도", "recommendation"])
    enriched["agency"] = series_from_candidates(enriched, ["전문기관명", "전문기관", "agency"])
    enriched["ministry"] = series_from_candidates(enriched, ["소관부처", "ministry"])
    enriched["detail_link"] = series_from_candidates(enriched, ["상세링크", "detail_link"])
    enriched["notice_id"] = series_from_candidates(enriched, ["공고ID", "notice_id"])
    enriched["document_id"] = series_from_candidates(enriched, ["문서ID", "document_id"])
    enriched["keywords"] = series_from_candidates(enriched, ["키워드", "keywords"])
    enriched["reason"] = series_from_candidates(enriched, ["추천이유", "reason"])
    enriched["concept_and_development"] = series_from_candidates(
        enriched, ["개념 및 개발 내용", "concept_and_development"]
    )
    enriched["support_necessity"] = series_from_candidates(
        enriched, ["지원필요성(과제 배경)", "support_necessity"]
    )
    enriched["application_field"] = series_from_candidates(
        enriched, ["활용분야", "application_field"]
    )
    enriched["support_plan"] = series_from_candidates(
        enriched, ["지원기간 및 예산·추진체계", "support_plan"]
    )
    enriched["technical_background"] = series_from_candidates(
        enriched, ["기술개발 배경 및 지원필요성", "technical_background"]
    )
    enriched["development_content"] = series_from_candidates(
        enriched, ["기술개발 내용", "development_content"]
    )
    enriched["support_need"] = series_from_candidates(
        enriched, ["지원필요성", "support_need"]
    )

    if "공고일자" in enriched.columns:
        enriched["_sort_date"] = parse_date_column(enriched["공고일자"])
    else:
        enriched["_sort_date"] = pd.NaT
    return enriched.sort_values(
        by=["_sort_date", "rfp_score", "notice_title", "project_name"],
        ascending=[False, False, True, True],
        na_position="last",
    )


def enrich_opportunity_with_notice_meta(opportunity_df: pd.DataFrame, notice_df: pd.DataFrame) -> pd.DataFrame:
    if opportunity_df.empty:
        return opportunity_df
    if notice_df.empty or "공고ID" not in notice_df.columns:
        return opportunity_df

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
        "상세링크",
        "소관부처",
    ]
    available_columns = [column for column in keep_columns if column in notice_meta.columns]
    notice_meta = notice_meta[available_columns].drop_duplicates(subset=["공고ID"], keep="first")

    enriched = opportunity_df.copy()
    enriched["notice_id"] = series_from_candidates(enriched, ["notice_id", "공고ID"])
    merged = enriched.merge(notice_meta, left_on="notice_id", right_on="공고ID", how="left", suffixes=("", "_notice"))

    fallback_pairs = {
        "공고일자": ["공고일자", "ancm_de"],
        "공고번호": ["공고번호", "ancm_no"],
        "전문기관명": ["전문기관명", "전문기관", "agency"],
        "공고명": ["공고명", "notice_title"],
        "공고상태": ["공고상태", "rcve_status"],
        "접수기간": ["접수기간", "period"],
        "상세링크": ["상세링크", "detail_link"],
        "소관부처": ["소관부처", "ministry"],
    }
    for target, candidates in fallback_pairs.items():
        candidate_columns = [target]
        for candidate in candidates:
            if candidate in merged.columns:
                candidate_columns.append(candidate)
            notice_candidate = f"{candidate}_notice"
            if notice_candidate in merged.columns:
                candidate_columns.append(notice_candidate)
        merged[target] = series_from_candidates(merged, candidate_columns)

    merged["notice_title"] = series_from_candidates(merged, ["notice_title", "공고명"])
    merged["agency"] = series_from_candidates(merged, ["agency", "전문기관", "전문기관명"])
    merged["ministry"] = series_from_candidates(merged, ["ministry", "주관부처"])
    merged["ancm_de"] = series_from_candidates(merged, ["ancm_de", "공고일자"])
    merged["ancm_no"] = series_from_candidates(merged, ["ancm_no", "공고번호"])
    merged["rcve_status"] = series_from_candidates(merged, ["rcve_status", "공고상태"])
    merged["period"] = series_from_candidates(merged, ["period", "접수기간"])
    merged["detail_link"] = series_from_candidates(merged, ["detail_link", "상세링크"])
    merged["review_status"] = series_from_candidates(merged, ["review_status", "검토여부", "검토완료여부"])

    return merged


def enrich_summary_df(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    enriched = df.copy()
    if "대표점수" in enriched.columns:
        enriched["대표점수"] = to_numeric_column(enriched["대표점수"])
    else:
        enriched["대표점수"] = 0
    if "예산" in enriched.columns:
        enriched["예산"] = enriched["예산"].apply(extract_budget_summary)
    if "공고일자" in enriched.columns:
        enriched["_sort_date"] = parse_date_column(enriched["공고일자"])
    else:
        enriched["_sort_date"] = pd.NaT
    return enriched.sort_values(
        by=["_sort_date", "대표점수", "공고명"],
        ascending=[False, False, True],
        na_position="last",
    )


def enrich_summary_with_notice_meta(summary_df: pd.DataFrame, notice_df: pd.DataFrame) -> pd.DataFrame:
    if summary_df.empty:
        return summary_df
    if notice_df.empty or "공고ID" not in summary_df.columns or "공고ID" not in notice_df.columns:
        return summary_df

    notice_meta = notice_df.copy()
    notice_meta["공고ID"] = notice_meta["공고ID"].fillna("").astype(str).str.strip()
    keep_columns = ["공고ID", "상세링크", "전문기관", "소관부처", "공고상태", "접수기간", "공고일자"]
    available_columns = [column for column in keep_columns if column in notice_meta.columns]
    notice_meta = notice_meta[available_columns].drop_duplicates(subset=["공고ID"], keep="first")

    merged = summary_df.copy()
    merged["공고ID"] = merged["공고ID"].fillna("").astype(str).str.strip()
    merged = merged.merge(notice_meta, on="공고ID", how="left", suffixes=("", "_notice"))

    for target in ["상세링크", "전문기관", "소관부처", "공고상태", "접수기간", "공고일자"]:
        candidate_columns = [target]
        notice_target = f"{target}_notice"
        if notice_target in merged.columns:
            candidate_columns.append(notice_target)
        merged[target] = series_from_candidates(merged, candidate_columns)

    return merged


def build_notice_analysis_summary(opportunity_df: pd.DataFrame) -> pd.DataFrame:
    if opportunity_df.empty or "notice_id" not in opportunity_df.columns:
        return pd.DataFrame(
            columns=["공고ID", "대표추천도", "대표점수", "대표과제명", "대표예산", "대표추천이유", "대표키워드"]
        )

    working = opportunity_df.copy()
    working["rfp_score"] = to_numeric_column(working.get("rfp_score", pd.Series(0, index=working.index)))
    recommendation_rank = {"추천": 3, "검토권장": 2, "보통": 1, "비추천": 0}
    working["_recommendation_rank"] = working.get("recommendation", pd.Series("", index=working.index)).map(recommendation_rank).fillna(-1)
    working["_project_name"] = working.get("project_name", pd.Series("", index=working.index)).fillna("").astype(str).str.strip()
    working["_budget"] = working.get("budget", pd.Series("", index=working.index)).fillna("").astype(str).apply(extract_budget_summary)
    working["_reason"] = working.get("reason", pd.Series("", index=working.index)).fillna("").astype(str).str.strip()
    working["_keywords"] = working.get("keywords", pd.Series("", index=working.index)).fillna("").astype(str).str.strip()

    working = working.sort_values(
        by=["notice_id", "rfp_score", "_recommendation_rank", "_project_name"],
        ascending=[True, False, False, True],
        na_position="last",
    )
    best = working.drop_duplicates(subset=["notice_id"], keep="first").copy()

    return pd.DataFrame(
        {
            "공고ID": best["notice_id"].fillna("").astype(str).str.strip(),
            "대표추천도": best.get("recommendation", pd.Series("", index=best.index)).fillna("").astype(str).str.strip(),
            "대표점수": best.get("rfp_score", pd.Series(0, index=best.index)),
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
            mask = mask | df[column].fillna("").astype(str).str.contains(query, case=False, na=False)
    return mask


def find_related_opportunities_for_notice(row: dict, opportunity_df: pd.DataFrame) -> pd.DataFrame:
    if opportunity_df.empty:
        return pd.DataFrame()

    working = opportunity_df.copy()
    notice_id = clean(row.get("공고ID"))
    if notice_id and "notice_id" in working.columns:
        matched = working[working["notice_id"].fillna("").astype(str).str.strip().eq(notice_id)].copy()
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
          font-size: 16px;
          font-weight: 500;
          line-height: 1.6;
          color: #111827;
          word-break: break-word;
          white-space: pre-wrap;
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
          padding: 0;
          border-bottom: 1px solid #f1f5f9;
          vertical-align: middle;
          height: 44px;
          white-space: nowrap;
          overflow: hidden;
          text-overflow: ellipsis;
        }
        .list-table tbody tr:hover {
          background: #f8fbff;
        }
        .list-table tbody td a {
          display: block;
          padding: 12px 14px;
          color: #0f172a;
          text-decoration: none;
          white-space: nowrap;
          overflow: hidden;
          text-overflow: ellipsis;
        }
        .list-table tbody td a:hover {
          color: #2563eb;
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
        text = sanitize_display_text(label, value)
        if not text:
            continue
        items.append(
            f'<div class="detail-field"><div class="detail-label">{escape(label)}</div><div class="detail-value">{escape(text)}</div></div>'
        )

    if not items:
        items.append('<div class="detail-field"><div class="detail-value">표시할 정보가 없습니다.</div></div>')

    st.markdown(
        f'<div class="detail-card"><div class="detail-card-title">{escape(title)}</div>{"".join(items)}</div>',
        unsafe_allow_html=True,
    )


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


def switch_to_table(page_key: str) -> None:
    st.query_params.clear()
    st.query_params.update({"page": page_key, "view": "table"})
    st.rerun()


def compact_table_value(value: str, max_chars: int = 80) -> str:
    text = sanitize_display_text("", value)
    if len(text) <= max_chars:
        return text
    return f"{text[:max_chars].rstrip()}..."


def build_route_href(page_key: str, identifier: str) -> str:
    return f"?{urlencode({'page': page_key, 'view': 'detail', 'id': clean(identifier)})}"


def render_clickable_table(df: pd.DataFrame, preferred_columns: list[str], page_key: str, id_column: str) -> None:
    display_columns = [col for col in preferred_columns if col in df.columns]
    display_columns = [col for col in display_columns if df[col].fillna("").astype(str).str.strip().ne("").any()]
    if not display_columns:
        display_columns = [col for col in df.columns if not col.startswith("_")]

    if df.empty:
        st.info("표시할 데이터가 없습니다.")
        return

    header_html = "".join(f"<th>{escape(column)}</th>" for column in display_columns)
    body_rows = []
    for _, row in df.iterrows():
        identifier = clean(row.get(id_column))
        if not identifier:
            continue
        href = build_route_href(page_key, identifier)
        cells = []
        for column in display_columns:
            value = compact_table_value(row.get(column), 100 if column in {"공고명", "project_name"} else 50)
            cells.append(
                f'<td><a href="{escape(href, quote=True)}" title="{escape(sanitize_display_text(column, row.get(column)))}" target="_self">{escape(value)}</a></td>'
            )
        body_rows.append(f"<tr>{''.join(cells)}</tr>")

    st.markdown(
        f"""
        <div class="list-table-wrap">
          <table class="list-table">
            <thead><tr>{header_html}</tr></thead>
            <tbody>{''.join(body_rows)}</tbody>
          </table>
        </div>
        """,
        unsafe_allow_html=True,
    )


def get_row_by_column_value(df: pd.DataFrame, column: str, value: str) -> dict | None:
    if df.empty or column not in df.columns or not clean(value):
        return None
    matched = df[df[column].fillna("").astype(str).str.strip().eq(clean(value))]
    if matched.empty:
        return None
    return matched.iloc[0].to_dict()


load_dotenv(BASE_DIR / ".env")
