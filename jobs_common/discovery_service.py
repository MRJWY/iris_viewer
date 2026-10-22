"""Cached discovery projections using the existing status and recommendation rules."""
from datetime import date
from collections.abc import Mapping
from time import perf_counter
from urllib.parse import urlencode

import pandas as pd
import streamlit as st

from jobs_common.notice_discovery import attach_project_search, prepare_notices
from jobs_common.discovery_metadata import FIELDS
from jobs_common.repositories.discovery_repository import SOURCE_KEYS, read_discovery_records


DATABASE_URL_KEYS = ("SUPABASE_DB_URL", "SUPABASE_DATABASE_URL", "DATABASE_URL", "POSTGRES_URL")


def resolve_discovery_database_url(get_env, *, secrets=None):
    configured = next((get_env(key) for key in DATABASE_URL_KEYS if get_env(key)), "")
    if configured:
        return configured
    # TOML entries added below a [section] belong to that section. Accept one
    # unambiguous explicitly named DB URL there, without changing other secrets.
    if secrets is None:
        try:
            secrets = st.secrets.to_dict()
        except Exception:
            secrets = {}
    nested = {
        str(section[key]).strip()
        for section in secrets.values() if isinstance(section, Mapping)
        for key in DATABASE_URL_KEYS if section.get(key)
    }
    if len(nested) > 1:
        raise ValueError("Conflicting nested database settings; use a top-level DB URL")
    return next(iter(nested), "")


def uses_supabase_discovery(get_env):
    backend = get_env("DISCOVERY_DATA_BACKEND", "auto").lower()
    if backend == "auto":
        return bool(resolve_discovery_database_url(get_env))
    return backend == "supabase"


def _date_text(value):
    if not value or pd.isna(value):
        return ""
    timestamp = pd.Timestamp(value)
    if timestamp.tzinfo:
        timestamp = timestamp.tz_convert("Asia/Seoul")
    return timestamp.strftime("%Y-%m-%d")


def project_records(notice_records, opportunity_records, *, today, api):
    notices = pd.DataFrame(notice_records, columns=[
        "source_site", "notice_id", "notice_no", "notice_title", "agency", "ministry",
        "registered_at", "period", "status", "status_key", "is_current", "review_status",
        "detail_link", "source_notice_url", "document_link", "updated_at",
    ]).fillna("")
    notices["source_key"] = notices["source_site"].map(SOURCE_KEYS)
    notices["is_current"] = notices["is_current"].map(lambda v: "Y" if v is True else "N" if v is False else v)
    notices["registered_at"] = notices["registered_at"].map(_date_text)
    rows = notices.rename(columns={
        "source_site": "Source", "notice_id": "Notice ID", "notice_no": "Notice No",
        "notice_title": "Title", "agency": "Agency", "ministry": "Ministry",
        "registered_at": "Date", "period": "Period", "status": "Status",
        "review_status": "Review", "detail_link": "Detail Link",
    }).copy()
    rows["Scope"] = "current"
    rows.loc[api.build_notice_archive_mask(notices), "Scope"] = "archive"
    rows["Status"] = rows["Status"].map(lambda v: "접수중" if api.clean(v) == "마감임박" else api.normalize_notice_status_label(v))
    rows = prepare_notices(rows, today=today)

    records = []
    for record in opportunity_records:
        item = dict(record)
        metadata = item.pop("discovery_metadata", None) or {}
        # Metadata never overrides canonical identity, title, review or notice dates.
        item.update({key: value for key, value in metadata.items() if key in FIELDS})
        item["source_key"] = SOURCE_KEYS[item["source_site"]]
        item["Source"] = item["source_site"]
        item["Notice ID"] = item["notice_id"]
        item["Notice Title"] = item["notice_title"]
        if item.get("id") is not None:
            item["Detail Href"] = "?" + urlencode({
                "source": "notices", "page": "notice_queue", "view": "detail",
                "notice_source": item["source_key"], "id": item["notice_id"], "rfp_id": item["id"],
            })
        item["notice_is_current"] = "Y" if item.get("notice_is_current") else "N"
        if item.get("is_archived"):
            item["manual_archive"] = "Y"
        item["registered_at"] = _date_text(item.get("registered_at"))
        item["Keywords"] = item.get("llm_keywords") or item.get("keywords") or ""
        item["Reason"] = item.get("llm_reason") or item.get("summary_excerpt") or ""
        item["rfp_score"] = pd.to_numeric(item.get("rfp_score"), errors="coerce")
        records.append(item)
    opportunities = pd.DataFrame(records).fillna("")
    if not opportunities.empty:
        opportunities["rfp_score"] = pd.to_numeric(opportunities["rfp_score"], errors="coerce")
    candidates = opportunities.copy()
    if not candidates.empty:
        candidates = api.filter_rankable_opportunity_rows(api.filter_current_opportunity_rows(candidates))
        candidates = candidates[api.build_candidate_mask(candidates)].copy()
        if not candidates.empty:
            candidates = api.ensure_opportunity_row_ids(candidates)
            candidates["Row ID"] = candidates["_row_id"]
            candidates["Project"] = candidates.apply(api.choose_display_project_title, axis=1)
    rows = attach_project_search(rows, opportunities)
    return rows, candidates, notices, opportunities


@st.cache_data(ttl=300, show_spinner=False, max_entries=4)
def load_discovery_projection(database_url: str, today: date, *, _api):
    if not database_url:
        raise RuntimeError("Discovery database is not configured")
    started = perf_counter()
    print("[discovery] Supabase read started", flush=True)
    notices, opportunities = read_discovery_records(database_url)
    projection = project_records(notices, opportunities, today=today, api=_api)
    print(f"[discovery] Supabase read ready: notices={len(notices)}, "
          f"opportunities={len(opportunities)}, seconds={perf_counter() - started:.2f}", flush=True)
    return projection
