from __future__ import annotations

from datetime import datetime
import re
from typing import Any

from jobs_common.narajangteo_links import repair_narajangteo_notice_links
from jobs_common.opportunities import is_stale_undated_pending_notice
from jobs_common.schemas import NOTICE_MASTER_HEADER, canonicalize_notice_row


CANONICAL_NOTICE_MASTER_HEADER = [
    "notice_pk",
    "notice_group_id",
    "source_site",
    "source_notice_id",
    "source_notice_url",
    "manual_archive",
    "manual_archive_at",
    "manual_archive_by",
    "manual_archive_reason",
]

CANONICAL_NOTICE_MASTER_HEADER += NOTICE_MASTER_HEADER

NOTICE_VIEW_BASE_HEADER = [
    "notice_pk",
    "notice_group_id",
    "source_site",
    "source_notice_id",
    "source_notice_url",
    "archive_reason",
    "view_status",
    "days_to_deadline",
]

NOTICE_VIEW_HEADER = NOTICE_VIEW_BASE_HEADER + [
    column for column in CANONICAL_NOTICE_MASTER_HEADER
    if column not in {"notice_pk", "notice_group_id", "source_site", "source_notice_id", "source_notice_url"}
]

STATUS_PENDING_KEYS = {"ancmPre"}
STATUS_CLOSED_KEYS = {"ancmCls", "ancmEnd"}
OPEN_STATUS_MARKERS = ("접수중", "진행중", "공고중", "예정")
PENDING_STATUS_MARKERS = ("예정", "pre")
CLOSED_STATUS_MARKERS = ("마감", "종료", "closed", "end")
PERIOD_RE = re.compile(r"(\d{4}[-.]\d{2}[-.]\d{2})\s*~\s*(\d{4}[-.]\d{2}[-.]\d{2})")


def clean(value: Any) -> str:
    return str(value or "").strip()


def parse_date(value: object) -> datetime | None:
    text = clean(value).replace(".", "-")
    if not text:
        return None
    for fmt in ("%Y-%m-%d", "%Y-%m-%d %H:%M:%S"):
        try:
            return datetime.strptime(text, fmt)
        except Exception:
            continue
    return None


def notice_sort_key(row: dict) -> tuple[datetime, str, str, str]:
    sort_dt = (
        parse_date(row.get("registered_at"))
        or parse_date(row.get("updated_at"))
        or datetime.min
    )
    return (
        sort_dt,
        clean(row.get("notice_no")),
        clean(row.get("notice_title")),
        clean(row.get("notice_pk")) or clean(row.get("source_notice_id")) or clean(row.get("notice_id")),
    )


def sort_notice_rows(rows: list[dict]) -> list[dict]:
    return sorted(rows, key=notice_sort_key, reverse=True)


def extract_period_range(period: object) -> tuple[datetime | None, datetime | None]:
    text = clean(period)
    if not text:
        return None, None

    match = PERIOD_RE.search(text)
    if match:
        return parse_date(match.group(1)), parse_date(match.group(2))

    parts = [part.strip() for part in text.split("~") if part.strip()]
    if len(parts) == 2:
        return parse_date(parts[0]), parse_date(parts[1])
    return None, None


def build_notice_pk(row: dict, *, source_site: str = "") -> str:
    canonical = canonicalize_notice_row(row, source_site=source_site)
    resolved_source = clean(canonical.get("source_site")) or clean(source_site)
    source_notice_id = clean(canonical.get("notice_id"))
    if resolved_source and source_notice_id:
        return f"{resolved_source}:{source_notice_id}"
    return ""


def build_notice_group_id(row: dict, *, source_site: str = "") -> str:
    canonical = canonicalize_notice_row(row, source_site=source_site)
    detail_link = clean(canonical.get("detail_link"))
    if detail_link:
        return f"url:{detail_link}"

    title = clean(canonical.get("notice_title"))
    registered_at = clean(canonical.get("registered_at"))
    agency = clean(canonical.get("agency"))
    if title:
        return "||".join(["notice-group", registered_at, agency, title])
    return build_notice_pk(canonical, source_site=source_site)


def normalize_notice_master_row(
    row: dict,
    *,
    source_site: str = "",
    source_notice_url: str = "",
) -> dict:
    canonical = canonicalize_notice_row(row, source_site=source_site)
    normalized = dict(canonical)
    normalized.update(repair_narajangteo_notice_links(row, source_site=source_site or canonical.get("source_site")))
    normalized["notice_pk"] = clean(row.get("notice_pk")) or build_notice_pk(canonical, source_site=source_site)
    normalized["notice_group_id"] = clean(row.get("notice_group_id")) or build_notice_group_id(canonical, source_site=source_site)
    normalized["source_notice_id"] = clean(row.get("source_notice_id")) or clean(canonical.get("notice_id"))
    normalized["source_notice_url"] = (
        clean(normalized.get("source_notice_url"))
        or clean(source_notice_url)
        or clean(canonical.get("detail_link"))
    )
    normalized["manual_archive"] = clean(row.get("manual_archive"))
    normalized["manual_archive_at"] = clean(row.get("manual_archive_at"))
    normalized["manual_archive_by"] = clean(row.get("manual_archive_by"))
    normalized["manual_archive_reason"] = clean(row.get("manual_archive_reason"))
    return normalized


def build_notice_master_row(
    row: dict,
    *,
    source_site: str,
    now: str,
    old_row: dict | None = None,
) -> dict:
    old_row = old_row or {}
    normalized = normalize_notice_master_row(row, source_site=source_site)
    first_seen_at = clean(old_row.get("first_seen_at")) or clean(normalized.get("first_seen_at")) or now
    last_seen_at = now

    merged = {column: clean(normalized.get(column) or old_row.get(column)) for column in CANONICAL_NOTICE_MASTER_HEADER}
    merged["source_site"] = clean(normalized.get("source_site")) or clean(source_site)
    merged["notice_id"] = clean(normalized.get("notice_id"))
    merged["detail_link"] = clean(normalized.get("detail_link"))
    merged["review_status"] = clean(old_row.get("review_status")) or clean(normalized.get("review_status"))
    merged["first_seen_at"] = first_seen_at
    merged["last_seen_at"] = last_seen_at
    merged["updated_at"] = now

    merged["notice_pk"] = clean(old_row.get("notice_pk")) or clean(normalized.get("notice_pk"))
    merged["notice_group_id"] = clean(old_row.get("notice_group_id")) or clean(normalized.get("notice_group_id"))
    merged["source_notice_id"] = clean(normalized.get("source_notice_id"))
    merged["source_notice_url"] = clean(normalized.get("source_notice_url"))
    merged["manual_archive"] = clean(old_row.get("manual_archive")) or clean(normalized.get("manual_archive"))
    merged["manual_archive_at"] = clean(old_row.get("manual_archive_at")) or clean(normalized.get("manual_archive_at"))
    merged["manual_archive_by"] = clean(old_row.get("manual_archive_by")) or clean(normalized.get("manual_archive_by"))
    merged["manual_archive_reason"] = clean(old_row.get("manual_archive_reason")) or clean(normalized.get("manual_archive_reason"))
    return merged


def is_manual_archived(row: dict) -> bool:
    return clean(row.get("manual_archive")).upper() == "Y"


def is_current_notice(row: dict) -> bool:
    return clean(row.get("is_current")).upper() == "Y"


def is_closed_status(row: dict) -> bool:
    status_key = clean(row.get("status_key"))
    if status_key in STATUS_CLOSED_KEYS:
        return True

    status = clean(row.get("status")).lower()
    if any(marker in status for marker in OPEN_STATUS_MARKERS):
        return False
    return any(marker in status for marker in CLOSED_STATUS_MARKERS)


def is_pending_status(row: dict) -> bool:
    status_key = clean(row.get("status_key"))
    if status_key in STATUS_PENDING_KEYS:
        return True

    status = clean(row.get("status")).lower()
    return any(marker in status for marker in PENDING_STATUS_MARKERS)


def has_ended_period(row: dict) -> bool:
    _, end = extract_period_range(row.get("period"))
    if end is None:
        return False
    return end.date() < datetime.now().date()


def days_to_deadline(row: dict) -> str:
    _, end = extract_period_range(row.get("period"))
    if end is None:
        return ""
    return str((end.date() - datetime.now().date()).days)


def classify_notice_view_status(row: dict) -> str:
    if is_manual_archived(row):
        return "archive"
    if clean(row.get("is_current")) and not is_current_notice(row):
        return "archive"
    if has_ended_period(row) or is_closed_status(row) or is_stale_undated_pending_notice(row):
        return "archive"
    if is_pending_status(row):
        return "pending"
    return "current"


def build_notice_view_row(row: dict) -> dict:
    view_status = classify_notice_view_status(row)
    archive_reason = ""
    if is_manual_archived(row):
        archive_reason = "manual_archive"
    elif clean(row.get("is_current")) and not is_current_notice(row):
        archive_reason = "not_current"
    elif has_ended_period(row):
        archive_reason = "ended_period"
    elif is_stale_undated_pending_notice(row):
        archive_reason = "stale_missing_period"
    elif is_closed_status(row):
        archive_reason = "closed_status"

    view_row = {
        "notice_pk": clean(row.get("notice_pk")) or build_notice_pk(row, source_site=clean(row.get("source_site"))),
        "notice_group_id": clean(row.get("notice_group_id")) or build_notice_group_id(row, source_site=clean(row.get("source_site"))),
        "source_site": clean(row.get("source_site")),
        "source_notice_id": clean(row.get("source_notice_id")) or clean(row.get("notice_id")),
        "source_notice_url": clean(row.get("source_notice_url")) or clean(row.get("detail_link")),
        "archive_reason": archive_reason,
        "view_status": view_status,
        "days_to_deadline": days_to_deadline(row),
    }
    for column in NOTICE_MASTER_HEADER:
        if column in {"source_site"}:
            continue
        view_row[column] = clean(row.get(column))
    return view_row


def filter_current_notice_rows(rows: list[dict]) -> list[dict]:
    return [row for row in rows if classify_notice_view_status(row) == "current"]


def filter_pending_notice_rows(rows: list[dict]) -> list[dict]:
    return [row for row in rows if classify_notice_view_status(row) == "pending"]


def filter_archived_notice_rows(rows: list[dict]) -> list[dict]:
    return [row for row in rows if classify_notice_view_status(row) == "archive"]


def build_notice_views(rows: list[dict]) -> dict[str, list[dict]]:
    normalized = [
        normalize_notice_master_row(row, source_site=clean(row.get("source_site")))
        for row in rows
        if clean(row.get("notice_title")) or clean(row.get("title"))
    ]
    return {
        "current": [build_notice_view_row(row) for row in sort_notice_rows(filter_current_notice_rows(normalized))],
        "pending": [build_notice_view_row(row) for row in sort_notice_rows(filter_pending_notice_rows(normalized))],
        "archive": [build_notice_view_row(row) for row in sort_notice_rows(filter_archived_notice_rows(normalized))],
    }
