from datetime import datetime, timedelta
import re

from jobs_common.schemas import canonicalize_notice_row


CLOSED_STATUS_KEYS = {"ancmCls", "ancmEnd"}
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
ARCHIVE_REVIEW_STATUS_VALUES = {"완료", "검토완료"}

PERIOD_RE = re.compile(r"(\d{4}[-.]\d{2}[-.]\d{2})\s*~\s*(\d{4}[-.]\d{2}[-.]\d{2})")


OPPORTUNITY_HEADER = [
    "created_at",
    "updated_at",
    "document_id",
    "source_site",
    "notice_id",
    "notice_no",
    "notice_title",
    "business_name",
    "project_name",
    "rfp_title",
    "keywords",
    "target_market",
    "rfp_score",
    "budget",
    "recommendation",
    "rfp_period",
    "eligibility",
    "strategic_fit_score",
    "strategic_fit_reason",
    "tech_relevance_score",
    "tech_relevance_reason",
    "urgency_score",
    "urgency_reason",
    "market_alignment_score",
    "market_alignment_reason",
    "software_delivery_fit_score",
    "software_delivery_fit_reason",
    "hardware_dominance_score",
    "hardware_dominance_reason",
    "candidate_flag",
    "candidate_reason",
    "why_interesting",
    "risk_notes",
    "next_action",
    "review_priority",
    "agency",
    "ministry",
    "registered_at",
    "period",
    "status",
    "status_key",
    "detail_link",
    "document_link",
    "review_status",
    "manual_archive",
    "manual_archive_at",
    "manual_archive_by",
    "manual_archive_reason",
    "archive_reason",
    "archive_reason_label",
    "is_current",
    "notice_status",
    "notice_is_current",
    "notice_period",
    "document_role",
    "file_name",
    "file_type",
    "file_path",
    "document_type",
    "notice_id_source",
    "llm_enriched",
    "llm_mode",
    "project_name_source",
    "project_name_confidence",
    "rfp_title_source",
    "evidence",
    "conflict_flags",
    "search_summary_index",
    "reason",
    "concept_and_development",
    "support_necessity",
    "application_field",
    "support_plan",
    "technical_background",
    "development_content",
    "support_need",
    "special_notes",
    "validation_errors",
]


def clean(value) -> str:
    return str(value or "").strip()


def is_closed_status_value(value: object) -> bool:
    text = clean(value).lower()
    if not text:
        return False
    if any(marker in text for marker in OPEN_STATUS_MARKERS):
        return False
    compact_text = "".join(text.split())
    compact_closed_values = {"".join(value.split()) for value in CLOSED_STATUS_VALUES}
    return (
        text in CLOSED_STATUS_VALUES
        or compact_text in compact_closed_values
        or compact_text.endswith("마감")
        or compact_text.endswith("종료")
    )


def is_closed_opportunity_row(row: dict) -> bool:
    status_key = clean(row.get("status_key"))
    if status_key in CLOSED_STATUS_KEYS:
        return True

    return any(
        is_closed_status_value(row.get(key))
        for key in ("status", "rcve_status", "notice_status", "공고상태")
    )


def has_missing_opportunity_period(row: dict) -> bool:
    return not any(
        clean(row.get(key))
        for key in ("notice_period", "period", "접수기간", "신청기간")
    )


def is_archived_review_status_value(value: object) -> bool:
    return clean(value).replace(" ", "") in {
        review_status.replace(" ", "")
        for review_status in ARCHIVE_REVIEW_STATUS_VALUES
    }


def has_archived_review_status(row: dict) -> bool:
    return any(
        is_archived_review_status_value(row.get(key))
        for key in ("review_status", "검토 여부", "검토여부")
    )


def parse_date_order(value: str) -> datetime:
    text = clean(value).replace(".", "-")
    if not text:
        return datetime(1900, 1, 1)
    for fmt in ("%Y-%m-%d", "%Y-%m-%d %H:%M:%S"):
        try:
            return datetime.strptime(text, fmt)
        except Exception:
            continue
    return datetime(1900, 1, 1)


def extract_period_end(value: object) -> datetime | None:
    text = clean(value)
    if not text:
        return None

    match = PERIOD_RE.search(text)
    if match:
        parsed = parse_date_order(match.group(2))
        return parsed if parsed.year > 1900 else None

    parts = [part.strip() for part in text.split("~") if part.strip()]
    if len(parts) == 2:
        parsed = parse_date_order(parts[1])
        return parsed if parsed.year > 1900 else None

    return None


def has_ended_period(row: dict) -> bool:
    today = datetime.now().date()
    for key in ("notice_period", "period", "접수기간", "신청기간"):
        end_date = extract_period_end(row.get(key))
        if end_date is None:
            continue
        return end_date.date() < today
    return False


def is_stale_undated_pending_notice(row: dict, *, now: datetime | None = None) -> bool:
    """Exclude old undated IRIS pre-announcements from current views.

    The source's ``is_current`` flag only says that the notice was seen in a
    crawled list. It does not establish an application deadline.
    """
    if clean(row.get("status_key")) != "ancmPre":
        return False
    if any(clean(row.get(key)) for key in ("notice_period", "period", "접수기간", "신청기간")):
        return False
    registered_at = clean(row.get("registered_at")) or clean(row.get("ancm_de"))
    match = re.match(r"^(\d{4})[-./](\d{1,2})[-./](\d{1,2})", registered_at)
    if not match:
        return False
    try:
        registered_date = datetime(*(int(part) for part in match.groups())).date()
    except ValueError:
        return False
    return registered_date < (now or datetime.now()).date() - timedelta(days=365)


def is_manual_archived(row: dict) -> bool:
    return clean(row.get("manual_archive")).upper() == "Y"


ARCHIVE_REASON_LABELS = {
    "notice_closed": "공고 마감",
    "application_closed": "접수 마감",
    "manual_archive": "수동 보관",
    "review_archived": "검토 보관",
}


def derive_archive_reason(row: dict) -> str:
    if is_manual_archived(row):
        return "manual_archive"
    if has_archived_review_status(row):
        return "review_archived"
    if has_ended_period(row):
        current_value = clean(row.get("notice_is_current")) or clean(row.get("is_current"))
        if current_value == "N" or is_closed_opportunity_row(row):
            return "notice_closed"
        return "application_closed"
    return ""


def derive_archive_reason_label(row: dict) -> str:
    return ARCHIVE_REASON_LABELS.get(derive_archive_reason(row), "")


def build_notice_opportunity_id(row: dict, *, source_site: str) -> str:
    canonical = canonicalize_notice_row(row, source_site=source_site)
    notice_id = clean(canonical.get("notice_id"))
    if notice_id:
        return f"{source_site}:notice:{notice_id}"

    detail_link = clean(canonical.get("detail_link"))
    if detail_link:
        return f"{source_site}:notice:{detail_link}"

    return "||".join(
        [
            source_site,
            "notice",
            clean(canonical.get("registered_at")),
            clean(canonical.get("notice_no")),
            clean(canonical.get("notice_title")),
        ]
    )


def build_notice_opportunity_row(
    row: dict,
    *,
    source_site: str,
    now: str,
    old_row: dict | None = None,
) -> dict:
    canonical = canonicalize_notice_row(row, source_site=source_site)
    old_row = old_row or {}

    document_id = build_notice_opportunity_id(row, source_site=source_site)
    created_at = clean(old_row.get("created_at")) or clean(row.get("first_seen_at")) or now
    review_status = clean(old_row.get("review_status")) or clean(canonical.get("review_status"))
    is_current = clean(canonical.get("is_current"))

    validation_errors = []
    if not clean(canonical.get("notice_id")):
        validation_errors.append("missing_notice_id")
    if not clean(canonical.get("notice_title")):
        validation_errors.append("missing_notice_title")

    return {
        "created_at": created_at,
        "updated_at": now,
        "document_id": document_id,
        "source_site": source_site,
        "notice_id": clean(canonical.get("notice_id")),
        "notice_title": clean(canonical.get("notice_title")),
        "notice_no": clean(canonical.get("notice_no")),
        "project_name": "",
        "rfp_title": "",
        "agency": clean(canonical.get("agency")),
        "ministry": clean(canonical.get("ministry")),
        "registered_at": clean(canonical.get("registered_at")),
        "period": clean(canonical.get("period")),
        "status": clean(canonical.get("status")),
        "status_key": clean(canonical.get("status_key")),
        "detail_link": clean(canonical.get("detail_link")),
        "document_link": clean(canonical.get("document_link") or row.get("document_link")),
        "review_status": review_status,
        "archive_reason": "",
        "archive_reason_label": "",
        "is_current": is_current,
        "notice_status": clean(canonical.get("status")),
        "notice_is_current": is_current,
        "notice_period": clean(canonical.get("period")),
        "document_role": "notice_only",
        "validation_errors": " | ".join(validation_errors),
    }


def build_notice_opportunity_rows(
    notice_rows: list[dict],
    existing_rows: list[dict],
    *,
    source_site: str,
    now: str | None = None,
) -> list[dict]:
    timestamp = now or datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    existing_by_id = {
        clean(row.get("document_id")): dict(row)
        for row in existing_rows
        if clean(row.get("document_id"))
    }

    opportunity_rows = []
    for notice_row in notice_rows:
        document_id = build_notice_opportunity_id(notice_row, source_site=source_site)
        old_row = existing_by_id.get(document_id, {})
        opportunity_rows.append(
            build_notice_opportunity_row(
                notice_row,
                source_site=source_site,
                now=timestamp,
                old_row=old_row,
            )
        )

    opportunity_rows.sort(
        key=lambda row: (
            clean(row.get("is_current")) != "Y",
            -parse_date_order(row.get("registered_at")).toordinal(),
            clean(row.get("notice_title")),
            clean(row.get("document_id")),
        )
    )
    return opportunity_rows


def opportunity_sort_key(row: dict) -> tuple:
    current_value = clean(row.get("notice_is_current")) or clean(row.get("is_current"))
    return (
        current_value != "Y",
        -parse_date_order(clean(row.get("registered_at"))).toordinal(),
        clean(row.get("notice_title")),
        clean(row.get("project_name")),
        clean(row.get("document_id")),
    )


def filter_archived_opportunity_rows(rows: list[dict]) -> list[dict]:
    return [row for row in rows if not is_current_opportunity_row(row)]


def is_current_opportunity_row(row: dict) -> bool:
    return not (
        is_manual_archived(row)
        or has_archived_review_status(row)
        or is_closed_opportunity_row(row)
        or has_missing_opportunity_period(row)
        or has_ended_period(row)
        or is_stale_undated_pending_notice(row)
        or clean(row.get("archive_reason")) in {"invalid_project_name", "missing_project_name"}
        or (clean(row.get("source_site")).upper() == "IRIS" and not clean(row.get("project_name")))
    )


def filter_current_opportunity_rows(rows: list[dict]) -> list[dict]:
    return [row for row in rows if is_current_opportunity_row(row)]


def upsert_archive_opportunity_rows(existing_rows: list[dict], new_rows: list[dict]) -> list[dict]:
    merged = {}

    for row in existing_rows:
        document_id = clean(row.get("document_id"))
        if not document_id:
            continue
        merged[document_id] = dict(row)

    for row in new_rows:
        document_id = clean(row.get("document_id"))
        if not document_id:
            continue

        old_row = merged.get(document_id, {})
        merged_row = dict(old_row)
        merged_row.update(row)
        merged_row["document_id"] = document_id
        merged_row["created_at"] = clean(old_row.get("created_at")) or clean(row.get("created_at")) or clean(row.get("updated_at"))
        merged_row["updated_at"] = clean(row.get("updated_at")) or clean(old_row.get("updated_at")) or merged_row["created_at"]
        merged_row["review_status"] = clean(old_row.get("review_status")) or clean(row.get("review_status"))
        merged_row["manual_archive"] = clean(row.get("manual_archive")) or clean(old_row.get("manual_archive"))
        merged_row["manual_archive_at"] = clean(row.get("manual_archive_at")) or clean(old_row.get("manual_archive_at"))
        merged_row["manual_archive_by"] = clean(row.get("manual_archive_by")) or clean(old_row.get("manual_archive_by"))
        merged_row["manual_archive_reason"] = clean(row.get("manual_archive_reason")) or clean(old_row.get("manual_archive_reason"))
        merged_row["archive_reason"] = derive_archive_reason(merged_row)
        merged_row["archive_reason_label"] = derive_archive_reason_label(merged_row)
        merged[document_id] = merged_row

    rows = list(merged.values())
    rows.sort(key=opportunity_sort_key)
    return rows
