from jobs_common.narajangteo_links import repair_narajangteo_notice_links


NOTICE_CURRENT_HEADER = [
    "source_site",
    "notice_id",
    "notice_no",
    "notice_title",
    "business_name",
    "ministry",
    "agency",
    "registered_at",
    "period",
    "status",
    "status_key",
    "detail_link",
    "document_link",
    "views",
    "author",
    "d_day",
    "page",
    "row_number",
    "review_status",
    "is_current",
    "updated_at",
]

NOTICE_MASTER_HEADER = [
    "source_site",
    "notice_id",
    "notice_no",
    "notice_title",
    "business_name",
    "ministry",
    "agency",
    "registered_at",
    "period",
    "status",
    "status_key",
    "detail_link",
    "document_link",
    "views",
    "author",
    "d_day",
    "page",
    "row_number",
    "review_status",
    "first_seen_at",
    "last_seen_at",
    "is_current",
    "updated_at",
]

NOTICE_FIELD_ALIASES = {
    "source_site": ["source_site", "출처사이트"],
    "notice_id": ["notice_id", "공고ID"],
    "notice_no": ["notice_no", "ancm_no", "공고번호"],
    "notice_title": ["notice_title", "title", "공고명"],
    "business_name": ["business_name", "project_name", "사업명"],
    "ministry": ["ministry", "소관부처"],
    "agency": ["agency", "department", "전문기관", "전문기관명", "담당부서"],
    "registered_at": ["registered_at", "ancm_de", "공고일자", "등록일"],
    "period": ["period", "접수기간", "신청기간"],
    "status": ["status", "rcve_status", "공고상태", "상태"],
    "status_key": ["status_key", "상태키"],
    "detail_link": ["detail_link", "상세링크"],
    "document_link": ["document_link", "document_url", "download_url", "문서링크", "다운로드링크"],
    "views": ["views", "조회"],
    "author": ["author", "작성자"],
    "d_day": ["d_day", "남은신청기간"],
    "page": ["page"],
    "row_number": ["row_number"],
    "review_status": ["review_status", "검토 여부", "검토여부"],
    "first_seen_at": ["first_seen_at"],
    "last_seen_at": ["last_seen_at"],
    "is_current": ["is_current"],
    "updated_at": ["updated_at"],
}


def clean(value) -> str:
    return str(value or "").strip()


def pick_first(row: dict, candidates: list[str]) -> str:
    for key in candidates:
        value = clean(row.get(key))
        if value:
            return value
    return ""


def canonicalize_notice_row(row: dict, *, source_site: str = "") -> dict:
    normalized = {
        field: pick_first(row, aliases)
        for field, aliases in NOTICE_FIELD_ALIASES.items()
    }
    if source_site and not normalized["source_site"]:
        normalized["source_site"] = source_site
    normalized = {
        **normalized,
        **repair_narajangteo_notice_links(normalized, source_site=normalized.get("source_site") or source_site),
    }
    return normalized


def canonicalize_notice_rows(rows: list[dict], *, source_site: str = "") -> list[dict]:
    return [canonicalize_notice_row(row, source_site=source_site) for row in rows]


def row_for_header(row: dict, header: list[str], *, source_site: str = "") -> dict:
    canonical = canonicalize_notice_row(row, source_site=source_site)
    merged = {**row, **canonical}
    return {column: merged.get(column, "") for column in header}
