"""Read-only notice discovery projections. Never writes source or workflow data."""
from __future__ import annotations

from datetime import date, timedelta

import pandas as pd
import re

from jobs_common.notice_views import extract_period_range
from jobs_common.rfp_field_quality import looks_low_value_project_name


IDENTITY = ["source_key", "Notice ID"]


def _text(frame: pd.DataFrame, column: str) -> pd.Series:
    if column not in frame:
        return pd.Series("", index=frame.index, dtype=str)
    return frame[column].fillna("").astype(str).str.strip()


def prepare_notices(notices: pd.DataFrame, *, today: date) -> pd.DataFrame:
    """One display row per source/notice; preserve rows without a stable ID."""
    rows = notices.copy()
    if rows.empty:
        return rows
    rows["_scope_order"] = _text(rows, "Scope").map({"current": 0, "scheduled": 1, "archive": 2}).fillna(3)
    rows = rows.sort_values("_scope_order", kind="stable")
    identified = _text(rows, "source_key").ne("") & _text(rows, "Notice ID").ne("")
    rows = pd.concat([rows[identified].drop_duplicates(IDENTITY), rows[~identified]], ignore_index=True)
    statuses, deadlines = [], []
    for _, row in rows.iterrows():
        status = str(row.get("Status") or "").strip()
        start, end = extract_period_range(row.get("Period") or "")
        # Explicit closure and archive membership take precedence over dates.
        if status == "마감":
            deadline = "마감"
        elif end and end.date() < today:
            status, deadline = "마감", "마감"
        elif start and start.date() > today:
            status, deadline = "예정", "접수 예정"
        elif end:
            days = (end.date() - today).days
            deadline = "오늘 마감" if days == 0 else f"D-{days}"
            if start:
                status = "접수중"
        else:
            deadline = ""
        statuses.append(status)
        deadlines.append(deadline)
    rows["Status"], rows["D-Day"] = statuses, deadlines
    rows["_sort_date"] = pd.to_datetime(_text(rows, "Date"), errors="coerce")
    return rows.sort_values(["_sort_date", "source_key", "Notice ID"], ascending=[False, True, True], na_position="last", kind="stable")


def attach_project_search(notices: pd.DataFrame, projects: pd.DataFrame) -> pd.DataFrame:
    """Index every related task, not only the representative RFP; never its filename."""
    rows = notices.copy()
    rows["_project_search"] = ""
    if rows.empty or projects.empty:
        return rows
    related = pd.DataFrame(index=projects.index)
    related["source_key"] = _text(projects, "source_key")
    related["Notice ID"] = _text(projects, "Notice ID")
    related["_project_search"] = ""
    for column in ("project_name", "rfp_title", "Project", "llm_keywords", "Keywords", "llm_reason", "Reason"):
        related["_project_search"] += " " + _text(projects, column)
    related = related[related["source_key"].ne("") & related["Notice ID"].ne("")]
    lookup = related.groupby(IDENTITY, sort=False)["_project_search"].agg(" ".join)
    rows["_project_search"] = [lookup.get((source, notice_id), "") for source, notice_id in zip(_text(rows, "source_key"), _text(rows, "Notice ID"))]
    return rows


def recommended_notices(notices: pd.DataFrame, candidates: pd.DataFrame) -> pd.DataFrame:
    """Use the existing pipeline's candidate selection, matched to source notices."""
    if notices.empty or candidates.empty:
        return notices.iloc[:0].copy()
    keys = set(zip(_text(candidates, "source_key"), _text(candidates, "Notice ID")))
    matched = pd.Series([
        bool(source and notice_id) and (source, notice_id) in keys
        for source, notice_id in zip(_text(notices, "source_key"), _text(notices, "Notice ID"))
    ], index=notices.index)
    return notices[matched & _text(notices, "Status").isin(["접수중", "마감임박"]) & _text(notices, "Scope").ne("archive")].copy()


def filter_notices(notices: pd.DataFrame, query: str = "", *, status: str = "전체", source: str = "전체") -> pd.DataFrame:
    rows = notices.copy()
    if rows.empty:
        return rows
    if query.strip():
        searchable = pd.Series("", index=rows.index)
        for column in ("Title", "Notice No", "Notice ID", "Agency", "Ministry", "Source", "Summary", "Keywords", "Project", "_project_search"):
            searchable += " " + _text(rows, column)
        # User input is literal text, so inputs such as C++, [, and ( are safe.
        for word in query.split():
            rows = rows[searchable.loc[rows.index].str.contains(word, case=False, regex=False)]
    if status != "전체":
        rows = rows[_text(rows, "Status").eq(status)]
    if source != "전체":
        rows = rows[_text(rows, "source_key").eq(source)]
    return rows.copy()


def latest_notices(notices: pd.DataFrame, *, limit: int = 5) -> pd.DataFrame:
    """Latest dated source notices, independently of recommendation or deadline."""
    if notices.empty:
        return notices.copy()
    dated = notices[notices["_sort_date"].notna()]
    return dated.sort_values("_sort_date", ascending=False, kind="stable").head(limit).copy()


def resolve_recommended_project_title(row, *, project_title=None) -> str:
    """Display fallback only: never replace the persisted RFP identity with a notice title."""
    def text(key):
        value = row.get(key, "")
        return "" if pd.isna(value) else str(value).strip()

    title = text("Project") if project_title is None else str(project_title or "").strip()
    file_name = text("file_name") or text("File Name")
    file_stem = re.sub(r"\.(?:pdf|hwpx?|docx?|zip)$", "", file_name, flags=re.IGNORECASE)
    normalize = lambda value: re.sub(r"[\s_]+", "", value).lower()
    from_file = bool(file_name and normalize(title) in {normalize(file_name), normalize(file_stem)})
    low_confidence = any(
        title == text(field) and text(confidence).lower() == "low"
        for field, confidence in (
            ("project_name", "project_name_confidence"),
            ("llm_project_name", "llm_project_name_confidence"),
        )
    )
    if (title and not looks_low_value_project_name(title) and not from_file and not low_confidence
            and not re.search(r"\.(?:pdf|hwpx?|docx?|zip)$", title, flags=re.IGNORECASE)):
        return title
    if "Notice Title" in row:
        return text("Notice Title")
    return text("notice_title") or text("공고명")


def recommended_rfps(notices: pd.DataFrame, candidates: pd.DataFrame) -> pd.DataFrame:
    """Keep distinct tasks under each eligible notice and use its authoritative title."""
    eligible = recommended_notices(notices, candidates)
    if eligible.empty:
        return candidates.iloc[:0].copy()
    metadata = ["Title", "Status", "Period", "D-Day", "Date", "Agency", "Source"]
    notice_metadata = eligible[IDENTITY + metadata].rename(columns={"Title": "_authoritative_title"})
    rows = candidates.merge(notice_metadata, on=IDENTITY, how="inner", suffixes=("", "_notice"), validate="many_to_one")
    rows["Notice Title"] = rows.pop("_authoritative_title")
    # Resolve after joining the source notice so stale opportunity titles cannot
    # supply the fallback. Source fields, row IDs and review state stay intact.
    rows["notice_title"] = rows["Notice Title"]
    rows["Project"] = rows.apply(resolve_recommended_project_title, axis=1)
    for column in metadata[1:]:
        if f"{column}_notice" in rows:
            rows[column] = rows.pop(f"{column}_notice")
    rows["_sort_date"] = pd.to_datetime(_text(rows, "Date"), errors="coerce")
    rows = rows.sort_values("_sort_date", ascending=False, kind="stable")
    identified = _text(rows, "Project").ne("")
    return pd.concat([rows[identified].drop_duplicates(IDENTITY + ["Project"]), rows[~identified]], ignore_index=True)


def closing_notices(notices: pd.DataFrame, *, today: date, days: int = 7) -> pd.DataFrame:
    """Inclusive D-0 through D-7; unknown, closed and future application periods excluded."""
    if notices.empty:
        return notices.copy()
    rows = notices.copy()
    rows["_closing_date"] = _text(rows, "Period").map(lambda value: extract_period_range(value)[1])
    end_dates = pd.to_datetime(rows["_closing_date"], errors="coerce")
    mask = (
        end_dates.between(pd.Timestamp(today), pd.Timestamp(today + timedelta(days=days)))
        & _text(rows, "Status").isin(["접수중", "마감임박"])
        & _text(rows, "Scope").ne("archive")
    )
    return rows[mask].sort_values(["_closing_date", "_sort_date"], ascending=[True, False], kind="stable").copy()
