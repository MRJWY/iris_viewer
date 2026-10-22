"""Discovery selection rules mirrored from the canonical admin runtime.

Legacy viewer workflows continue to use shared_app's original rules.
"""
import pandas as pd
from shared_app import series_from_candidates, extract_period_end, normalize_notice_status_label, is_candidate_value

def build_period_ended_mask(df: pd.DataFrame, candidate_columns: list[str]) -> pd.Series:
    if df.empty:
        return pd.Series(dtype="bool")

    period_series = series_from_candidates(df, candidate_columns)
    period_end = period_series.apply(extract_period_end)
    today = pd.Timestamp.now().normalize()
    return period_end.notna() & period_end.dt.normalize().lt(today)


def build_not_current_mask(df: pd.DataFrame, candidate_columns: list[str]) -> pd.Series:
    if df.empty:
        return pd.Series(dtype="bool")

    current_series = series_from_candidates(df, candidate_columns).fillna("").astype(str).str.strip().str.upper()
    return current_series.eq("N")


def build_closed_status_mask(df: pd.DataFrame, candidate_columns: list[str]) -> pd.Series:
    if df.empty:
        return pd.Series(dtype="bool")

    status_series = series_from_candidates(df, candidate_columns)
    normalized = status_series.fillna("").astype(str).apply(normalize_notice_status_label)
    compact = normalized.str.replace(r"\s+", "", regex=True).str.lower()
    return (
        normalized.eq("마감")
        | compact.isin({"마감", "종료", "closed", "end", "ended", "접수마감", "신청마감", "공고마감", "ancmcls", "ancmend"})
        | compact.str.endswith("마감")
        | compact.str.endswith("종료")
        | compact.str.contains("closed|ended|ancmcls|ancmend", regex=True)
    )


def build_meaningful_opportunity_mask(df: pd.DataFrame) -> pd.Series:
    if df.empty:
        return pd.Series(dtype="bool")

    document_role = series_from_candidates(df, ["document_role", "문서역할", "document_type", "문서유형"])
    document_role = document_role.fillna("").astype(str).str.strip().str.upper()
    mask = document_role.eq("RFP")

    project_name = series_from_candidates(df, ["project_name", "과제명"])
    rfp_title = series_from_candidates(df, ["rfp_title", "RFP 제목"])
    budget = series_from_candidates(df, ["budget", "예산"])
    reason = series_from_candidates(df, ["reason", "추천사유"])
    score = pd.to_numeric(series_from_candidates(df, ["rfp_score", "점수"]), errors="coerce").fillna(0)

    content_mask = (
        project_name.fillna("").astype(str).str.strip().ne("")
        | rfp_title.fillna("").astype(str).str.strip().ne("")
        | budget.fillna("").astype(str).str.strip().ne("")
        | (reason.fillna("").astype(str).str.strip().ne("") & score.gt(0))
    )
    return mask & content_mask


def build_opportunity_archive_mask(df: pd.DataFrame) -> pd.Series:
    if df.empty:
        return pd.Series(dtype="bool")

    ended_mask = build_period_ended_mask(df, ["notice_period", "접수기간", "신청기간", "period"])
    not_current_mask = build_not_current_mask(df, ["notice_is_current", "is_current"])
    manual_mask = series_from_candidates(df, ["manual_archive"]).fillna("").astype(str).str.strip().str.upper().eq("Y")
    closed_mask = build_closed_status_mask(
        df,
        ["notice_status", "공고상태", "status", "rcve_status"],
    )
    return ended_mask | not_current_mask | manual_mask | closed_mask


def build_current_opportunity_mask(df: pd.DataFrame) -> pd.Series:
    if df.empty:
        return pd.Series(dtype="bool")

    return build_meaningful_opportunity_mask(df) & ~build_opportunity_archive_mask(df)


def filter_current_opportunity_rows(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    return df[build_current_opportunity_mask(df)].copy()


def build_candidate_mask(df: pd.DataFrame) -> pd.Series:
    if df.empty:
        return pd.Series(dtype=bool)
    candidate = series_from_candidates(
        df,
        ["candidate_flag", "확장후보여부", "llm_candidate_flag", "llm_score_candidate_flag", "Candidate"],
    )
    keywords = series_from_candidates(
        df,
        ["keywords", "llm_keywords", "대표키워드", "Keywords"],
    ).fillna("").astype(str).str.strip()
    return candidate.apply(is_candidate_value) | keywords.ne("")


def build_notice_archive_mask(df: pd.DataFrame) -> pd.Series:
    if df.empty:
        return pd.Series(dtype="bool")
    ended_mask = build_period_ended_mask(df, ["접수기간", "신청기간", "period", "notice_period"])
    not_current_mask = build_not_current_mask(df, ["is_current", "notice_is_current"])
    closed_mask = build_closed_status_mask(
        df,
        ["공고상태", "상태", "status", "rcve_status", "notice_status"],
    )
    return ended_mask | not_current_mask | closed_mask
