"""Small display/recommendation fields retained alongside canonical RFP records."""

FIELDS = (
    "candidate_flag", "llm_candidate_flag", "llm_score_candidate_flag", "llm_keywords",
    "llm_project_name", "llm_rfp_title", "document_role", "document_type",
    "llm_document_role", "validation_errors", "rfp_score", "llm_fit_score",
    "llm_reason", "reason", "candidate_reason", "llm_score_candidate_reason",
    "file_name", "manual_archive", "_row_id",
    "project_name_confidence", "llm_project_name_confidence",
)


def discovery_metadata(row):
    # Omitted fields stay omitted, so reconciliation upserts cannot erase them.
    metadata = dict(row.get("discovery_metadata") or {})
    metadata.update({key: str(row[key] or "")[:2000] for key in FIELDS if key in row})
    return metadata
