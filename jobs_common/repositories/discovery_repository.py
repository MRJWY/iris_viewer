"""Read-only, source-scoped Supabase reads for the discovery workspace."""
from jobs_common import db


SOURCE_KEYS = {"IRIS": "iris", "MSS": "tipa", "NIPA": "nipa", "BIPA": "bipa", "BIZINFO": "bizinfo"}


def read_discovery_records(database_url: str):
    """Two compact reads in one consistent snapshot; never retrieve document bodies."""
    db._require_psycopg()
    with db.psycopg.connect(
        database_url, row_factory=db.dict_row, connect_timeout=8,
        options="-c default_transaction_read_only=on -c statement_timeout=15000",
    ) as connection:
        with connection.cursor() as cursor:
            cursor.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
            cursor.execute("""
                SELECT source_site, notice_id, notice_no, notice_title, agency, ministry,
                       registered_at, period, status, status_key, is_current, review_status,
                       detail_link, source_notice_url, document_link, updated_at
                FROM notices WHERE source_site = ANY(%s)
            """, (list(SOURCE_KEYS),))
            notices = cursor.fetchall()
            cursor.execute("""
                SELECT o.id, o.source_site, o.notice_id, o.project_name, o.rfp_title,
                       o.document_id, o.keywords, o.budget, o.rfp_period, o.eligibility,
                       o.review_status, o.is_archived, o.summary_excerpt, o.discovery_metadata,
                       n.notice_title, n.period AS notice_period, n.status AS notice_status,
                       n.is_current AS notice_is_current, n.registered_at, n.agency,
                       n.detail_link, n.source_notice_url,
                       d.document_role, d.document_type, d.file_name
                FROM opportunities o
                JOIN notices n ON n.source_site=o.source_site AND n.notice_id=o.notice_id
                LEFT JOIN documents d ON d.document_id=o.document_id
                     AND d.source_site=o.source_site AND d.notice_id=o.notice_id
                WHERE o.source_site = ANY(%s)
            """, (list(SOURCE_KEYS),))
            return notices, cursor.fetchall()
