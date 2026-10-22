from datetime import date
from unittest.mock import patch

import shared_app as core
from jobs_common.discovery_service import project_records
from pages.discovery_workspace import ViewerDiscoveryApi


def projection():
    notices = [dict(source_site="IRIS", notice_id="n1", notice_title="Authoritative notice",
                    period="2026-10-01 ~ 2026-10-28", status="마감", is_current=False,
                    registered_at="2026-10-20", agency="Agency")]
    tasks = [dict(id=1, source_site="IRIS", notice_id="n1", notice_title="Authoritative notice",
                  project_name="Edge task", notice_period=notices[0]["period"],
                  notice_status="마감", notice_is_current=False, is_archived=True,
                  registered_at="2026-10-20", keywords="edge", review_status="검토완료",
                  discovery_metadata={"candidate_flag": "N", "document_role": "RFP"})]
    return project_records(notices, tasks, today=date(2026, 10, 22), api=ViewerDiscoveryApi())


def viewer_fixture(authenticated=True):
    from unittest.mock import patch
    import streamlit as st
    import public_viewer_app
    import shared_app as core
    from test_supabase_viewer import projection

    def login(mode):
        assert mode.mode == "viewer"
        if not authenticated:
            st.info("LOGIN REQUIRED")
            st.stop()
        st.session_state["auth_user"] = {"user_id": "test-viewer"}
        st.session_state["auth_token"] = "fixture-signed-session"

    with patch("public_viewer_app.core.require_login", side_effect=login), \
         patch("jobs_common.discovery_service.uses_supabase_discovery", return_value=True), \
         patch("pages.supabase_discovery.load_discovery_projection", return_value=projection()) as loader, \
         patch.object(core, "load_app_datasets", side_effect=AssertionError("No Sheets dataset reads")), \
         patch.object(core, "build_source_datasets", side_effect=AssertionError("No Sheets dataset reads")), \
         patch.object(core, "render_favorite_scrap_button"):
        public_viewer_app.main()
        if not authenticated:
            assert not loader.called


def test_cards_all_notices_search_and_auth_links():
    from streamlit.testing.v1 import AppTest
    at = AppTest.from_function(viewer_fixture).run(timeout=20)
    assert not at.exception and not at.error
    html = "\n".join(x.value for x in at.markdown)
    assert "전체 공고 1건" in html and "source=notices" in html
    assert "auth=fixture-signed-session" in html
    assert "Edge task" not in html
    at.query_params["dashboard_section"] = "latest"
    at.run(timeout=20)
    assert not at.exception
    html = "\n".join(x.value for x in at.markdown)
    assert "Authoritative notice" in html and "return_to=" in html
    at.text_input[0].set_value("edge")
    at.button[0].click().run(timeout=20)
    assert not at.exception
    assert at.query_params["source"] == ["notices"]
    assert at.query_params["auth"] == ["fixture-signed-session"]
    assert any("1건" in c.value for c in at.caption)


def test_login_still_blocks_discovery():
    from streamlit.testing.v1 import AppTest
    at = AppTest.from_function(viewer_fixture, kwargs={"authenticated": False}).run(timeout=20)
    assert not at.exception
    assert at.info[0].value == "LOGIN REQUIRED"
    assert not at.text_input


def test_db_detail_and_return_stay_on_discovery():
    from streamlit.testing.v1 import AppTest
    at = AppTest.from_function(viewer_fixture)
    at.query_params.update(source="notices", page="notice_queue", view="detail", id="n1", notice_source="iris", rfp_id="1")
    at.run(timeout=20)
    assert not at.exception and not at.error
    assert any(x.label == "댓글 불러오기" for x in at.button)
    assert at.subheader[0].value == "Edge task"


def test_all_notices_keep_archived_records_and_exclude_them_from_recommendations():
    rows, candidates, _, tasks = projection()
    assert rows.iloc[0]["Status"] == "마감"
    assert rows.iloc[0]["Title"] == "Authoritative notice"
    assert candidates.empty
    assert len(tasks) == 1
