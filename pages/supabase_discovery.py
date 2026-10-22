"""Database-backed entry for the two discovery routes; legacy workflow routes remain available."""
import pandas as pd

from components.notice_discovery import render_results, render_search
from jobs_common.discovery_service import load_discovery_projection, resolve_discovery_database_url
from jobs_common.notice_discovery import closing_notices, latest_notices, recommended_rfps
from components.discovery_sections import render_lists


def render_page(st, source, *, api):
    today = pd.Timestamp.now(tz="Asia/Seoul").date()
    try:
        with st.spinner("공고를 불러오는 중입니다…"):
            rows, candidates, notices, opportunities = load_discovery_projection(
                resolve_discovery_database_url(api.get_env), today, _api=api,
            )
    except Exception as exc:
        # Connection exceptions can contain credentials/hostnames. Never display them.
        print(f"[discovery] load failed: {type(exc).__name__}", flush=True)
        st.error("공고 데이터에 연결하지 못했습니다. 잠시 후 다시 시도해 주세요.")
        if st.button("다시 불러오기", key="discovery_db_retry"):
            load_discovery_projection.clear()
            st.rerun()
        return

    if source == "dashboard":
        render_lists(st, latest_notices(rows), recommended_rfps(rows, candidates),
                     closing_notices(rows, today=today), all_notices=rows, today=today, api=api)
        return

    notice_id = api.clean(api.get_query_param("id"))
    if api.get_query_param("view") in {"detail", "summary"} and notice_id:
        selected_source = api.clean(api.get_query_param("notice_source"))
        selected = notices[notices["notice_id"].eq(notice_id)]
        if selected_source:
            selected = selected[selected["source_key"].eq(selected_source)]
        api._render_same_tab_link_button("← 목록으로", api.build_table_return_href("notice_queue", source_key="notices"), key="notice_discovery_back")
        if len(selected) != 1:
            st.info("공고를 찾을 수 없습니다. 목록으로 돌아가 다시 검색해 주세요.")
            return
        row = selected.iloc[0].to_dict()
        # The established detail/workflow renderer still consumes these aliases.
        for original, alias in (("notice_id", "공고ID"), ("notice_title", "공고명"),
                                ("notice_no", "공고번호"), ("period", "접수기간"),
                                ("status", "공고상태"), ("review_status", "검토 여부")):
            row[alias] = row.get(original, "")
        related = opportunities
        if not related.empty:
            related = related[related["source_key"].eq(row["source_key"]) & related["notice_id"].eq(notice_id)]
        selected_rfp = api.clean(api.get_query_param("rfp_id"))
        if selected_rfp:
            selected_tasks = related[related["id"].astype(str).eq(selected_rfp)] if "id" in related else related.iloc[:0]
            if selected_tasks.empty:
                st.info("선택한 RFP를 찾을 수 없습니다.")
            else:
                task = selected_tasks.iloc[0]
                st.subheader(api.choose_display_project_title(task) or "과제명 미확인")
                st.caption("선택한 RFP")
                st.dataframe(pd.DataFrame([
                    {"항목": label, "내용": api.clean(task.get(field)) or "미확인"}
                    for label, field in (("키워드", "Keywords"), ("예산", "budget"),
                                         ("과제 수행기간", "rfp_period"), ("신청 자격", "eligibility"),
                                         ("분석 요약", "Reason"))
                ]), hide_index=True, use_container_width=True)
        # Existing detail actions retain their established review/comment storage.
        api.render_notice_detail_from_row(row, related)
        return

    st.markdown(
        '<section class="discovery-heading"><div class="discovery-eyebrow">공고 모아보기</div>'
        '<h1>전체 공고 검색</h1><p>수집된 모든 공고를 한 곳에서. 접수 중인 공고부터 마감된 공고까지 검색하세요.</p></section>',
        unsafe_allow_html=True,
    )
    filtered = render_search(st, rows, prefix="all_notices")
    render_results(st, filtered, prefix="all_notices", api=api)
