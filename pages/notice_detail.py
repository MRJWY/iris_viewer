from __future__ import annotations

"""Legacy notice detail page shim.

The active public viewer runtime now delegates to the repository-root `app.py`.
Prefer updating the root app first when changing notice detail behavior or UI.
"""

import pandas as pd

from components.detail_blocks import (
    first_present,
    inject_detail_workspace_styles,
    present_value,
    render_detail_fact_rows_card,
    render_detail_header_card,
    render_detail_outline_card,
    render_detail_related_panel_card,
    truncate,
)


def _status_badge_tone(status: str) -> str:
    normalized = (status or "").strip()
    if normalized == "접수중":
        return "primary"
    if normalized == "마감임박":
        return "warning"
    if normalized == "마감":
        return "danger"
    if normalized == "예정":
        return "neutral"
    return "neutral"


def _dday_badge_tone(dday_text: str) -> str:
    normalized = (dday_text or "").strip()
    if not normalized:
        return "neutral"
    if normalized == "D-Day":
        return "danger"
    if normalized.startswith("D-"):
        try:
            days = int(normalized.split("-", 1)[1])
        except ValueError:
            return "neutral"
        if days <= 7:
            return "danger"
        if days <= 30:
            return "warning"
        return "primary"
    return "neutral"


def _source_label(source_key: str) -> str:
    return {
        "tipa": "MSS",
        "nipa": "NIPA",
    }.get((source_key or "").lower(), "IRIS")


def _as_dict(row) -> dict:
    if row is None:
        return {}
    if isinstance(row, dict):
        return row
    if hasattr(row, "to_dict"):
        return row.to_dict()
    return dict(row)


def _notice_outline_sections(api, story: dict[str, object], ai_summary: str) -> list[dict[str, str]]:
    background_text = (
        present_value(api, story.get("background_text"))
        or present_value(api, story.get("support_need_text"))
        or ai_summary
    )
    objective_text = (
        present_value(api, story.get("objective_text"))
        or present_value(api, story.get("summary_text"))
        or ai_summary
    )
    detail_text = (
        present_value(api, story.get("detail_text"))
        or present_value(api, story.get("support_plan_text"))
        or ai_summary
    )
    return [
        {"title": "사업 개요 및 배경", "body": background_text or "없음"},
        {"title": "공고 핵심 요약", "body": objective_text or "없음"},
        {"title": "지원 내용 및 검토 포인트", "body": detail_text or "없음"},
    ]


def render_page(st, row, opportunity_df: pd.DataFrame, *, api) -> None:
    inject_detail_workspace_styles(st)

    row_dict = _as_dict(row)
    if not row_dict:
        st.info("표시할 공고가 없습니다.")
        return

    source_key = api.resolve_notice_source_key(row_dict)
    source_label = _source_label(source_key)
    notice_id = first_present(api, row_dict, "공고ID", "notice_id")
    notice_title = first_present(api, row_dict, "공고명", "notice_title", "title")
    if not notice_title:
        st.info("공고 제목을 확인할 수 없습니다.")
        return

    related = api.find_related_opportunities_for_notice(row_dict, opportunity_df)
    top_related: dict[str, object] = {}
    if not related.empty:
        related = api.ensure_opportunity_row_ids(related.copy())
        related = related.sort_values(
            by=["rfp_score", "project_name"],
            ascending=[False, True],
            na_position="last",
        )
        top_related = related.iloc[0].to_dict()
        row_dict = api.ensure_notice_analysis_fallback(row_dict, top_related)

    period_text = first_present(api, row_dict, "접수기간", "신청기간", "period")
    status_text = api.normalize_notice_status_label(
        first_present(api, row_dict, "공고상태", "status", "rcve_status")
    )
    dday_text = present_value(api, api.format_dashboard_deadline_badge(period_text, status_text))
    score_text = first_present(api, row_dict, "대표점수", "추천점수", "rfp_score", "llm_fit_score")
    recommendation_text = first_present(
        api,
        row_dict,
        "대표추천도",
        "추천정도",
        "recommendation",
        "llm_recommendation",
    )
    budget_text = first_present(api, row_dict, "대표예산", "사업비", "예산")
    ministry_text = first_present(api, row_dict, "소관부처", "주관부처", "ministry")
    agency_text = first_present(api, row_dict, "전문기관", "담당부서", "agency")
    detail_link = api.resolve_external_detail_link(row_dict, source_key=source_key)

    story = api.build_analysis_story_bundle(
        top_related or {},
        notice_row=row_dict,
        period_text=period_text,
    )

    ai_summary = (
        first_present(api, row_dict, "대표추천이유", "summary", "analysis_summary")
        or present_value(api, story.get("summary_text"))
        or present_value(api, api.build_project_analysis_text(row_dict, top_related))
    )

    registered_text = first_present(api, row_dict, "공고일자", "registered_at", "Date")
    notice_no = first_present(api, row_dict, "공고번호", "ancm_no")

    basic_info_rows = [
        {"label": "공고 상태", "value": status_text or "없음", "kind": "badge", "badge_tone": "green"},
        {"label": "추천", "value": recommendation_text or "없음", "kind": "badge", "badge_tone": "violet"},
        {"label": "예산", "value": budget_text or "없음", "kind": "text"},
        {"label": "소관 부처", "value": ministry_text or "없음", "kind": "text"},
        {"label": "전문기관", "value": agency_text or "없음", "kind": "text"},
        {"label": "접수 기간", "value": period_text or "없음", "kind": "text"},
        {"label": "공고 등록일", "value": registered_text or "없음", "kind": "text"},
        {"label": "공고 번호", "value": notice_no or "없음", "kind": "text"},
    ]

    related_items: list[dict[str, object]] = []
    if not related.empty:
        for _, related_row in related.head(8).iterrows():
            related_dict = related_row.to_dict()
            row_id = first_present(api, related_dict, "_row_id", "document_id")
            project_title = first_present(
                api,
                related_dict,
                "llm_project_name",
                "project_name",
                "해당 과제명",
                "rfp_title",
                "file_name",
            )
            if not row_id or not project_title:
                continue
            related_items.append(
                {
                    "title": truncate(api, project_title, max_chars=92),
                    "href": api.build_route_href("rfp_queue", row_id, source_key=source_key),
                    "subtitle": truncate(
                        api,
                        first_present(api, related_dict, "notice_title", "공고명"),
                        max_chars=110,
                    ),
                    "meta": " / ".join(
                        part
                        for part in [
                            first_present(api, related_dict, "llm_recommendation", "recommendation"),
                            first_present(api, related_dict, "llm_fit_score", "rfp_score"),
                            first_present(api, related_dict, "budget", "llm_total_budget_text", "total_budget_text"),
                        ]
                        if part
                    ),
                    "badges": [
                        value
                        for value in [
                            first_present(api, related_dict, "llm_recommendation", "recommendation"),
                            first_present(api, related_dict, "llm_fit_score", "rfp_score"),
                        ]
                        if value
                    ],
                }
            )

    original_items: list[dict[str, object]] = []
    if detail_link:
        original_items.append(
            {
                "title": "원문 공고 바로가기",
                "href": detail_link,
                "subtitle": f"{source_label} 공고 원문",
                "meta": notice_no,
                "badges": [source_label],
            }
        )

    badges = [
        (status_text, _status_badge_tone(status_text)),
        (recommendation_text, "primary"),
        (dday_text, _dday_badge_tone(dday_text)),
        (f"점수 {score_text}" if score_text else "", "neutral"),
    ]

    def render_header_actions() -> None:
        if detail_link:
            st.link_button("원문 공고 보기", detail_link, use_container_width=False)
        if notice_id:
            api.render_favorite_scrap_button(
                notice_id=notice_id,
                current_value=first_present(api, row_dict, "검토여부", "검토 여부", "review_status"),
                source_key=source_key,
                notice_title=notice_title,
                button_key=f"notice_detail_header_favorite_{notice_id}",
                compact=True,
                use_container_width=False,
            )

    render_detail_header_card(
        st,
        title=notice_title,
        kicker=f"{source_label} Notice",
        subtitle=truncate(api, first_present(api, row_dict, "summary", "analysis_summary") or "", max_chars=100),
        badges=badges,
        action_renderer=render_header_actions,
        container_key=f"notice_detail_header_{notice_id or 'unknown'}",
    )

    content_col, comment_col = st.columns([1.65, 1.0], gap="large")

    with content_col:
        render_detail_fact_rows_card(
            st,
            title="주요 정보",
            rows=basic_info_rows,
            key=f"notice_detail_basic_info_{notice_id or 'unknown'}",
            tone="panel",
        )
        render_detail_outline_card(
            st,
            title="공고 개요",
            sections=_notice_outline_sections(api, story, ai_summary),
            key=f"notice_detail_overview_{notice_id or 'unknown'}",
            tone="panel",
        )
        render_detail_related_panel_card(
            st,
            title="관련 RFP",
            items=related_items,
            key=f"notice_detail_related_rfp_{notice_id or 'unknown'}",
            empty_text="연결된 RFP가 아직 없습니다.",
        )
        render_detail_related_panel_card(
            st,
            title="원문 / 첨부",
            items=original_items,
            key=f"notice_detail_original_{notice_id or 'unknown'}",
            empty_text="확인 가능한 원문 링크가 없습니다.",
        )

    with comment_col:
        with st.container(key="detail_side_notice"):
            api.render_notice_comments(
                row_dict,
                section_key=f"notice_{notice_id}",
                show_title=False,
                modern_layout=True,
            )
