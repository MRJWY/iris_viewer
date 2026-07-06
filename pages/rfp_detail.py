from __future__ import annotations

"""Legacy RFP detail page shim.

The active public viewer runtime now delegates to the repository-root `app.py`.
Prefer updating the root app first when changing RFP detail behavior or UI.
"""

import re

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


def _deadline_tone(deadline_text: str) -> str:
    normalized = (deadline_text or "").strip()
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


def _keyword_tokens(keyword_text: str) -> list[str]:
    text = (keyword_text or "").strip()
    if not text or text == "-":
        return []

    parts = re.split(r"[,/\n|·]+", text)
    tokens: list[str] = []
    seen: set[str] = set()
    for part in parts:
        token = part.strip()
        if not token or token in seen:
            continue
        seen.add(token)
        tokens.append(token)
        if len(tokens) >= 8:
            break
    return tokens or [text]


def _normalize_date_text(value: str) -> str:
    text = (value or "").strip()
    if not text or text == "-":
        return ""

    match = re.search(r"(\d{4})[./-](\d{1,2})[./-](\d{1,2})", text)
    if not match:
        return text

    year, month, day = match.groups()
    return f"{year}-{int(month):02d}-{int(day):02d}"


def _extract_period_bounds(period_text: str) -> tuple[str, str]:
    text = (period_text or "").strip()
    if not text or text == "-":
        return "", ""

    matches = re.findall(r"\d{4}[./-]\d{1,2}[./-]\d{1,2}", text)
    if len(matches) >= 2:
        return _normalize_date_text(matches[0]), _normalize_date_text(matches[-1])
    if len(matches) == 1:
        normalized = _normalize_date_text(matches[0])
        return normalized, normalized
    return "", ""


def _summary_sections(api, story: dict[str, object], ai_summary: str) -> list[dict[str, str]]:
    background_text = present_value(api, story.get("background_text"))
    objective_text = present_value(api, story.get("objective_text"))
    detail_text = present_value(api, story.get("detail_text"))
    support_need_text = present_value(api, story.get("support_need_text"))
    support_plan_text = present_value(api, story.get("support_plan_text"))
    special_notes_text = present_value(api, story.get("special_notes_text"))

    section_one = "\n\n".join(
        part for part in [background_text, support_need_text, ai_summary] if part and part != "-"
    ).strip()
    section_two = objective_text or ai_summary or "없음"
    section_three = "\n\n".join(
        part for part in [detail_text, support_plan_text] if part and part != "-"
    ).strip() or "없음"
    section_four = special_notes_text or "없음"

    return [
        {"title": "사업 개요 및 배경", "body": section_one or "없음"},
        {"title": "과제 목표", "body": section_two},
        {"title": "과제 내용", "body": section_three},
        {"title": "특기사항", "body": section_four},
    ]


def render_page(st, row, *, api) -> None:
    inject_detail_workspace_styles(st)

    row_dict = _as_dict(row)
    if not row_dict:
        st.info("표시할 RFP가 없습니다.")
        return

    source_key = first_present(api, row_dict, "source_key") or api.resolve_notice_source_key(row_dict)
    source_label = _source_label(source_key)
    notice_id = first_present(api, row_dict, "notice_id", "공고ID")
    row_id = first_present(api, row_dict, "_row_id", "document_id")
    title = first_present(
        api,
        row_dict,
        "llm_project_name",
        "project_name",
        "해당 과제명",
        "llm_rfp_title",
        "rfp_title",
        "file_name",
    )
    if not title:
        st.info("RFP 제목을 확인할 수 없습니다.")
        return

    detail_link = api.resolve_external_detail_link(row_dict, source_key=source_key)
    period_text = first_present(api, row_dict, "notice_period", "period", "접수기간", "신청기간")
    status_text = api.normalize_notice_status_label(
        first_present(api, row_dict, "notice_status", "공고상태", "status", "rcve_status")
    )
    deadline_text = present_value(api, api.format_dashboard_deadline_badge(period_text, status_text))
    recommendation_text = first_present(api, row_dict, "llm_recommendation", "recommendation", "추천여부")
    score_text = first_present(api, row_dict, "llm_fit_score", "rfp_score")
    support_type = first_present(api, row_dict, "support_type", "사업유형", "business_type", "document_type")
    notice_title = first_present(api, row_dict, "notice_title", "공고명")
    keyword_text = first_present(api, row_dict, "llm_keywords", "keywords", "핵심키워드")
    if not keyword_text:
        keyword_text = first_present(api, row_dict, "target_market")

    story = api.build_analysis_story_bundle(row_dict, period_text=period_text)
    ai_summary = present_value(api, story.get("summary_text")) or present_value(
        api,
        api.build_project_analysis_text(row_dict),
    )

    total_budget_text = present_value(api, story.get("total_budget_text")) or first_present(
        api,
        row_dict,
        "llm_total_budget_text",
        "total_budget_text",
        "budget",
        "예산",
    )
    per_project_budget_text = present_value(api, story.get("per_project_budget_text"))
    eligibility_text = present_value(api, story.get("eligibility_text")) or first_present(
        api,
        row_dict,
        "eligibility",
        "지원대상",
    )
    registered_text = first_present(
        api,
        row_dict,
        "registered_at",
        "공고일자",
        "notice_date",
        "Date",
    )
    registered_date = _normalize_date_text(registered_text) or "없음"
    period_start, period_end = _extract_period_bounds(period_text)
    end_date = period_end or "없음"

    basic_info_rows = [
        {"label": "지원 유형", "value": support_type or "없음", "kind": "badge", "badge_tone": "amber"},
        {"label": "핵심 키워드", "value": _keyword_tokens(keyword_text) or ["없음"], "kind": "badges"},
        {"label": "사업 규모", "value": total_budget_text or "없음", "kind": "badge", "badge_tone": "violet"},
        {"label": "지원금", "value": per_project_budget_text or "없음", "kind": "badge", "badge_tone": "green"},
        {"label": "지원 가능 기관", "value": eligibility_text or "없음", "kind": "badge", "badge_tone": "neutral"},
        {"label": "공고 등록일", "value": registered_date, "kind": "text"},
        {"label": "공고 마감일", "value": end_date, "kind": "text"},
        {
            "label": "신청 기간",
            "value": period_text or "없음",
            "kind": "text",
            "prefix_badge": deadline_text if deadline_text and deadline_text != "-" else "",
            "prefix_badge_tone": _deadline_tone(deadline_text),
        },
    ]

    related_notice_items: list[dict[str, object]] = []
    if notice_id and notice_title:
        related_notice_items.append(
            {
                "title": truncate(api, notice_title, max_chars=96),
                "href": api.build_route_href("notice_queue", notice_id, source_key="notices"),
                "subtitle": "연결 공고",
                "meta": " / ".join(
                    part
                    for part in [
                        first_present(api, row_dict, "ministry", "소관부처", "주관부처"),
                        first_present(api, row_dict, "agency", "전문기관", "담당부서"),
                        period_start or period_text,
                    ]
                    if part
                ),
                "badges": [value for value in [status_text, deadline_text] if value],
            }
        )

    badges = [
        (recommendation_text, "primary"),
        (f"점수 {score_text}" if score_text else "", "neutral"),
        (deadline_text, _deadline_tone(deadline_text)),
        (status_text, "neutral"),
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
                button_key=f"rfp_detail_header_favorite_{row_id or notice_id}",
                compact=True,
                use_container_width=False,
            )

    render_detail_header_card(
        st,
        title=title,
        kicker=f"{source_label} RFP",
        subtitle=truncate(api, notice_title, max_chars=100),
        badges=badges,
        action_renderer=render_header_actions,
        container_key=f"rfp_detail_header_{row_id or notice_id or 'unknown'}",
    )

    content_col, comment_col = st.columns([1.65, 1.0], gap="large")

    with content_col:
        render_detail_fact_rows_card(
            st,
            title="주요 정보",
            rows=basic_info_rows,
            key=f"rfp_detail_basic_info_{row_id or notice_id or 'unknown'}",
            tone="panel",
        )
        render_detail_outline_card(
            st,
            title="과제 개요",
            sections=_summary_sections(api, story, ai_summary),
            key=f"rfp_detail_overview_{row_id or notice_id or 'unknown'}",
            tone="panel",
        )
        render_detail_related_panel_card(
            st,
            title="관련 공고",
            items=related_notice_items,
            key=f"rfp_detail_notice_{row_id or notice_id or 'unknown'}",
            empty_text="연결된 공고 정보가 없습니다.",
        )

    with comment_col:
        with st.container(key="detail_side_rfp"):
            api.render_notice_comments(
                row_dict,
                section_key=f"rfp_{notice_id or row_id}",
                show_title=False,
                modern_layout=True,
            )
