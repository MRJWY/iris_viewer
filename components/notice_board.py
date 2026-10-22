"""Read-only expandable rows matching the award_list project board."""
from html import escape
from urllib.parse import parse_qsl


def render_notice_board(st, rows, *, api, rfp=False):
    # Local import keeps the shared search controls independent of the board.
    from components.notice_discovery import discovery_href, notice_href

    cards = []
    for _, row in rows.iterrows():
        def value(column, fallback=""):
            return api.clean(row.get(column)) or fallback

        title = value("Project" if rfp else "Title", "과제명 미확인" if rfp else "공고명 미확인")
        notice_title = value("Notice Title" if rfp else "Title", "공고명 미확인")
        identifier = value("Row ID")
        href = value("Detail Href") or (api.build_route_href("rfp_queue", identifier, source_key=api.resolve_route_source_key_for_row(row))
                if rfp and identifier else notice_href(row, api=api))
        if value("Detail Href").startswith("?"):
            href = discovery_href(api.apply_return_route(dict(parse_qsl(href[1:]))), api=api)
        status = value("Status", "상태 미확인")
        tone = "open" if status == "접수중" else "closed" if status == "마감" else "neutral"
        subtitle = notice_title if rfp else " · ".join(filter(None, (value("Source"), "등록 " + value("Date") if value("Date") else "")))
        fields = [("공고 출처", value("Source", "미확인")), ("주관기관", value("Agency", "미확인")),
                  ("등록일", value("Date", "미확인")), ("접수기간", value("Period", "미확인"))]
        if value("Keywords"):
            fields.append(("키워드", value("Keywords")))
        field_html = "".join(f'<div><dt>{escape(label)}</dt><dd>{escape(text)}</dd></div>' for label, text in fields)
        action = (f'<a href="{escape(href, quote=True)}" target="_self">{"RFP" if rfp else "공고"} 상세 보기 →</a>'
                  if href else '<span>상세 연결 정보 미확인</span>')
        cards.append(
            '<details class="discovery-board-row"><summary class="discovery-board-grid">'
            f'<div class="discovery-board-title"><strong>{escape(title)}</strong><small>{escape(subtitle)}</small></div>'
            f'<div class="discovery-board-agency">{escape(value("Agency", "기관 미확인"))}</div>'
            f'<div class="discovery-board-period">{escape(value("Period", "접수기간 미확인"))}</div>'
            f'<div class="discovery-board-status"><span class="discovery-status {tone}">{escape(status)}</span>'
            f'<small>{escape(value("D-Day"))}</small></div>'
            '<span class="discovery-board-chevron" aria-hidden="true">⌄</span></summary>'
            '<div class="discovery-board-expanded">'
            f'<h3>{escape(title)}</h3>'
            + (f'<p class="discovery-board-parent">{escape(notice_title)}</p>' if rfp else "")
            + f'<dl class="discovery-board-fields">{field_html}</dl>'
            f'<div class="discovery-board-action">{action}</div></div></details>'
        )
    header = ('<div class="discovery-board-grid discovery-board-header" aria-hidden="true">'
              f'<span>{"과제명 / 공고명" if rfp else "공고명 / 출처"}</span>'
              '<span>주관기관</span><span>접수기간</span><span>상태</span><span></span></div>')
    st.markdown('<div class="discovery-board">' + header + "".join(cards) + '</div>', unsafe_allow_html=True)
