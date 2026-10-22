"""Numbered, URL-addressable pagination shared by discovery lists."""
from html import escape


def clamp_page(value, total_pages: int) -> int:
    try:
        return max(1, min(int(value), max(1, total_pages)))
    except (TypeError, ValueError):
        return 1


def page_window(current: int, total: int, *, size: int = 10) -> list[int]:
    total = max(1, total)
    current = clamp_page(current, total)
    size = max(1, min(size, total))
    start = max(1, min(current - (size - 1) // 2, total - size + 1))
    return list(range(start, start + size))


def render_pagination(st, *, current: int, total: int, page_param: str, api) -> None:
    if total <= 1:
        return
    from components.notice_discovery import discovery_href

    current = clamp_page(current, total)
    params = dict(st.query_params)

    def link(page, label, *, css="", rel=""):
        href = discovery_href({**params, page_param: str(page)}, api=api)
        return (f'<a class="{css}" href="{escape(href, quote=True)}" target="_self" '
                f'aria-label="{page}페이지"' + (f' rel="{rel}"' if rel else "")
                + f'>{label}</a>')

    parts = [link(current - 1, "‹ 이전", css="discovery-page-direction", rel="prev")
             if current > 1 else '<span class="discovery-page-direction is-disabled" aria-disabled="true">‹ 이전</span>']
    for page in page_window(current, total):
        if page == current:
            parts.append(f'<span class="is-current" aria-current="page" aria-label="현재 {page}페이지">{page}</span>')
        else:
            parts.append(link(page, str(page), css="is-distant" if abs(page - current) > 2 else ""))
    parts.append(link(current + 1, "다음 ›", css="discovery-page-direction", rel="next")
                 if current < total else '<span class="discovery-page-direction is-disabled" aria-disabled="true">다음 ›</span>')
    st.markdown(
        '<nav class="discovery-pagination" aria-label="목록 페이지">' + "".join(parts) + '</nav>'
        f'<div class="discovery-page-count">{current} / {total:,} 페이지</div>',
        unsafe_allow_html=True,
    )
