"""Numbered pagination that updates a list fragment without browser navigation."""


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


def _change_page(st, page_param: str, page: int) -> None:
    # Updating query params preserves deep links, filters and auth without a reload.
    st.query_params[page_param] = str(page)


def render_pagination(st, *, current: int, total: int, page_param: str, key: str) -> None:
    if total <= 1:
        return
    current = clamp_page(current, total)

    def button(page, label, suffix, *, disabled=False, selected=False):
        st.button(
            label, key=f"discovery_pager_{suffix}_{key}",
            help=f"현재 {page}페이지" if selected else f"{page}페이지로 이동" if not disabled else None,
            disabled=disabled, type="primary" if selected else "tertiary",
            on_click=_change_page, args=(st, page_param, page),
        )

    with st.container(key=f"discovery_pagination_{key}", horizontal=True,
                      horizontal_alignment="center", gap=None):
        button(current - 1, "‹ 이전", "prev", disabled=current == 1)
        for page in page_window(current, total):
            suffix = f"distant_{page}" if abs(page - current) > 2 else f"page_{page}"
            button(page, str(page), suffix, disabled=page == current, selected=page == current)
        button(current + 1, "다음 ›", "next", disabled=current == total)
    st.markdown(
        f'<div class="discovery-page-count">{current} / {total:,} 페이지</div>',
        unsafe_allow_html=True,
    )
