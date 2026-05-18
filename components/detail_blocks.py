from __future__ import annotations

from html import escape
from pathlib import Path
from typing import Callable, Iterable, Sequence


DETAIL_STYLE_START = "/* detail-workspace:start */"
DETAIL_STYLE_END = "/* detail-workspace:end */"
STYLES_PATH = Path(__file__).resolve().parent.parent / "assets" / "styles.css"


def inject_detail_workspace_styles(st) -> None:
    session_key = "_detail_workspace_styles_injected"
    if st.session_state.get(session_key):
        return

    try:
        css_text = STYLES_PATH.read_text(encoding="utf-8")
    except OSError:
        return

    start = css_text.find(DETAIL_STYLE_START)
    end = css_text.find(DETAIL_STYLE_END)
    if start == -1 or end == -1 or end <= start:
        return

    scoped_css = css_text[start + len(DETAIL_STYLE_START):end].strip()
    if not scoped_css:
        return

    st.markdown(f"<style>{scoped_css}</style>", unsafe_allow_html=True)
    st.session_state[session_key] = True


def present_value(api, value: object) -> str:
    if value is None:
        return ""
    if isinstance(value, dict):
        return ""
    if isinstance(value, (list, tuple, set)):
        parts = [present_value(api, item) for item in value]
        return ", ".join(part for part in parts if part)

    text = api.clean(value) if hasattr(api, "clean") else str(value).strip()
    if text in {"", "-", "None", "none", "NaN", "nan", "NaT"}:
        return ""
    return text


def first_present(api, row: dict | None, *keys: str) -> str:
    if not row:
        return ""
    if hasattr(api, "first_non_empty"):
        return present_value(api, api.first_non_empty(row, *keys))
    for key in keys:
        value = present_value(api, row.get(key))
        if value:
            return value
    return ""


def filter_meta_items(api, items: Sequence[tuple[str, object]]) -> list[tuple[str, str]]:
    normalized: list[tuple[str, str]] = []
    for label, value in items:
        display = present_value(api, value)
        if display:
            normalized.append((label, display))
    return normalized


def filter_points(api, items: Iterable[object]) -> list[str]:
    points: list[str] = []
    for item in items:
        display = present_value(api, item)
        if display and display not in points:
            points.append(display)
    return points


def truncate(api, value: object, *, max_chars: int) -> str:
    text = present_value(api, value)
    if not text:
        return ""
    if hasattr(api, "truncate_text"):
        return api.truncate_text(text, max_chars=max_chars)
    return text if len(text) <= max_chars else text[: max_chars - 1] + "…"


def render_detail_breadcrumb(st, items: Sequence[tuple[str, str | None]]) -> None:
    crumbs: list[str] = []
    total = len(items)
    for index, (label, href) in enumerate(items):
        safe_label = escape(label)
        if href and index < total - 1:
            crumbs.append(
                f'<a class="detail-breadcrumb-link" href="{escape(href, quote=True)}" target="_self">{safe_label}</a>'
            )
        else:
            crumbs.append(f'<span class="detail-breadcrumb-current">{safe_label}</span>')

    separator = '<span class="detail-breadcrumb-sep">/</span>'
    st.markdown(
        f'<div class="detail-breadcrumb">{separator.join(crumbs)}</div>',
        unsafe_allow_html=True,
    )


def render_detail_badge_row(st, badges: Sequence[dict[str, str] | tuple[str, str]]) -> None:
    badge_html: list[str] = []
    for badge in badges:
        if isinstance(badge, tuple):
            label, tone = badge
        else:
            label = badge.get("label", "")
            tone = badge.get("tone", "neutral")
        if not label:
            continue
        badge_html.append(
            f'<span class="detail-badge is-{escape(tone)}">{escape(label)}</span>'
        )

    if badge_html:
        st.markdown(
            f'<div class="detail-badge-row">{"".join(badge_html)}</div>',
            unsafe_allow_html=True,
        )


def render_detail_header_card(
    st,
    *,
    title: str,
    badges: Sequence[dict[str, str] | tuple[str, str]],
    action_renderer: Callable[[], None] | None = None,
    kicker: str = "",
    subtitle: str = "",
    container_key: str = "detail_header_card",
) -> None:
    with st.container(border=True, key=container_key):
        header_cols = st.columns([4.4, 1.6], gap="large")
        with header_cols[0]:
            st.markdown('<div class="detail-header-card">', unsafe_allow_html=True)
            if kicker:
                st.markdown(f'<div class="detail-kicker">{escape(kicker)}</div>', unsafe_allow_html=True)
            st.markdown(f'<div class="detail-title">{escape(title)}</div>', unsafe_allow_html=True)
            if subtitle:
                st.markdown(
                    f'<div class="detail-header-subtitle">{escape(subtitle)}</div>',
                    unsafe_allow_html=True,
                )
            render_detail_badge_row(st, badges)
            st.markdown("</div>", unsafe_allow_html=True)
        with header_cols[1]:
            if action_renderer is not None:
                st.markdown('<div class="detail-header-actions">', unsafe_allow_html=True)
                action_renderer()
                st.markdown("</div>", unsafe_allow_html=True)


def render_detail_kpi_strip(st, items: Sequence[tuple[str, object]]) -> None:
    cards: list[str] = []
    for label, value in items:
        text = str(value).strip() if value is not None else ""
        if not text or text == "-":
            continue
        cards.append(
            (
                '<div class="detail-kpi-item">'
                f'<div class="detail-kpi-label">{escape(label)}</div>'
                f'<div class="detail-kpi-value">{escape(text)}</div>'
                "</div>"
            )
        )

    if cards:
        st.markdown(
            f'<div class="detail-kpi-strip">{"".join(cards)}</div>',
            unsafe_allow_html=True,
        )


def render_detail_meta_grid(st, items: Sequence[tuple[str, str]]) -> None:
    if not items:
        return
    html = "".join(
        (
            '<div class="detail-meta-item">'
            f'<div class="detail-meta-label">{escape(label)}</div>'
            f'<div class="detail-meta-value">{escape(value).replace(chr(10), "<br>")}</div>'
            "</div>"
        )
        for label, value in items
    )
    st.markdown(f'<div class="detail-meta-grid">{html}</div>', unsafe_allow_html=True)


def _render_section_shell(
    st,
    *,
    title: str,
    key: str,
    body_renderer: Callable[[], None],
    caption: str = "",
) -> None:
    with st.container(border=True, key=key):
        st.markdown('<div class="detail-section">', unsafe_allow_html=True)
        st.markdown(f'<div class="detail-section-title">{escape(title)}</div>', unsafe_allow_html=True)
        if caption:
            st.markdown(
                f'<div class="detail-section-caption">{escape(caption)}</div>',
                unsafe_allow_html=True,
            )
        body_renderer()
        st.markdown("</div>", unsafe_allow_html=True)


def render_detail_summary_card(st, *, title: str, body: str, key: str, caption: str = "") -> None:
    if not body:
        return

    def _body() -> None:
        st.markdown(
            f'<div class="detail-summary-card">{escape(body).replace(chr(10), "<br>")}</div>',
            unsafe_allow_html=True,
        )

    _render_section_shell(st, title=title, key=key, body_renderer=_body, caption=caption)


def render_detail_decision_card(
    st,
    *,
    title: str,
    points: Sequence[str],
    key: str,
    caption: str = "",
) -> None:
    if not points:
        return

    def _body() -> None:
        html = "".join(f"<li>{escape(point)}</li>" for point in points if point)
        st.markdown(f'<ul class="detail-decision-list">{html}</ul>', unsafe_allow_html=True)

    _render_section_shell(st, title=title, key=key, body_renderer=_body, caption=caption)


def render_detail_support_card(
    st,
    *,
    title: str,
    items: Sequence[tuple[str, str]],
    key: str,
    caption: str = "",
) -> None:
    if not items:
        return

    def _body() -> None:
        render_detail_meta_grid(st, items)

    _render_section_shell(st, title=title, key=key, body_renderer=_body, caption=caption)


def render_detail_schedule_card(
    st,
    *,
    title: str,
    items: Sequence[tuple[str, str]],
    key: str,
    caption: str = "",
) -> None:
    if not items:
        return

    def _body() -> None:
        render_detail_meta_grid(st, items)

    _render_section_shell(st, title=title, key=key, body_renderer=_body, caption=caption)


def render_detail_related_items_card(
    st,
    *,
    title: str,
    items: Sequence[dict[str, object]],
    key: str,
    empty_text: str = "",
    caption: str = "",
) -> None:
    def _body() -> None:
        if not items:
            st.markdown(
                f'<div class="detail-empty">{escape(empty_text or "표시할 항목이 없습니다.")}</div>',
                unsafe_allow_html=True,
            )
            return

        blocks: list[str] = []
        for item in items:
            item_title = escape(str(item.get("title") or ""))
            href = str(item.get("href") or "")
            subtitle = str(item.get("subtitle") or "")
            meta = str(item.get("meta") or "")
            badges = item.get("badges") or []
            badge_html = "".join(
                f'<span class="detail-related-badge">{escape(str(badge))}</span>'
                for badge in badges
                if str(badge).strip()
            )
            title_html = (
                f'<a class="detail-related-title" href="{escape(href, quote=True)}" target="_self">{item_title}</a>'
                if href
                else f'<div class="detail-related-title">{item_title}</div>'
            )
            blocks.append(
                (
                    '<div class="detail-related-item">'
                    f"{title_html}"
                    + (f'<div class="detail-related-subtitle">{escape(subtitle)}</div>' if subtitle else "")
                    + (f'<div class="detail-related-meta">{escape(meta)}</div>' if meta else "")
                    + (f'<div class="detail-related-badges">{badge_html}</div>' if badge_html else "")
                    + "</div>"
                )
            )
        st.markdown(f'<div class="detail-related-card">{"".join(blocks)}</div>', unsafe_allow_html=True)

    _render_section_shell(st, title=title, key=key, body_renderer=_body, caption=caption)


def render_detail_action_panel(
    st,
    *,
    key: str,
    render_actions: Callable[[], None],
    title: str = "빠른 작업",
) -> None:
    with st.container(border=True, key=key):
        st.markdown('<div class="detail-action-panel">', unsafe_allow_html=True)
        st.markdown(f'<div class="detail-section-title">{escape(title)}</div>', unsafe_allow_html=True)
        render_actions()
        st.markdown("</div>", unsafe_allow_html=True)


def render_detail_review_card(
    st,
    *,
    key: str,
    review_caption: str = "",
    render_review: Callable[[], None] | None = None,
    render_comments: Callable[[], None] | None = None,
) -> None:
    with st.container(border=True, key=key):
        st.markdown('<div class="detail-review-card">', unsafe_allow_html=True)
        st.markdown('<div class="detail-section-title">검토 / 메모</div>', unsafe_allow_html=True)
        if review_caption:
            st.markdown(
                f'<div class="detail-section-caption">{escape(review_caption)}</div>',
                unsafe_allow_html=True,
            )
        if render_review is not None:
            render_review()
        if render_comments is not None:
            st.markdown('<div class="detail-divider"></div>', unsafe_allow_html=True)
            render_comments()
        st.markdown("</div>", unsafe_allow_html=True)


def render_detail_compact_meta_card(
    st,
    *,
    title: str,
    items: Sequence[tuple[str, str]],
    key: str,
) -> None:
    if not items:
        return
    with st.container(border=True, key=key):
        st.markdown('<div class="detail-compact-meta-card">', unsafe_allow_html=True)
        st.markdown(f'<div class="detail-section-title">{escape(title)}</div>', unsafe_allow_html=True)
        render_detail_meta_grid(st, items)
        st.markdown("</div>", unsafe_allow_html=True)
