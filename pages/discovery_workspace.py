"""Supabase discovery adapter for the existing authenticated public viewer."""
from html import escape
from urllib.parse import parse_qsl, urlencode

import streamlit as st
import shared_app as core

from components.app_shell import render_app_shell
from components.notice_discovery import discovery_href
from pages.supabase_discovery import render_page
from jobs_common import discovery_rules


class ViewerDiscoveryApi:
    def __getattr__(self, name):
        if name in {"filter_current_opportunity_rows", "build_candidate_mask", "build_notice_archive_mask"}:
            return getattr(discovery_rules, name)
        return getattr(core, name)

    def build_table_return_href(self, page_key, *, source_key=None):
        route = core.decode_return_route(core.get_query_param("return_to"))
        return discovery_href(route or {"source": source_key or "notices", "page": page_key, "view": "table"}, api=self)

    def _render_same_tab_link_button(self, label, href, **kwargs):
        st.markdown(f'<a class="discovery-footer-link" href="{escape(href, quote=True)}" target="_self">{escape(label)}</a>', unsafe_allow_html=True)

    def render_notice_detail_from_row(self, row, related):
        from pages.notice_detail import render_page as render_detail
        related = related.copy()
        if "Detail Href" in related:
            related["Detail Href"] = related["Detail Href"].map(
                lambda href: discovery_href(core.apply_return_route(dict(parse_qsl(href[1:]))), api=self)
                if isinstance(href, str) and href.startswith("?") else href
            )
        render_detail(st, row, related, api=self)

    def render_notice_comments(self, row, *, section_key, **kwargs):
        # Workflow storage stays unchanged, but only loads when requested.
        key = f"discovery_comments_{section_key}"
        if not st.session_state.get(key):
            if st.button("댓글 불러오기", key=f"{key}_load"):
                st.session_state[key] = True
                st.rerun()
            return
        core.render_notice_comments(row, section_key=section_key, **kwargs)


def render_workspace():
    mode = core.build_app_mode_config("viewer", nipa_view_columns=tuple(core.NIPA_VIEW_COLUMNS))
    st.set_page_config(page_title="VUREON | 공고 찾기", layout="wide", initial_sidebar_state="collapsed")
    core.inject_page_styles()
    # Preserve the deployed viewer's account validation and workspace permissions.
    core.require_login(mode)
    core.consume_workspace_logout_query_action()
    core.consume_favorite_toggle_query_action()
    core.render_favorite_toggle_monitor()
    core.render_notice_comment_action_monitor()
    source = core.get_query_param("source") or "dashboard"
    page = core.get_query_param("page") or "dashboard"
    if source not in {"dashboard", "notices"}:
        source, page = ("notices", "notice_queue") if page == "notice_queue" else ("dashboard", "dashboard")
        params = core.get_query_params_dict()
        if core.get_query_param("page") == "rfp_queue":
            params["dashboard_section"] = "recommended"
        params.update(source=source, page=page, view="table")
        core.replace_query_params(core.with_auth_params(params))
    api = ViewerDiscoveryApi()
    render_app_shell(st, mode, source, page, api=api)
    print("[discovery] viewer data backend: supabase", flush=True)
    render_page(st, source, api=api)
