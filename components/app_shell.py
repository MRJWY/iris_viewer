from html import escape
from urllib.parse import urlencode

import router
from components.notice_discovery import discovery_href, inject_styles


def render_app_shell(st, mode_config, current_source: str, current_page: str, *, api) -> None:
    inject_styles(st)
    links = []
    for item in router.PRIMARY_NAV_ITEMS:
        active = router.is_primary_nav_route(current_source, current_page, item)
        href = discovery_href({"source": item.source_key, "page": item.page_key, "view": "table"}, api=api)
        active_class = " is-active" if active else ""
        aria = ' aria-current="page"' if active else ""
        links.append(f'<a href="{escape(href, quote=True)}" class="discovery-nav-link{active_class}"{aria} target="_self">{escape(item.label)}</a>')
    account_links = []
    if mode_config.mode == "admin":
        for group in mode_config.nav_groups:
            if group.key == "admin_tools":
                for item in group.items:
                    href = "?" + urlencode({"source": item.source_key, "page": item.page_key, "view": "table"})
                    account_links.append(f'<a href="{escape(href, quote=True)}" target="_self">가입 요청 관리</a>')
    logout_params = api.get_query_params_dict()
    logout_params["logout"] = "1"
    logout_href = discovery_href(logout_params, api=api)
    home_href = escape(discovery_href({"source": "dashboard", "page": "dashboard", "view": "table"}, api=api), quote=True)
    user_name = escape(api.clean(api.get_current_app_user_key()) or "사용자")
    st.markdown(
        '<header class="discovery-header">'
        f'<a class="discovery-brand" href="{home_href}" target="_self">'
        '<span class="discovery-brand-mark">V</span> VUREON <span class="discovery-brand-sub">공고 찾기</span></a>'
        f'<nav aria-label="주 메뉴">{"".join(links)}</nav>'
        '<details class="discovery-account"><summary>계정</summary>'
        f'<div class="discovery-account-menu"><span>{user_name}</span>{"".join(account_links)}'
        f'<a href="{escape(logout_href, quote=True)}" target="_self">로그아웃</a></div></details>'
        '</header>', unsafe_allow_html=True,
    )
