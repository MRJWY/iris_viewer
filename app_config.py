from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class NavItemConfig:
    key: str
    label: str
    source_key: str
    page_key: str


@dataclass(frozen=True)
class NavGroupConfig:
    key: str
    label: str
    items: tuple[NavItemConfig, ...]


@dataclass(frozen=True)
class SourcePageConfig:
    key: str
    label: str
    kind: str
    title: str
    data_key: str = ""
    origin_key: str = ""
    secondary_data_key: str = ""
    secondary_origin_key: str = ""
    view_columns: tuple[str, ...] = ()


@dataclass(frozen=True)
class SourceRouteConfig:
    key: str
    label: str
    default_page: str
    requires_source_datasets: bool
    renderer_key: str
    page_configs: tuple[SourcePageConfig, ...] = ()


@dataclass(frozen=True)
class AppModeConfig:
    mode: str
    page_title: str
    header_title: str
    header_caption: str
    supports_summary: bool
    nav_groups: tuple[NavGroupConfig, ...]
    sources: tuple[SourceRouteConfig, ...]
    default_source: str
    default_iris_page: str
    iris_tabs: tuple[tuple[str, str], ...]
    valid_iris_pages: frozenset[str]
    iris_tab_key: str


def build_app_mode_config(app_mode: str, *, nipa_view_columns: tuple[str, ...] = ()) -> AppModeConfig:
    normalized_mode = "viewer" if str(app_mode or "").strip().lower() == "viewer" else "admin"

    tipa_pages = (
        SourcePageConfig("tipa_current", "진행 공고", "notice", "진행 공고", data_key="mss_current", origin_key="mss_current_origin"),
        SourcePageConfig("tipa_scheduled", "예정 공고", "notice", "예정 공고", data_key="mss_current", origin_key="mss_current_origin"),
        SourcePageConfig("tipa_opportunity", "Opportunity", "opportunity", "Opportunity", data_key="mss_opportunity"),
        SourcePageConfig(
            "tipa_archive",
            "Archive",
            "archive",
            "Archive",
            data_key="mss_current",
            origin_key="mss_current_origin",
            secondary_data_key="mss_past",
            secondary_origin_key="mss_past_origin",
        ),
    )
    nipa_pages = (
        SourcePageConfig(
            "nipa_current",
            "진행 공고",
            "notice",
            "진행 공고",
            data_key="nipa_current",
            origin_key="nipa_current_origin",
            view_columns=tuple(nipa_view_columns),
        ),
        SourcePageConfig(
            "nipa_scheduled",
            "예정 공고",
            "notice",
            "예정 공고",
            data_key="nipa_current",
            origin_key="nipa_current_origin",
            view_columns=tuple(nipa_view_columns),
        ),
        SourcePageConfig("nipa_opportunity", "Opportunity", "opportunity", "Opportunity", data_key="nipa_opportunity"),
        SourcePageConfig(
            "nipa_archive",
            "Archive",
            "archive",
            "Archive",
            data_key="nipa_current",
            origin_key="nipa_current_origin",
            secondary_data_key="nipa_past",
            secondary_origin_key="nipa_past_origin",
            view_columns=tuple(nipa_view_columns),
        ),
    )

    sources = (
        SourceRouteConfig("dashboard", "Dashboard", "dashboard", True, "dashboard"),
        SourceRouteConfig("iris", "IRIS", "notice", False, "iris"),
        SourceRouteConfig("admin_requests", "Signup Requests", "requests", False, "admin_requests"),
        SourceRouteConfig("tipa", "중소기업기술정보진흥원", "tipa_current", True, "external", page_configs=tipa_pages),
        SourceRouteConfig("nipa", "NIPA", "nipa_current", True, "external", page_configs=nipa_pages),
        SourceRouteConfig("proposal", "제안관리", "proposal", False, "proposal"),
        SourceRouteConfig("operations", "운영관리", "operations", True, "operations"),
        SourceRouteConfig("favorites", "관심 공고", "favorites", True, "favorites"),
    )

    common_nav_groups = (
        NavGroupConfig(
            "overview",
            "Dashboard",
            (
                NavItemConfig("dashboard_home", "대시보드", "dashboard", "dashboard"),
            ),
        ),
        NavGroupConfig(
            "notice",
            "공고탐색",
            (
                NavItemConfig("iris_notice", "IRIS 진행", "iris", "notice"),
                NavItemConfig("iris_notice_scheduled", "IRIS 예정", "iris", "notice_scheduled"),
                NavItemConfig("tipa_current", "중기부 진행", "tipa", "tipa_current"),
                NavItemConfig("tipa_scheduled", "중기부 예정", "tipa", "tipa_scheduled"),
                NavItemConfig("nipa_current", "NIPA 진행", "nipa", "nipa_current"),
                NavItemConfig("nipa_scheduled", "NIPA 예정", "nipa", "nipa_scheduled"),
            ),
        ),
        NavGroupConfig(
            "opportunity",
            "추천기회",
            (
                NavItemConfig("iris_opportunity", "IRIS Opportunity", "iris", "opportunity"),
                NavItemConfig("tipa_opportunity", "중기부 Opportunity", "tipa", "tipa_opportunity"),
                NavItemConfig("nipa_opportunity", "NIPA Opportunity", "nipa", "nipa_opportunity"),
            ),
        ),
        NavGroupConfig(
            "proposal",
            "제안관리",
            (
                NavItemConfig("proposal_home", "제안 현황", "proposal", "proposal"),
            ),
        ),
        NavGroupConfig(
            "operations",
            "운영관리",
            (
                NavItemConfig("operations_home", "운영 현황", "operations", "operations"),
                NavItemConfig("favorites", "관심 공고", "favorites", "favorites"),
            ),
        ),
        NavGroupConfig(
            "archive",
            "Archive",
            (
                NavItemConfig("iris_archive", "IRIS Archive", "iris", "notice_archive"),
                NavItemConfig("tipa_archive", "중기부 Archive", "tipa", "tipa_archive"),
                NavItemConfig("nipa_archive", "NIPA Archive", "nipa", "nipa_archive"),
            ),
        ),
    )

    viewer_nav_groups = (
        NavGroupConfig(
            "workspace",
            "Workspace",
            (
                NavItemConfig("rfp_queue", "RFP Queue", "iris", "opportunity"),
                NavItemConfig("notice_queue", "Notice Queue", "iris", "notice"),
            ),
        ),
        NavGroupConfig(
            "archive",
            "Archive",
            (
                NavItemConfig("iris_archive", "IRIS Archive", "iris", "notice_archive"),
                NavItemConfig("tipa_archive", "중기부 Archive", "tipa", "tipa_archive"),
                NavItemConfig("nipa_archive", "NIPA Archive", "nipa", "nipa_archive"),
            ),
        ),
    )
    admin_nav_groups = common_nav_groups + (
        NavGroupConfig(
            "admin_tools",
            "Admin",
            (
                NavItemConfig("signup_requests", "Signup Requests", "admin_requests", "requests"),
            ),
        ),
    )

    if normalized_mode == "viewer":
        return AppModeConfig(
            mode="viewer",
            page_title="Crawler Hub Viewer",
            header_title="Crawler Hub Viewer",
            header_caption="정부사업 공고 수집, 추천, 검토를 한 곳에서 보는 뷰어입니다.",
            supports_summary=False,
            nav_groups=viewer_nav_groups,
            sources=sources,
            default_source="iris",
            default_iris_page="opportunity",
            iris_tabs=(
                ("opportunity", "RFP Queue"),
                ("notice", "진행 공고"),
                ("notice_scheduled", "예정 공고"),
                ("notice_archive", "Archive"),
            ),
            valid_iris_pages=frozenset({"opportunity", "notice", "notice_scheduled", "notice_archive"}),
            iris_tab_key="iris_page_tabs",
        )

    return AppModeConfig(
        mode="admin",
        page_title="Crawler Hub Admin",
        header_title="Crawler Hub Admin",
        header_caption="정부사업 공고 수집, 추천, 검토, 제안관리를 한 곳에서 운영합니다.",
        supports_summary=False,
        nav_groups=admin_nav_groups,
        sources=sources,
        default_source="dashboard",
        default_iris_page="notice",
        iris_tabs=(
            ("notice", "진행 공고"),
            ("notice_scheduled", "예정 공고"),
            ("opportunity", "Opportunity"),
            ("notice_archive", "Archive"),
        ),
        valid_iris_pages=frozenset({"opportunity", "notice", "notice_scheduled", "notice_archive"}),
        iris_tab_key="iris_page_tabs",
    )


def get_source_label_map(mode_config: AppModeConfig) -> dict[str, str]:
    return {source.key: source.label for source in mode_config.sources}


def get_source_key_map(mode_config: AppModeConfig) -> dict[str, str]:
    return {source.label: source.key for source in mode_config.sources}


def get_source_config_map(mode_config: AppModeConfig) -> dict[str, SourceRouteConfig]:
    return {source.key: source for source in mode_config.sources}


def get_default_page_for_source(mode_config: AppModeConfig, source_key: str) -> str:
    source_config = get_source_config_map(mode_config).get(source_key)
    if source_config is not None:
        return source_config.default_page
    return mode_config.default_iris_page


def find_nav_group_for_route(mode_config: AppModeConfig, source_key: str, page_key: str) -> NavGroupConfig:
    for group in mode_config.nav_groups:
        for item in group.items:
            if item.source_key == source_key and item.page_key == page_key:
                return group
    return mode_config.nav_groups[0]
