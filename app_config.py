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
    normalized_mode = "viewer"

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
        SourceRouteConfig("tipa", "중소기업기술정보진흥원", "tipa_current", True, "external", page_configs=tipa_pages),
        SourceRouteConfig("nipa", "NIPA", "nipa_current", True, "external", page_configs=nipa_pages),
        SourceRouteConfig("proposal", "제안관리", "proposal", False, "proposal"),
        SourceRouteConfig("operations", "운영관리", "operations", True, "operations"),
        SourceRouteConfig("favorites", "관심 공고", "favorites", True, "favorites"),
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
                ("notice", "Notice Queue"),
                ("notice_scheduled", "예정 공고"),
                ("notice_archive", "Archive"),
            ),
            valid_iris_pages=frozenset({"opportunity", "notice", "notice_scheduled", "notice_archive"}),
            iris_tab_key="iris_page_tabs",
        )

    return AppModeConfig(
        mode="viewer",
        page_title="Crawler Hub Viewer",
        header_title="Crawler Hub Viewer",
        header_caption="?????? ??? ???, ???, ????? ?????????? ????????",
        supports_summary=False,
        nav_groups=viewer_nav_groups,
        sources=sources,
        default_source="iris",
        default_iris_page="opportunity",
        iris_tabs=(
            ("opportunity", "RFP Queue"),
            ("notice", "Notice Queue"),
            ("notice_scheduled", "??? ???"),
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
