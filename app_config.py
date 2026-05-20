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


def build_workspace_nav_items(sources: tuple[SourceRouteConfig, ...]) -> tuple[NavItemConfig, ...]:
    items: list[NavItemConfig] = []
    for source in sources:
        if source.key in {"dashboard", "notices", "iris", "favorites"} or source.page_configs:
            items.append(
                NavItemConfig(
                    f"{source.key}_home",
                    source.label,
                    source.key,
                    source.default_page,
                )
            )
    return tuple(items)


def build_app_mode_config(app_mode: str, *, nipa_view_columns: tuple[str, ...] = ()) -> AppModeConfig:
    normalized_mode = "viewer" if str(app_mode or "").strip().lower() == "viewer" else "viewer"

    tipa_pages = (
        SourcePageConfig("tipa_current", "Current Notices", "notice", "Current Notices", data_key="mss_current", origin_key="mss_current_origin"),
        SourcePageConfig("tipa_scheduled", "Scheduled Notices", "notice", "Scheduled Notices", data_key="mss_current", origin_key="mss_current_origin"),
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
            "Current Notices",
            "notice",
            "Current Notices",
            data_key="nipa_current",
            origin_key="nipa_current_origin",
            view_columns=tuple(nipa_view_columns),
        ),
        SourcePageConfig(
            "nipa_scheduled",
            "Scheduled Notices",
            "notice",
            "Scheduled Notices",
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
    bipa_pages = (
        SourcePageConfig(
            "bipa_current",
            "Current Notices",
            "notice",
            "Current Notices",
            data_key="bipa_current",
            origin_key="bipa_current_origin",
            view_columns=tuple(nipa_view_columns),
        ),
        SourcePageConfig(
            "bipa_scheduled",
            "Scheduled Notices",
            "notice",
            "Scheduled Notices",
            data_key="bipa_current",
            origin_key="bipa_current_origin",
            view_columns=tuple(nipa_view_columns),
        ),
        SourcePageConfig("bipa_opportunity", "Opportunity", "opportunity", "Opportunity", data_key="bipa_opportunity"),
        SourcePageConfig(
            "bipa_archive",
            "Archive",
            "archive",
            "Archive",
            data_key="bipa_current",
            origin_key="bipa_current_origin",
            secondary_data_key="bipa_past",
            secondary_origin_key="bipa_past_origin",
            view_columns=tuple(nipa_view_columns),
        ),
    )
    bizinfo_pages = (
        SourcePageConfig(
            "bizinfo_current",
            "Current Notices",
            "notice",
            "Current Notices",
            data_key="bizinfo_current",
            origin_key="bizinfo_current_origin",
            view_columns=tuple(nipa_view_columns),
        ),
        SourcePageConfig(
            "bizinfo_scheduled",
            "Scheduled Notices",
            "notice",
            "Scheduled Notices",
            data_key="bizinfo_current",
            origin_key="bizinfo_current_origin",
            view_columns=tuple(nipa_view_columns),
        ),
        SourcePageConfig("bizinfo_opportunity", "Opportunity", "opportunity", "Opportunity", data_key="bizinfo_opportunity"),
        SourcePageConfig(
            "bizinfo_archive",
            "Archive",
            "archive",
            "Archive",
            data_key="bizinfo_current",
            origin_key="bizinfo_current_origin",
            secondary_data_key="bizinfo_past",
            secondary_origin_key="bizinfo_past_origin",
            view_columns=tuple(nipa_view_columns),
        ),
    )

    sources = (
        SourceRouteConfig("dashboard", "Dashboard", "dashboard", True, "dashboard"),
        SourceRouteConfig("notices", "Notice Queue", "notice_queue", True, "notices"),
        SourceRouteConfig("iris", "RFP Queue", "rfp_queue", True, "iris"),
        SourceRouteConfig("tipa", "MSS", "tipa_opportunity", True, "tipa", page_configs=tipa_pages),
        SourceRouteConfig("nipa", "NIPA", "nipa_opportunity", True, "nipa", page_configs=nipa_pages),
        SourceRouteConfig("bipa", "BIPA", "bipa_opportunity", True, "bipa", page_configs=bipa_pages),
        SourceRouteConfig("bizinfo", "BIZINFO", "bizinfo_opportunity", True, "bizinfo", page_configs=bizinfo_pages),
        SourceRouteConfig("proposal", "Proposal", "proposal", False, "proposal"),
        SourceRouteConfig("operations", "Operations", "operations", True, "operations"),
        SourceRouteConfig("favorites", "Favorites", "favorites", True, "favorites"),
    )

    viewer_nav_groups = (
        NavGroupConfig(
            "workspace",
            "Workspace",
            build_workspace_nav_items(sources),
        ),
    )

    return AppModeConfig(
        mode=normalized_mode,
        page_title="RFP Intelligence Viewer",
        header_title="RFP Intelligence Viewer",
        header_caption="Viewer workspace for dashboard, queues, and crawler sources.",
        supports_summary=False,
        nav_groups=viewer_nav_groups,
        sources=sources,
        default_source="dashboard",
        default_iris_page="rfp_queue",
        iris_tabs=(
            ("rfp_queue", "RFP Queue"),
            ("notice_queue", "Notice Queue"),
            ("notice_scheduled", "Scheduled Notices"),
            ("notice_archive", "Archive"),
        ),
        valid_iris_pages=frozenset({"rfp_queue", "notice_queue", "notice_scheduled", "notice_archive"}),
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
