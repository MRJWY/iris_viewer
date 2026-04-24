from __future__ import annotations

from dataclasses import dataclass


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
    sources: tuple[SourceRouteConfig, ...]
    default_source: str
    default_iris_page: str
    iris_tabs: tuple[tuple[str, str], ...]
    valid_iris_pages: frozenset[str]
    iris_tab_key: str


def build_app_mode_config(app_mode: str, *, nipa_view_columns: tuple[str, ...] = ()) -> AppModeConfig:
    normalized_mode = "viewer" if str(app_mode or "").strip().lower() == "viewer" else "admin"
    tipa_pages = (
        SourcePageConfig("tipa_current", "진행공고", "notice", "진행공고", data_key="mss_current", origin_key="mss_current_origin"),
        SourcePageConfig("tipa_scheduled", "예정공고", "notice", "예정공고", data_key="mss_current", origin_key="mss_current_origin"),
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
            "진행공고",
            "notice",
            "진행공고",
            data_key="nipa_current",
            origin_key="nipa_current_origin",
            view_columns=tuple(nipa_view_columns),
        ),
        SourcePageConfig(
            "nipa_scheduled",
            "예정공고",
            "notice",
            "예정공고",
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
        SourceRouteConfig("iris", "IRIS", "notice", False, "iris"),
        SourceRouteConfig("tipa", "중소기업벤처부", "tipa_current", True, "external", page_configs=tipa_pages),
        SourceRouteConfig("nipa", "NIPA", "nipa_current", True, "external", page_configs=nipa_pages),
        SourceRouteConfig("favorites", "관심 공고", "favorites", True, "favorites"),
    )

    if normalized_mode == "viewer":
        return AppModeConfig(
            mode="viewer",
            page_title="Crawler Hub",
            header_title="Crawler Hub",
            header_caption="IRIS / SUMMARY / OPPORTUNITY 시트를 같은 화면 구조로 조회합니다.",
            supports_summary=True,
            sources=sources,
            default_source="iris",
            default_iris_page="notice",
            iris_tabs=(
                ("notice", "진행공고"),
                ("notice_scheduled", "예정공고"),
                ("summary", "Summary"),
                ("opportunity", "Opportunity"),
                ("notice_archive", "Archive"),
            ),
            valid_iris_pages=frozenset({"opportunity", "notice", "notice_scheduled", "notice_archive", "summary"}),
            iris_tab_key="viewer_iris_page_tabs",
        )

    return AppModeConfig(
        mode="admin",
        page_title="Crawler Hub Admin",
        header_title="Crawler Hub Admin",
        header_caption="현재 요약, 검토 가능한 Opportunity, 누적 Opportunity, 오류 행을 조회합니다.",
        supports_summary=False,
        sources=sources,
        default_source="iris",
        default_iris_page="notice",
        iris_tabs=(
            ("notice", "진행공고"),
            ("notice_scheduled", "예정공고"),
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
