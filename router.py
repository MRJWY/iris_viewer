from dataclasses import dataclass


@dataclass(frozen=True)
class NavItem:
    source_key: str
    page_key: str
    label: str


PRIMARY_NAV_ITEMS = [
    NavItem("dashboard", "dashboard", "오늘의 공고"),
    NavItem("notices", "notice_queue", "전체 공고 검색"),
]


def normalize_primary_route(source_key: str, page_key: str) -> tuple[str, str]:
    normalized_source = (source_key or "").strip().lower()
    normalized_page = (page_key or "").strip().lower()

    legacy_map = {
        ("iris", "summary"): ("iris", "rfp_queue"),
        ("iris", "opportunity"): ("iris", "rfp_queue"),
        ("iris", "notice"): ("notices", "notice_queue"),
        ("iris", "notice_queue"): ("notices", "notice_queue"),
        ("notices", "notice"): ("notices", "notice_queue"),
        ("notices", "notices"): ("notices", "notice_queue"),
    }
    return legacy_map.get((normalized_source, normalized_page), (normalized_source, normalized_page))


def is_primary_nav_route(source_key: str, page_key: str, nav_item: NavItem) -> bool:
    current_source, current_page = normalize_primary_route(source_key, page_key)
    return current_source == nav_item.source_key and current_page == nav_item.page_key
