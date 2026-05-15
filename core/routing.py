from __future__ import annotations

from copy import deepcopy
from typing import Any, Mapping

import streamlit as st


LEGACY_PAGE_KEY_MAP = {
    "opportunity": "rfp_queue",
    "notice": "notice_queue",
    "mss_current": "tipa_current",
    "mss_past": "tipa_archive",
    "mss_archive": "tipa_archive",
    "mss_opportunity": "tipa_opportunity",
}

DEFAULT_ROUTE: dict[str, Any] = {
    "source": "iris",
    "page": "rfp_queue",
    "view": "list",
    "item_type": "",
    "item_id": "",
    "source_key": "",
    "filters": {},
    "page_no": 1,
    "page_size": 20,
}

ROUTE_STATE_KEY = "current_route"
ROUTE_STACK_KEY = "route_stack"


def _clean(value: object) -> str:
    if value is None:
        return ""
    return str(value).strip()


def normalize_page_key(page_key: object) -> str:
    return LEGACY_PAGE_KEY_MAP.get(_clean(page_key), _clean(page_key))


def normalize_route(route: Mapping[str, Any] | None = None) -> dict[str, Any]:
    route = dict(route or {})
    normalized = deepcopy(DEFAULT_ROUTE)
    normalized["source"] = _clean(route.get("source")) or DEFAULT_ROUTE["source"]
    normalized["page"] = normalize_page_key(route.get("page")) or DEFAULT_ROUTE["page"]
    normalized["view"] = _clean(route.get("view")) or DEFAULT_ROUTE["view"]
    normalized["item_type"] = _clean(route.get("item_type"))
    normalized["item_id"] = _clean(route.get("item_id") or route.get("id"))
    normalized["source_key"] = _clean(route.get("source_key")) or normalized["source"]
    normalized["filters"] = dict(route.get("filters") or {})
    try:
        normalized["page_no"] = max(1, int(route.get("page_no") or DEFAULT_ROUTE["page_no"]))
    except Exception:
        normalized["page_no"] = DEFAULT_ROUTE["page_no"]
    try:
        normalized["page_size"] = max(1, int(route.get("page_size") or DEFAULT_ROUTE["page_size"]))
    except Exception:
        normalized["page_size"] = DEFAULT_ROUTE["page_size"]
    return normalized


def route_equals(left: Mapping[str, Any] | None, right: Mapping[str, Any] | None) -> bool:
    return normalize_route(left) == normalize_route(right)


def get_current_route(default_route: Mapping[str, Any] | None = None) -> dict[str, Any]:
    candidate = st.session_state.get(ROUTE_STATE_KEY)
    if isinstance(candidate, Mapping):
        return normalize_route(candidate)
    if default_route is not None:
        return normalize_route(default_route)
    return normalize_route(DEFAULT_ROUTE)


def set_current_route(route: Mapping[str, Any]) -> dict[str, Any]:
    normalized = normalize_route(route)
    st.session_state[ROUTE_STATE_KEY] = normalized
    return normalized


def update_current_route(**changes: Any) -> dict[str, Any]:
    current = get_current_route()
    current.update(changes)
    return set_current_route(current)


def get_route_stack() -> list[dict[str, Any]]:
    raw_stack = st.session_state.get(ROUTE_STACK_KEY, [])
    if not isinstance(raw_stack, list):
        return []
    normalized: list[dict[str, Any]] = []
    for item in raw_stack:
        if isinstance(item, Mapping):
            normalized.append(normalize_route(item))
    return normalized


def clear_route_stack() -> None:
    st.session_state[ROUTE_STACK_KEY] = []


def _set_route_stack(stack: list[Mapping[str, Any]]) -> None:
    st.session_state[ROUTE_STACK_KEY] = [normalize_route(item) for item in stack]


def navigate_to(route: Mapping[str, Any], *, push: bool = True) -> dict[str, Any]:
    next_route = normalize_route(route)
    current_route = get_current_route()
    if push and not route_equals(current_route, next_route):
        stack = get_route_stack()
        if not stack or not route_equals(stack[-1], current_route):
            stack.append(current_route)
            _set_route_stack(stack)
    return set_current_route(next_route)


def go_back(fallback_route: Mapping[str, Any] | None = None) -> dict[str, Any]:
    stack = get_route_stack()
    if stack:
        previous = stack.pop()
        _set_route_stack(stack)
        return set_current_route(previous)
    if fallback_route is not None:
        return set_current_route(fallback_route)
    return set_current_route(DEFAULT_ROUTE)


def serialize_route(route: Mapping[str, Any] | None = None) -> dict[str, str]:
    current = normalize_route(route or get_current_route())
    params: dict[str, str] = {
        "source": _clean(current.get("source")),
        "page": _clean(current.get("page")),
        "view": _clean(current.get("view")),
    }
    item_id = _clean(current.get("item_id"))
    if item_id:
        params["id"] = item_id
    return params


def deserialize_route(
    query_params: Mapping[str, Any] | None = None,
    *,
    default_route: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    params = dict(query_params or {})
    route = normalize_route(default_route)
    route["source"] = _clean(params.get("source")) or route["source"]
    route["page"] = normalize_page_key(params.get("page")) or route["page"]
    route["view"] = _clean(params.get("view")) or route["view"]
    route["item_id"] = _clean(params.get("id"))
    route["source_key"] = route["source"]
    if route["page"] == "dashboard":
        route["source"] = "dashboard"
        route["source_key"] = "dashboard"
    elif route["page"] == "favorites":
        route["source"] = "favorites"
        route["source_key"] = "favorites"
    return route


def init_route(
    *,
    default_route: Mapping[str, Any],
    query_params: Mapping[str, Any] | None = None,
    reset_stack: bool = False,
) -> dict[str, Any]:
    current = st.session_state.get(ROUTE_STATE_KEY)
    if not isinstance(current, Mapping):
        set_current_route(deserialize_route(query_params, default_route=default_route))
    if reset_stack or not isinstance(st.session_state.get(ROUTE_STACK_KEY), list):
        clear_route_stack()
    return get_current_route(default_route)


def build_dashboard_route(*, view: str = "list", filters: Mapping[str, Any] | None = None) -> dict[str, Any]:
    return normalize_route(
        {
            "source": "dashboard",
            "page": "dashboard",
            "view": view,
            "source_key": "dashboard",
            "filters": dict(filters or {}),
        }
    )


def build_rfp_queue_route(
    *,
    filters: Mapping[str, Any] | None = None,
    page_no: int = 1,
    page_size: int = 20,
    view: str = "list",
    item_id: str = "",
    source_key: str = "iris",
) -> dict[str, Any]:
    return normalize_route(
        {
            "source": "iris",
            "page": "rfp_queue",
            "view": view,
            "item_type": "rfp" if item_id else "",
            "item_id": item_id,
            "source_key": source_key,
            "filters": dict(filters or {}),
            "page_no": page_no,
            "page_size": page_size,
        }
    )


def build_notice_queue_route(
    *,
    filters: Mapping[str, Any] | None = None,
    page_no: int = 1,
    page_size: int = 20,
    view: str = "list",
    item_id: str = "",
    source_key: str = "iris",
) -> dict[str, Any]:
    return normalize_route(
        {
            "source": "notices",
            "page": "notice_queue",
            "view": view,
            "item_type": "notice" if item_id else "",
            "item_id": item_id,
            "source_key": source_key,
            "filters": dict(filters or {}),
            "page_no": page_no,
            "page_size": page_size,
        }
    )


def build_favorites_route(
    *,
    filters: Mapping[str, Any] | None = None,
    page_no: int = 1,
    page_size: int = 20,
    view: str = "list",
    item_id: str = "",
    source_key: str = "favorites",
) -> dict[str, Any]:
    return normalize_route(
        {
            "source": "favorites",
            "page": "favorites",
            "view": view,
            "item_type": "notice" if item_id else "",
            "item_id": item_id,
            "source_key": source_key,
            "filters": dict(filters or {}),
            "page_no": page_no,
            "page_size": page_size,
        }
    )


def build_rfp_detail_route(item_id: str, source_key: str | None = None) -> dict[str, Any]:
    detail_source_key = _clean(source_key) or "iris"
    return normalize_route(
        {
            "source": "iris",
            "page": "rfp_queue",
            "view": "detail",
            "item_type": "rfp",
            "item_id": item_id,
            "source_key": detail_source_key,
        }
    )


def build_notice_detail_route(notice_id: str, source_key: str | None = None) -> dict[str, Any]:
    normalized_source = _clean(source_key) or "iris"
    detail_source_key = normalized_source if normalized_source in {"iris", "tipa", "nipa", "favorites"} else "iris"
    page_key = "favorites" if detail_source_key == "favorites" else "notice_queue"
    return normalize_route(
        {
            "source": "favorites" if page_key == "favorites" else "notices",
            "page": page_key,
            "view": "detail",
            "item_type": "notice",
            "item_id": notice_id,
            "source_key": detail_source_key,
        }
    )
