import json
import re
from html import escape

import pandas as pd
import streamlit as st
import streamlit.components.v1 as components


def apply_notice_browser_overrides(ns: dict, *, detail_page_key: str) -> None:
    clean = ns["clean"]
    first_non_empty = ns["first_non_empty"]
    normalize_notice_status_label = ns["normalize_notice_status_label"]
    resolve_route_source_key_for_row = ns["resolve_route_source_key_for_row"]
    build_route_href = ns["build_route_href"]
    original_render_notice_detail_from_row = ns["render_notice_detail_from_row"]

    def _coerce_notice_browser_links(raw, default_label: str) -> list[dict[str, str]]:
        items: list[dict[str, str]] = []

        def push(url: object, label: object = "") -> None:
            normalized_url = clean(url)
            if not normalized_url:
                return
            normalized_label = clean(label) or default_label
            items.append({"label": normalized_label, "url": normalized_url})

        def visit(value) -> None:
            if value is None:
                return
            try:
                if pd.isna(value):
                    return
            except TypeError:
                pass

            if isinstance(value, list):
                for entry in value:
                    visit(entry)
                return

            if isinstance(value, dict):
                nested = value.get("attachments") or value.get("items") or value.get("files") or value.get("links")
                if isinstance(nested, list):
                    visit(nested)
                push(
                    first_non_empty(value, "download_url", "url", "file_url", "attachment_url", "link", "href"),
                    first_non_empty(value, "file_name", "name", "title", "text", "label"),
                )
                return

            text = clean(value)
            if not text:
                return
            if text.startswith("[") or text.startswith("{"):
                try:
                    visit(json.loads(text))
                    return
                except Exception:
                    pass
            if re.match(r"^https?://", text, flags=re.IGNORECASE):
                push(text, default_label)

        visit(raw)

        seen: set[tuple[str, str]] = set()
        deduped: list[dict[str, str]] = []
        for item in items:
            key = (clean(item.get("url")), clean(item.get("label")))
            if not key[0] or key in seen:
                continue
            seen.add(key)
            deduped.append({"label": key[1] or default_label, "url": key[0]})
        return deduped

    def _extract_notice_browser_assets(row: dict | pd.Series | None) -> dict[str, object]:
        row_dict = row.to_dict() if isinstance(row, pd.Series) else dict(row or {})

        attachments: list[dict[str, str]] = []
        for field_name in ("attachments", "attachment_links", "rfp_files", "첨부파일", "첨부파일목록"):
            attachments.extend(_coerce_notice_browser_links(row_dict.get(field_name), "첨부파일"))

        direct_rfp_url = first_non_empty(row_dict, "download_url", "file_url", "attachment_url")
        rfp_label = first_non_empty(row_dict, "file_name", "파일명", "rfp_title") or "RFP 다운로드"
        rfp_candidates: list[dict[str, str]] = []
        if clean(direct_rfp_url):
            rfp_candidates.append({"label": clean(rfp_label) or "RFP 다운로드", "url": clean(direct_rfp_url)})

        rfp_candidates.extend(_coerce_notice_browser_links(row_dict.get("rfp_files"), "RFP 다운로드"))

        if not rfp_candidates:
            for item in attachments:
                label_key = clean(item.get("label")).lower()
                if any(keyword in label_key for keyword in ("rfp", "proposal", "제안요청", "사업계획", "공고", "신청")):
                    rfp_candidates.append(item)

        seen_urls: set[str] = set()
        deduped_attachments: list[dict[str, str]] = []
        for item in attachments:
            url = clean(item.get("url"))
            if not url or url in seen_urls:
                continue
            seen_urls.add(url)
            deduped_attachments.append({
                "label": clean(item.get("label")) or "첨부파일",
                "url": url,
            })

        rfp_download = rfp_candidates[0] if rfp_candidates else {}
        return {
            "rfp_download": {
                "label": clean(rfp_download.get("label")) or "RFP 다운로드",
                "url": clean(rfp_download.get("url")),
            },
            "attachments": deduped_attachments,
        }

    def _build_notice_browser_payload(row: dict | pd.Series | None, *, detail_page_key: str) -> dict[str, object]:
        row_dict = row.to_dict() if isinstance(row, pd.Series) else dict(row or {})
        source_key = resolve_route_source_key_for_row(row_dict, source_key=row_dict.get("source_key")) or "iris"
        scope = clean(first_non_empty(row_dict, "_notice_scope"))
        status = normalize_notice_status_label(first_non_empty(row_dict, "공고상태", "status", "rcve_status"))
        if not status:
            status = "마감" if scope == "archive" else "예정" if scope == "scheduled" else "접수중"

        notice_id = clean(first_non_empty(row_dict, "공고ID", "notice_id"))
        collection_id = clean(first_non_empty(row_dict, "_collection_id"))
        source_label = clean(first_non_empty(row_dict, "매체", "source_label")) or source_key.upper()
        ministry = clean(first_non_empty(row_dict, "소관부처", "ministry"))
        agency = clean(first_non_empty(row_dict, "전문기관", "담당부처", "agency"))
        breadcrumb_parts = [part for part in [ministry, agency] if clean(part) and part != "-"]
        breadcrumb = " > ".join(breadcrumb_parts) if breadcrumb_parts else source_label
        favorite_key = f"{source_key}::{notice_id}" if notice_id else f"{source_key}::{collection_id}"
        assets = _extract_notice_browser_assets(row_dict)

        return {
            "favorite_key": favorite_key,
            "notice_id": notice_id,
            "collection_id": collection_id,
            "source_key": source_key,
            "source_label": source_label,
            "breadcrumb": breadcrumb,
            "title": clean(first_non_empty(row_dict, "공고명", "notice_title")) or "-",
            "notice_no": clean(first_non_empty(row_dict, "공고번호", "notice_no")) or "-",
            "notice_date": clean(first_non_empty(row_dict, "공고일자", "notice_date")) or "-",
            "status": status or "-",
            "support_type": clean(first_non_empty(row_dict, "공모유형", "pbofr_type", "support_type")) or "-",
            "period": clean(first_non_empty(row_dict, "접수기간", "notice_period", "period")) or "-",
            "detail_href": build_route_href(detail_page_key, collection_id, source_key=source_key) if collection_id else "#",
            "rfp_download": assets["rfp_download"],
            "attachments": assets["attachments"],
        }

    def _render_notice_browser_rows_component(
        payloads: list[dict[str, object]],
        *,
        component_key: str,
        favorites_only: bool = False,
        local_storage_only: bool = False,
        empty_message: str = "표시할 공고가 없습니다.",
        max_height: int = 1800,
    ) -> None:
        payload_json = json.dumps(payloads, ensure_ascii=False).replace("</", "<\\/")
        empty_message_json = json.dumps(clean(empty_message) or "표시할 공고가 없습니다.", ensure_ascii=False)
        mode_json = json.dumps({"favorites_only": favorites_only, "local_storage_only": local_storage_only})
        row_count = len(payloads) if payloads else (8 if local_storage_only else 1)
        height = min(max(220, 176 * max(row_count, 1) + 40), max_height)
        html = f"""
        <div id="{escape(component_key, quote=True)}" class="notice-browser-root"></div>
        <script>
        (function() {{
          const root = document.getElementById({json.dumps(component_key)});
          if (!root) return;
          const storageKey = "favorite_notices";
          const initialRows = {payload_json};
          const mode = {mode_json};
          const emptyMessage = {empty_message_json};

          const escapeHtml = (value) => String(value ?? "")
            .replace(/&/g, "&amp;")
            .replace(/</g, "&lt;")
            .replace(/>/g, "&gt;")
            .replace(/"/g, "&quot;")
            .replace(/'/g, "&#39;");

          const normalizeKey = (item) => String(item?.favorite_key || "");
          const readFavorites = () => {{
            try {{
              const raw = window.localStorage.getItem(storageKey);
              const parsed = raw ? JSON.parse(raw) : [];
              return Array.isArray(parsed) ? parsed.filter(Boolean) : [];
            }} catch (error) {{
              return [];
            }}
          }};
          const writeFavorites = (items) => {{
            window.localStorage.setItem(storageKey, JSON.stringify(items));
          }};
          const upsertFavorite = (payload) => {{
            const favorites = readFavorites();
            const key = normalizeKey(payload);
            const next = favorites.filter((item) => normalizeKey(item) !== key);
            next.unshift(payload);
            writeFavorites(next);
          }};
          const removeFavorite = (payload) => {{
            const key = normalizeKey(payload);
            writeFavorites(readFavorites().filter((item) => normalizeKey(item) !== key));
          }};
          const renderAttachmentLinks = (attachments) => {{
            if (!Array.isArray(attachments) || !attachments.length) {{
              return '<span class="notice-browser-asset-empty">첨부파일 없음</span>';
            }}
            return `
              <div class="notice-browser-attachment-list">
                ${{attachments.map((file) => `
                  <a class="notice-browser-attachment-link" data-role="attachment-link" href="${{escapeHtml(file.url || '#')}}" target="_blank" rel="noopener noreferrer">
                    ${{escapeHtml(file.label || '첨부파일')}}
                  </a>
                `).join("")}}
              </div>`;
          }};
          const rowHtml = (item, isFavorite) => {{
            const buttonClass = isFavorite ? "notice-browser-favorite is-active" : "notice-browser-favorite";
            const buttonLabel = isFavorite ? "★ 관심등록됨" : "☆ 관심공고 등록";
            const badgeHtml = isFavorite ? '<span class="notice-browser-badge">관심</span>' : "";
            const rfp = item.rfp_download || {{}};
            const hasRfp = Boolean(rfp.url);
            return `
              <div class="notice-browser-row-shell" tabindex="0" role="link"
                   data-detail-href="${{escapeHtml(item.detail_href || '#')}}"
                   data-favorite-payload='${{escapeHtml(JSON.stringify(item))}}'>
                <button type="button" class="${{buttonClass}}" data-role="favorite">${{buttonLabel}}</button>
                <div class="notice-browser-row">
                  <div class="notice-browser-breadcrumb">${{escapeHtml(item.breadcrumb || item.source_label || '-')}}</div>
                  <div class="notice-browser-title-row">
                    <div class="notice-browser-title">${{escapeHtml(item.title || '-')}}</div>
                    ${{badgeHtml}}
                  </div>
                  <div class="notice-browser-meta">
                    <div class="notice-browser-meta-item"><span class="notice-browser-meta-label">공고번호</span> ${{escapeHtml(item.notice_no || '-')}}</div>
                    <div class="notice-browser-meta-item"><span class="notice-browser-meta-label">공고일자</span> ${{escapeHtml(item.notice_date || '-')}}</div>
                    <div class="notice-browser-meta-item"><span class="notice-browser-meta-label">공고상태</span> ${{escapeHtml(item.status || '-')}}</div>
                    <div class="notice-browser-meta-item"><span class="notice-browser-meta-label">공모유형</span> ${{escapeHtml(item.support_type || '-')}}</div>
                    <div class="notice-browser-meta-item"><span class="notice-browser-meta-label">매체</span> ${{escapeHtml(item.source_label || '-')}}</div>
                    <div class="notice-browser-meta-item"><span class="notice-browser-meta-label">접수기간</span> ${{escapeHtml(item.period || '-')}}</div>
                  </div>
                  <div class="notice-browser-assets">
                    <div class="notice-browser-assets-group">
                      <span class="notice-browser-assets-label">RFP</span>
                      ${{hasRfp
                        ? `<a class="notice-browser-action-button" data-role="attachment-link" href="${{escapeHtml(rfp.url)}}" target="_blank" rel="noopener noreferrer">${{escapeHtml(rfp.label || 'RFP 다운로드')}}</a>`
                        : '<span class="notice-browser-asset-empty">RFP 파일 없음</span>'}}
                    </div>
                    <div class="notice-browser-assets-group">
                      <span class="notice-browser-assets-label">첨부파일</span>
                      ${{renderAttachmentLinks(item.attachments)}}
                    </div>
                  </div>
                </div>
              </div>`;
          }};
          const styles = `
            <style>
              body {{ margin: 0; background: transparent; font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif; }}
              .notice-browser-list {{ display:flex; flex-direction:column; border-top:1px solid rgba(203,213,225,.85); }}
              .notice-browser-row-shell {{ position:relative; border-bottom:1px solid rgba(203,213,225,.85); }}
              .notice-browser-row-shell:hover .notice-browser-row {{ background:#f8fafc; }}
              .notice-browser-row-shell:focus-visible {{ outline: 2px solid #2563eb; outline-offset: 2px; }}
              .notice-browser-row {{ padding: 1.15rem 230px 1.1rem .25rem; cursor:pointer; transition:background-color .14s ease; }}
              .notice-browser-breadcrumb {{ color:#64748b; font-size:.85rem; font-weight:700; line-height:1.45; margin-bottom:.35rem; }}
              .notice-browser-title-row {{ display:flex; align-items:center; gap:.55rem; margin-bottom:.45rem; flex-wrap:wrap; }}
              .notice-browser-title {{ color:#0f172a; font-size:1.16rem; font-weight:900; line-height:1.45; }}
              .notice-browser-badge {{ display:inline-flex; align-items:center; height:24px; padding:0 .6rem; border-radius:999px; background:#dbeafe; color:#1d4ed8; font-size:.78rem; font-weight:800; }}
              .notice-browser-meta {{ display:flex; flex-wrap:wrap; gap:.4rem 1rem; align-items:center; }}
              .notice-browser-meta-item {{ color:#334155; font-size:.92rem; line-height:1.5; }}
              .notice-browser-meta-label {{ color:#64748b; font-weight:800; }}
              .notice-browser-assets {{ display:flex; flex-wrap:wrap; gap:1rem 2rem; margin-top:.85rem; }}
              .notice-browser-assets-group {{ display:flex; flex-direction:column; gap:.4rem; min-width:220px; }}
              .notice-browser-assets-label {{ color:#64748b; font-size:.8rem; font-weight:800; }}
              .notice-browser-action-button {{
                display:inline-flex; align-items:center; justify-content:center; width:fit-content; min-height:34px; padding:0 12px;
                border-radius:8px; border:1px solid rgba(203,213,225,.95); background:#fff; color:#1e293b;
                text-decoration:none; font-size:13px; font-weight:700;
              }}
              .notice-browser-action-button:hover {{ background:#f8fafc; }}
              .notice-browser-attachment-list {{ display:flex; flex-wrap:wrap; gap:.45rem; }}
              .notice-browser-attachment-link {{
                color:#2563eb; text-decoration:none; font-size:.88rem; font-weight:700; line-height:1.4;
              }}
              .notice-browser-attachment-link:hover {{ text-decoration:underline; }}
              .notice-browser-asset-empty {{ color:#94a3b8; font-size:.88rem; }}
              .notice-browser-favorite {{
                position:absolute; top:50%; right:44px; transform:translateY(-50%); z-index:4; display:inline-flex; align-items:center; justify-content:center;
                height:36px; padding:0 14px; border-radius:8px; border:1px solid rgba(203,213,225,.95); background:#fff;
                color:#475569; font-size:13px; font-weight:700; cursor:pointer;
              }}
              .notice-browser-favorite:hover {{ background:#f8fafc; }}
              .notice-browser-favorite.is-active {{ border-color:#2563eb; background:#eff6ff; color:#1d4ed8; }}
              .notice-browser-empty {{ color:#64748b; font-size:.95rem; padding:1.25rem .25rem; }}
              @media (max-width: 960px) {{
                .notice-browser-row-shell {{ padding-top:3.15rem; }}
                .notice-browser-favorite {{ top:0; right:0; transform:none; }}
                .notice-browser-row {{ padding:.95rem 0 1rem 0; }}
                .notice-browser-assets-group {{ min-width:100%; }}
              }}
            </style>`;

          const container = document.createElement("div");
          container.innerHTML = styles + '<div class="notice-browser-list"></div><div class="notice-browser-empty" hidden></div>';
          root.replaceChildren(container);
          const listNode = container.querySelector(".notice-browser-list");
          const emptyNode = container.querySelector(".notice-browser-empty");
          const getSourceRows = () => mode.local_storage_only ? readFavorites() : initialRows;

          const navigateTo = (href) => {{
            if (!href || href === "#") return;
            try {{
              window.parent.location.href = href;
            }} catch (error) {{
              window.location.href = href;
            }}
          }};

          const bindRowEvents = () => {{
            listNode.querySelectorAll(".notice-browser-row-shell").forEach((rowNode) => {{
              const favoriteButton = rowNode.querySelector('[data-role="favorite"]');
              rowNode.addEventListener("click", (event) => {{
                if (event.target.closest('[data-role="favorite"]') || event.target.closest('[data-role="attachment-link"]')) return;
                navigateTo(rowNode.dataset.detailHref);
              }});
              rowNode.addEventListener("keydown", (event) => {{
                if (event.target.closest('[data-role="favorite"]') || event.target.closest('[data-role="attachment-link"]')) return;
                if (event.key === "Enter" || event.key === " ") {{
                  event.preventDefault();
                  navigateTo(rowNode.dataset.detailHref);
                }}
              }});
              if (favoriteButton) {{
                favoriteButton.addEventListener("click", (event) => {{
                  event.preventDefault();
                  event.stopPropagation();
                  const payload = JSON.parse(rowNode.dataset.favoritePayload || "{{}}");
                  const favoriteKeys = new Set(readFavorites().map(normalizeKey));
                  if (favoriteKeys.has(normalizeKey(payload))) {{
                    removeFavorite(payload);
                  }} else {{
                    upsertFavorite(payload);
                  }}
                  render();
                }});
              }}
            }});
          }};

          function render() {{
            const favoriteKeys = new Set(readFavorites().map(normalizeKey));
            const rows = getSourceRows().filter(Boolean);
            const visibleRows = mode.favorites_only
              ? rows.filter((item) => favoriteKeys.has(normalizeKey(item)))
              : rows;

            if (!visibleRows.length) {{
              listNode.innerHTML = "";
              emptyNode.hidden = false;
              emptyNode.textContent = emptyMessage;
              return;
            }}

            emptyNode.hidden = true;
            listNode.innerHTML = visibleRows.map((item) => rowHtml(item, favoriteKeys.has(normalizeKey(item)))).join("");
            bindRowEvents();
          }}

          window.addEventListener("storage", render);
          render();
        }})();
        </script>
        """
        components.html(html, height=height, scrolling=row_count > 6 or local_storage_only)

    def render_local_favorite_notice_rows(*, component_key: str, empty_message: str) -> None:
        _render_notice_browser_rows_component(
            [],
            component_key=component_key,
            favorites_only=True,
            local_storage_only=True,
            empty_message=empty_message,
        )

    def render_crawled_notice_rows(
        rows: pd.DataFrame,
        *,
        key_prefix: str,
        limit: int = 30,
        page_key: str = detail_page_key,
    ) -> None:
        if rows is None or rows.empty:
            st.info("표시할 공고가 없습니다.")
            return
        payloads = [
            _build_notice_browser_payload(row, detail_page_key=detail_page_key)
            for _, row in rows.head(limit).iterrows()
        ]
        _render_notice_browser_rows_component(
            payloads,
            component_key=f"{key_prefix}_{page_key}_notice_rows_assets",
            empty_message="표시할 공고가 없습니다.",
        )

    def _render_notice_asset_links_section(row: dict | pd.Series | None) -> None:
        assets = _extract_notice_browser_assets(row)
        attachments = assets.get("attachments") or []
        rfp_download = assets.get("rfp_download") or {}

        st.markdown('<div class="detail-section-title">RFP / 첨부파일</div>', unsafe_allow_html=True)
        action_col, list_col = st.columns([1, 2], gap="large")
        with action_col:
            if clean(rfp_download.get("url")):
                st.link_button(
                    clean(rfp_download.get("label")) or "RFP 다운로드",
                    clean(rfp_download.get("url")),
                    use_container_width=True,
                )
            else:
                st.caption("RFP 파일 없음")
        with list_col:
            st.markdown("**첨부파일 보기**")
            if attachments:
                for file in attachments:
                    file_url = clean(file.get("url"))
                    file_label = clean(file.get("label")) or "첨부파일"
                    if file_url:
                        st.markdown(f"- [{file_label}]({file_url})")
            else:
                st.caption("첨부파일 없음")

    def render_notice_detail_from_row(row: dict, opportunity_df: pd.DataFrame) -> None:
        original_render_notice_detail_from_row(row, opportunity_df)
        if not row:
            return
        _render_notice_asset_links_section(row)

    ns["_coerce_notice_browser_links"] = _coerce_notice_browser_links
    ns["_extract_notice_browser_assets"] = _extract_notice_browser_assets
    ns["_build_notice_browser_payload"] = _build_notice_browser_payload
    ns["_render_notice_browser_rows_component"] = _render_notice_browser_rows_component
    ns["render_local_favorite_notice_rows"] = render_local_favorite_notice_rows
    ns["render_crawled_notice_rows"] = render_crawled_notice_rows
    ns["render_notice_detail_from_row"] = render_notice_detail_from_row
