from __future__ import annotations

from collections.abc import Mapping
from html import unescape
import re
from urllib.parse import parse_qs, urlencode, urlparse


NARAJANGTEO_SOURCE_KEYS = {"narajangteo"}
NARAJANGTEO_NOTICE_PATH = "/link/PNPE027_01/single/"
NARAJANGTEO_API_HOST = "apis.data.go.kr"
NARAJANGTEO_API_PATH = "/1230000/ad/"
DOWNLOAD_HINT_TOKENS = ("download", "atch", "attach", "file", "specdoc")
DOWNLOAD_HINT_EXTENSIONS = (".pdf", ".hwp", ".hwpx", ".zip")


def clean(value: object) -> str:
    return str(value or "").strip()


def normalize_source_key(value: object) -> str:
    return clean(value).lower()


def extract_narajangteo_notice_reference(text: str) -> dict[str, str] | None:
    """Read a public notice ID from Slack, Markdown, or plain G2B links."""
    references = {}
    text = unescape(str(text or "").replace("\\&", "&"))
    for raw_url in re.findall(r"https?://[^\s<>|]+", text, re.IGNORECASE):
        parsed = urlparse(raw_url.rstrip(")]>,.;"))
        host = (parsed.hostname or "").lower()
        if host != "g2b.go.kr" and not host.endswith(".g2b.go.kr"):
            continue
        if parsed.path.rstrip("/") != NARAJANGTEO_NOTICE_PATH.rstrip("/"):
            continue
        params = parse_qs(parsed.query)
        numbers, orders = params.get("bidPbancNo", []), params.get("bidPbancOrd", ["000"])
        if len(numbers) != 1 or len(orders) != 1:
            continue
        number, order = numbers[0].upper(), orders[0]
        if not re.fullmatch(r"R\d{2}[A-Z]{2}\d{8}|\d{11}", number) or not re.fullmatch(r"\d{3}", order):
            continue
        references[(number, order)] = {"notice_no": number, "bid_ntce_ord": order}
    if len(references) > 1:
        raise ValueError("낙찰자를 조회할 공고 링크를 한 개만 지정해 주세요.")
    return next(iter(references.values()), None)


def is_narajangteo_source(value: object) -> bool:
    return normalize_source_key(value) in NARAJANGTEO_SOURCE_KEYS


def build_narajangteo_notice_url(notice_id: object) -> str:
    text = clean(notice_id)
    if not text:
        return ""
    if "__" in text:
        bid_no, bid_ord = text.rsplit("__", 1)
    else:
        bid_no, bid_ord = text, "000"
    bid_no = clean(bid_no)
    bid_ord = clean(bid_ord) or "000"
    if not bid_no:
        return ""
    return f"https://www.g2b.go.kr{NARAJANGTEO_NOTICE_PATH}?{urlencode({'bidPbancNo': bid_no, 'bidPbancOrd': bid_ord})}"


def is_narajangteo_notice_url(url: object) -> bool:
    parsed = urlparse(clean(url))
    return parsed.scheme in {"http", "https"} and parsed.netloc.lower().endswith("g2b.go.kr") and parsed.path == NARAJANGTEO_NOTICE_PATH


def is_narajangteo_api_url(url: object) -> bool:
    parsed = urlparse(clean(url))
    return parsed.scheme in {"http", "https"} and parsed.netloc.lower().endswith(NARAJANGTEO_API_HOST) and parsed.path.startswith(NARAJANGTEO_API_PATH)


def is_narajangteo_download_url(url: object) -> bool:
    parsed = urlparse(clean(url))
    lowered = clean(url).lower()
    if any(lowered.endswith(ext) for ext in DOWNLOAD_HINT_EXTENSIONS):
        return True
    if any(ext in parsed.path.lower() for ext in DOWNLOAD_HINT_EXTENSIONS):
        return True
    if any(ext in parsed.query.lower() for ext in DOWNLOAD_HINT_EXTENSIONS):
        return True
    return any(token in lowered for token in DOWNLOAD_HINT_TOKENS)


def resolve_narajangteo_pre_links(detail_link: object, document_link: object) -> tuple[str, str]:
    """Split a preliminary-specification record into notice and document links.

    Preliminary specifications only have a public notice URL after a related
    bid notice exists. Their usual detail link is the specification download.
    """
    detail_url = clean(detail_link)
    document_url = clean(document_link)
    notice_url = detail_url if is_narajangteo_notice_url(detail_url) else ""
    if not document_url and is_narajangteo_download_url(detail_url):
        document_url = detail_url
    return notice_url, document_url


def needs_narajangteo_notice_url_repair(url: object) -> bool:
    text = clean(url)
    if not text:
        return True
    parsed = urlparse(text)
    if parsed.scheme not in {"http", "https"}:
        return True
    if is_narajangteo_notice_url(text):
        return False
    if is_narajangteo_api_url(text):
        return True
    if is_narajangteo_download_url(text):
        return True
    return False


def repair_narajangteo_notice_url(notice_id: object, url: object) -> str:
    current = clean(url)
    if not clean(notice_id):
        return current
    if needs_narajangteo_notice_url_repair(current):
        return build_narajangteo_notice_url(notice_id)
    return current


def repair_narajangteo_notice_links(row: Mapping[str, object], *, source_site: object = "") -> dict[str, str]:
    repaired = {key: clean(value) for key, value in row.items()}
    normalized_source = normalize_source_key(
        source_site
        or row.get("source_site")
        or row.get("source_key")
        or row.get("_source_key")
        or row.get("출처사이트")
    )
    if not is_narajangteo_source(normalized_source):
        return repaired

    notice_id = clean(row.get("notice_id") or row.get("source_notice_id") or row.get("공고ID"))
    if not notice_id:
        return repaired

    detail_link = row.get("detail_link") or row.get("상세링크")
    source_notice_url = row.get("source_notice_url") or row.get("source_url") or row.get("notice_url") or detail_link

    repaired_detail_link = repair_narajangteo_notice_url(notice_id, detail_link)
    repaired_source_notice_url = repair_narajangteo_notice_url(notice_id, source_notice_url or repaired_detail_link)

    repaired["detail_link"] = repaired_detail_link
    repaired["source_notice_url"] = repaired_source_notice_url
    return repaired
