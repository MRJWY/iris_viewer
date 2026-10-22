from __future__ import annotations

import re


def clean(value: object) -> str:
    return str(value or "").strip()


def normalize_space(value: object) -> str:
    return re.sub(r"\s+", " ", clean(value)).strip()


PERIOD_PLACEHOLDERS = {"", "-", "\uc5c6\uc74c", "\ubbf8\uae30\uc7ac", "\ubbf8\uc815", "\uae30\uac04"}
PERIOD_BAD_TOKEN_KEYS = {
    "\uae30\uac04",
    "\uacfc\uc81c\uae30\uac04",
    "\uae30\uc220\ub8cc",
    "\uc720\ud615",
    "\uacf5\uace0",
    "\uacfc\uc81c",
}
PERIOD_TABLE_HEADER_MARKERS = (
    "\uc5f0\uad6c\ucc45\uc784\uc790",
    "\uc628\ub77c\uc778",
    "\uc2e0\uccad\uae30\uac04",
    "\uc2b9\uc778\uae30\uac04",
    "\uc5f0\uad6c\uac1c\ubc1c\uae30\uad00",
)
PERIOD_NARRATIVE_MARKERS = (
    "\uc778\uac74\ube44",
    "\ucc38\uc5ec\uc5f0\uad6c\uc790",
    "\uc2e0\uaddc\uc778\ub825",
    "\uac04\uc8fc",
    "\ud604\uae08 \uc0b0\uc815",
    "\uc815\ubd80\uc9c0\uc6d0\uc5f0\uad6c\uac1c\ubc1c\ube44",
    "\uae30\uad00\ubd80\ub2f4\uc5f0\uad6c\uac1c\ubc1c\ube44",
)
PERIOD_PATTERNS = (
    r"20\d{2}[./-]\d{1,2}(?:[./-]\d{1,2})?\s*[~\-]\s*20\d{2}[./-]\d{1,2}(?:[./-]\d{1,2})?",
    r"\d+\s*~\s*\d+\s*\uac1c\uc6d4(?:\s*\([^)\n]{1,40}\))?",
    r"\d+\s*(?:\uac1c\uc6d4|\ub144)(?:\s*\([^)\n]{1,40}\))?",
    r"\ud611\uc57d\s*\ud6c4\s*\d+\s*(?:\uac1c\uc6d4|\ub144)",
)

BUDGET_PLACEHOLDERS = {"", "-", "\uc5c6\uc74c", "\ubbf8\uae30\uc7ac", "\ubbf8\uc815", "\uc608\uc0b0"}
BUDGET_REJECT_MARKERS = (
    "\uc5f0\uad6c\uc2dc\uc124",
    "\uc7a5\ube44",
    "\uc720\uc758\uc0ac\ud56d",
    "\ucc38\uc5ec\uc5f0\uad6c\uc790",
    "\uc2e0\uaddc\uc778\ub825",
    "\uac04\uc8fc",
    "\ud604\uae08 \uc0b0\uc815",
    "\uacfc\uc81c\uc81c\uc548\uc694\uad6c\uc11c",
    "\ubcc4\ub3c4\uc758 \uae30\uc900",
    "\uacf5\uace0\ubb38 \uc548\ub0b4 \ucc38\uace0",
)
BUDGET_LABEL_RE = re.compile(
    r"(?:\ucd1d\s*)?"
    r"(?:\uc0ac\uc5c5\ube44|\uc608\uc0b0|\uc815\ubd80\uc9c0\uc6d0(?:\uc5f0\uad6c\uac1c\ubc1c\ube44)?|\uae30\uad00\ubd80\ub2f4\uc5f0\uad6c\uac1c\ubc1c\ube44|\uc9c0\uc6d0\uae08|\uc9c0\uc6d0\uaddc\ubaa8|\uacfc\uc81c\ubcc4)"
    r"[^\n]{0,50}?\d[\d,\.]*\s*(?:\uc5b5\uc6d0|\ucc9c\ub9cc\uc6d0|\ubc31\ub9cc\uc6d0|\ub9cc\uc6d0|\ucc9c\uc6d0|\uc5b5|\uc6d0)",
    flags=re.IGNORECASE,
)
BUDGET_AMOUNT_RE = re.compile(
    r"\d[\d,\.]*\s*(?:\uc5b5\uc6d0|\ucc9c\ub9cc\uc6d0|\ubc31\ub9cc\uc6d0|\ub9cc\uc6d0|\ucc9c\uc6d0|\uc5b5|\uc6d0)",
    flags=re.IGNORECASE,
)

PROJECT_NAME_PLACEHOLDERS = {
    "",
    "\uc5c6\uc74c",
    "\ubbf8\uae30\uc7ac",
    "\ubbf8\uc815",
    "\uacfc\uc81c\uba85 \ubbf8\uae30\uc7ac",
}
PROJECT_NAME_EXACT_LOW_VALUE_KEYS = {
    "및 내용",
    "\uac1c\ubc1c\uacfc\uac80\uc99d",
    "\uac1c\ubc1c\uacfc \uac80\uc99d",
    "\uac1c\ubc1c \ubc0f \uac80\uc99d",
    "\uc18c\uc7ac \uc0ac\uc5c5\ud654",
    "\uae30\uc220 \uc0ac\uc5c5\ud654",
    "\uc0ac\uc5c5\ud654",
    "\uac80\uc99d",
    "\uc2e4\uc99d",
    "\uae30\ud68d",
    "\uc5f0\uad6c\uae30\uac04",
    "\ucd1d \uc5f0\uad6c\uae30\uac04",
    "\ub300\uc0c1\uc5f0\uad6c\uac1c\ubc1c\uacfc\uc81c",
    "\ub300\uc0c1 \uc5f0\uad6c\uac1c\ubc1c\uacfc\uc81c",
    "\uc5f0\uad6c\uac1c\ubc1c\uacfc\uc81c",
    "\ub300\uc0c1\uacfc\uc81c",
    "\uc8fc\uc694 \ub0b4\uc6a9",
    "\uae30\ud68d\uc5f0\uad6c",
    "\uac1c\ubc1c\uc0ac\uc5c5\uc758 \uae30\ud68d\uc5f0\uad6c",
    "\ub4f1\uae09] \ud488\ubaa9\uc815\uc758 \ube44\uace0",
    "\uacfc\uc81c \uc720\ud615 \uc815\ubd80\uc9c0\uc6d0",
    "\uc18c\uad00 \ubd80\ucc98",
    "\uc791\uc131\uc6d0\uce59",
    "\uacfc\uc81c \uc9c0\uc6d0",
    "\uc138\ubd80 \uc9c0\uc6d0\ub0b4\uc6a9",
    "\uc81c\uc7ac\ud604\ud669\ubaa9\ub85d",
    "\ub300\ud45c\uc790 \uc131\uba85",
    "\ud45c\ucc3d \uc2e0\uccad \uc591\uc2dd",
    "\ud45c\ucc3d \uc2ec\uc0ac\uae30\uc900\ud45c",
    "\ube44 \uace0 (\ucc9c\uc6d0) \uacc4",
    "\ube44\uace0 (\ucc9c\uc6d0) \uacc4",
    "\ubc0f 1.",
    "\ubc0f 1",
    "\uc815\ubd80\uc9c0\uc6d0\uae08 \uaddc\ubaa8 \ubd84\ub2f4\uae08",
    "\uc218\ud589\uae30\uac04 \uc8fc\uad00\ubd80\ucc98(\uae30\uad00)",
    "\ucc38\uc5ec\uae30\uac04 \uc8fc\uad00\ubd80\ucc98(\uae30\uad00)",
    "수량, 규격 기재",
    "수량 규격 기재",
    "과제 제안요구서(기획의도)",
    "정부지원",
    "정부 지원",
    "\uc0ac\uc5c5\ub0b4\uc6a9",
    "\ud655\uc778 \ud6c4 \uc2e0\uccad",
    "\uc18c\uad00 \ubd80\ucc98",
    "\uc81c\ud488\uc548\uc804\uad00\ub9ac\uae30\uc220\uac1c\ubc1c\ubc0f\uc2e4\uc99d\uc0ac\uc5c5",
    "1 \uad6d\uc870\uc2e4 \uc0dd\ud65cSOC \ubcf5\ud569\ud654 \uc0ac\uc5c5",
}
PROJECT_NAME_GUIDANCE_RE = re.compile(r"\uc791\uc131\s*\uc2dc.*\uacfc\uc81c\uba85\s*\uc791\uc131")
PROJECT_NAME_CONTACT_OR_META_RE = re.compile(
    r"(?:"
    r"@|"
    r"\d{2,4}-\d{3,4}-\d{4}|"
    r"\ubb38\uc11c\ubc88\ud638|"
    r"\ubc1c\uae09\uc77c|"
    r"\uc2e0\uc6a9\uc815\ubcf4|"
    r"\uccb4\ub0a9\uc815\ubcf4|"
    r"\uc0ac\ud68c\ubcf4\ud5d8\ub8cc|"
    r"\uacfc\uc81c\ub2f4\ub2f9\uad00|"
    r"\ub2f4\ub2f9\ubd80\uc11c|"
    r"\ub2f4\ub2f9\uc790|"
    r"\uc5f0\ub77d\ucc98|"
    r"\uc804\ud654\ubc88\ud638|"
    r"\uc774\uba54\uc77c|"
    r"e-?mail"
    r")",
    flags=re.IGNORECASE,
)
PROJECT_NAME_TABLE_HEADER_MARKERS = (
    "\uae30\uad00\uba85",
    "\ubd80\uc11c",
    "\uc9c0\uc6d0\uae30\uad00\uba85",
    "\uc9c0\uc6d0\uae30\uac04",
    "\uc9c0\uc6d0\uae08\uc561",
    "\uc9c0\uc6d0\uae08",
    "\uc9c0\uc6d0\uc608\uc0b0",
    "\ucd1d \uc815\ubd80\uc9c0\uc6d0\uc5f0\uad6c\uac1c\ubc1c\ube44",
    "\uacf5\uace0\uae30\uac04",
    "\uc5f0\uad6c\ucc45\uc784\uc790",
    "\uc628\ub77c\uc778",
    "\uc2e0\uccad\uae30\uac04",
    "\uc2b9\uc778\uae30\uac04",
    "\uc5f0\uad6c\uac1c\ubc1c\uae30\uad00",
    "\uc811\uc218\uae30\uac04",
    "\uc811\uc218\ubc29\ubc95",
    "\uc81c\ucd9c\uc11c\ub958",
    "\uacfc\uc81c\uc218",
    "\uacfc\uc81c\uc720\ud615",
    "\uacfc\uc81c\uba85",
    "RFP\uba85",
    "\uacf5\uace0\ub2e8\uc704",
    "\uc0ac\uc5c5\uba85",
    "\ud488\ubaa9\uba85",
)
PROJECT_NAME_SECTION_ONLY_RE = re.compile(
    r"^(?:"
    r"(?:\d+\s*[.)]\s*|[\u3131-\u318e\uac00-\ud7a3A-Za-z]\.\s*)?"
    r"(?:"
    r"\uc5f0\uad6c\uc758\s*(?:\uac1c\uc694|\uc815\uc758)|"
    r"\uc5f0\uad6c(?:\uae30\ud68d)?\s*(?:\ubaa9\ud45c|\ud544\uc694\uc131|\ub0b4\uc6a9|\ubc94\uc704)|"
    r"\uac1c\ubc1c\s*(?:\ubaa9\ud45c|\ub0b4\uc6a9)|"
    r"\uac1c\ub150(?:\s*\ubc0f\s*\uac1c\ubc1c\ub0b4\uc6a9)?|"
    r"\uc9c0\uc6d0\s*(?:\ub0b4\uc6a9|\ud544\uc694\uc131)|"
    r"\ud65c\uc6a9\s*\ubd84\uc57c|"
    r"\uc9c0\uc6d0\uae30\uac04(?:\s*/\s*\uc608\uc0b0(?:\s*/\s*\ucd94\uc9c4\uccb4\uacc4)?)?|"
    r"\ucd94\uc9c4\uccb4\uacc4|"
    r"\ucd5c\uc885\ubaa9\ud45c|"
    r"\uc5f0\uad6c\ud544\uc694\uc131|"
    r"\ud2b9\uae30\uc0ac\ud56d|"
    r"\uae30\ud0c0"
    r")"
    r")$",
    flags=re.IGNORECASE,
)
PROJECT_NAME_TRAILING_SECTION_PATTERNS = (
    re.compile(
        r"\s+(?:\d+\s*[.)]\s*|[\u3131-\u318e\uac00-\ud7a3A-Za-z]\.\s*)?"
        r"(?:"
        r"\uc5f0\uad6c\uc758\s*(?:\uac1c\uc694|\uc815\uc758)|"
        r"\uc5f0\uad6c(?:\uae30\ud68d)?\s*(?:\ubaa9\ud45c|\ud544\uc694\uc131|\ub0b4\uc6a9|\ubc94\uc704)|"
        r"\uac1c\ubc1c\s*(?:\ubaa9\ud45c|\ub0b4\uc6a9)|"
        r"\uac1c\ub150(?:\s*\ubc0f\s*\uac1c\ubc1c\ub0b4\uc6a9)?|"
        r"\uc9c0\uc6d0\s*(?:\ub0b4\uc6a9|\ud544\uc694\uc131)|"
        r"\ud65c\uc6a9\s*\ubd84\uc57c|"
        r"\uc9c0\uc6d0\uae30\uac04(?:\s*/\s*\uc608\uc0b0(?:\s*/\s*\ucd94\uc9c4\uccb4\uacc4)?)?|"
        r"\ucd94\uc9c4\uccb4\uacc4|"
        r"\ud2b9\uae30\uc0ac\ud56d|"
        r"\uae30\ud0c0"
        r").*$",
        flags=re.IGNORECASE,
    ),
    re.compile(
        r"\s+(?:\d+\s*[.)]\s*|[\u3131-\u318e\uac00-\ud7a3A-Za-z]\.\s*)?"
        r"(?:"
        r"\uc5f0\uad6c\uc758\s*(?:\uac1c\uc694|\uc815\uc758)|"
        r"\uc5f0\uad6c(?:\uae30\ud68d)?\s*(?:\ubaa9\ud45c|\ud544\uc694\uc131|\ub0b4\uc6a9|\ubc94\uc704)|"
        r"\uac1c\ubc1c\s*(?:\ubaa9\ud45c|\ub0b4\uc6a9)|"
        r"\uac1c\ub150(?:\s*\ubc0f\s*\uac1c\ubc1c\ub0b4\uc6a9)?|"
        r"\uc9c0\uc6d0\s*(?:\ub0b4\uc6a9|\ud544\uc694\uc131)|"
        r"\ud65c\uc6a9\s*\ubd84\uc57c|"
        r"\uc9c0\uc6d0\uae30\uac04(?:\s*/\s*\uc608\uc0b0(?:\s*/\s*\ucd94\uc9c4\uccb4\uacc4)?)?|"
        r"\ucd5c\uc885\ubaa9\ud45c|"
        r"\uc5f0\uad6c\ud544\uc694\uc131|"
        r"\ud2b9\uae30\uc0ac\ud56d|"
        r"\uae30\ud0c0"
        r").*$",
        flags=re.IGNORECASE,
    ),
    re.compile(
        r"\s+(?:\uacfc\uc81c\ub2f4\ub2f9\uad00|\ub2f4\ub2f9\ubd80\uc11c|\ubb38\uc758\ucc98|\ub2f4\ub2f9\uc790|\uc5f0\ub77d\ucc98)\b.*$",
        flags=re.IGNORECASE,
    ),
    re.compile(r"\s*[·•⋯…]{2,}\s*\d+\s*$"),
    re.compile(r"\s*[.]{3,}\s*\d+\s*$"),
)
PROJECT_NAME_LOW_VALUE_MARKERS = (
    "\uc18c\uad00 \ubd80\ucc98",
    "\uc791\uc131\uc6d0\uce59",
    "\uac80\uc0c9\ud558\uc5ec \uc2e0\uccad",
    "\uac80\uc0c9\ud558\uc5ec\uc2e0\uccad",
    "\uba85\uc2dc\ud558\uace0",
    "\uc138\ubd80 \uc9c0\uc6d0\ub0b4\uc6a9",
    "\uc138\ubd80\uc9c0\uc6d0\ub0b4\uc6a9",
    "smart-factory.kr",
    "\uc81c\uc7ac\ud604\ud669\ubaa9\ub85d",
    "\uc9c0\uc6d0\uacb0\uacfc\uc5d0 \ub300\ud55c \ud3c9\uac00",
    "\ub9e4\uc6b0 \ub9cc\uc871\ud55c\ub2e4",
    "\ub9e4\uc6b0 \ubd88\ub9cc\uc871\uc774\ub2e4",
    "\uccb4\ub0a9\uc815\ubcf4",
    "\uc2e0\uc6a9\uc815\ubcf4",
    "\uc0ac\ud68c\ubcf4\ud5d8\ub8cc",
    "\uc0ac\uc5c5\uacc4\ud68d\uc11c",
)
PROJECT_NAME_LONG_FORM_MARKERS = (
    "\uc785\ub825",
    "\uae30\uc7ac",
    "\uc81c\ucd9c",
    "\uc2e0\uccad",
    "\ud655\uc778",
    "\ud3c9\uac00",
    "\uc99d\ube59",
    "\ud611\uc57d",
    "\uae30\uc900",
    "\uc0ac\uc6a9(\uacc4\ud68d)",
    "\uad6d\uac00\uc5f0\uad6c\uac1c\ubc1c\uacfc\uc81c",
)
PROJECT_NAME_DOMAIN_RE = re.compile(r"(?:https?://|www\.|[A-Za-z0-9-]+\.(?:kr|com|net|org))", flags=re.IGNORECASE)
PROJECT_NAME_RESPONSE_SCALE_RE = re.compile(
    r"(?:\u2460|\u2461|\u2462|\u2463|\u2464).{0,30}(?:\ub9cc\uc871|\ubd88\ub9cc\uc871)",
    flags=re.IGNORECASE,
)
PROJECT_NAME_DOCUMENT_STYLE_RE = re.compile(
    r"(?:\uc2e0\uccad\uc11c|\uc591\uc2dd|\uc2ec\uc0ac\uae30\uc900\ud45c|\uacfc\uc5c5\uc9c0\uc2dc\uc11c|(?:\uc0ac\uc5c5|지원)\s*\uac1c\uc694)\s*$",
    flags=re.IGNORECASE,
)
PROJECT_NAME_PORTAL_ACTION_RE = re.compile(
    r"(?:후\s*신청\s*접수\s*(?:[・·ㆍ·/,&]|및)\s*완료)|"
    r"(?:\uc9c0\uc6d0\uae08\s*\uc785\ub825|\uc811\uc218\ud30c\uc77c\s*\uc5c5\ub85c\ub4dc|\uc81c\ucd9c\ud558\uae30|\uc628\ub77c\uc778\s*\uc0ac\uc5c5\uc2e0\uccad|\ucc38\uc5ec\uc2e0\uccad|\ud655\uc778\s*\ud6c4\s*\uc2e0\uccad|\ud074\ub9ad\s*\ud6c4)",
    flags=re.IGNORECASE,
)
PROJECT_NAME_SHORT_FRAGMENT_RE = re.compile(
    r"^(?:\ubc0f(?:\s*\d+[.]?)?|\uacc4)$",
    flags=re.IGNORECASE,
)
PROJECT_NAME_GENERIC_FRAGMENT_RE = re.compile(
    r"^(?:구축\s*(?:[·ㆍ·/,&]|및)\s*운영|체계\s*기획|"
    r"지원\s*목적\s*[,·ㆍ/|]\s*기술\s*범위|"
    r"중점\s*기술\s*분야\s*\*?\s*과제\s*유형\s*공모\s*유형)$"
)
PROJECT_NAME_TABLE_HEADER_RE = re.compile(
    r"(?:^과제\s*수\s*(?:\(\s*개\s*\))?\s*(?:['‘’ʼ]?\s*(?:20)?\d{2}\s*년\s*)?연구비)|"
    r"(?:\uc815\ubd80\uc9c0\uc6d0\uae08\s*\uaddc\ubaa8\s*\ubd84\ub2f4\uae08|\uc218\ud589\uae30\uac04\s*\uc8fc\uad00\ubd80\ucc98|\ucc38\uc5ec\uae30\uac04\s*\uc8fc\uad00\ubd80\ucc98|\uc18c\uad00\s*\ubd80\ucc98|(?:\ubc0f\s*)?\uae30\uc5c5\uba85\s*\ubc18\ub4dc\uc2dc\s*\ud45c\uc2dc|수량\s*,?\s*규격\s*기재|품목[··ㆍ,]\s*규격[··ㆍ,]\s*수량)",
    flags=re.IGNORECASE,
)
# Full field lists from application/consulting forms, not technical task titles.
# Anchor the whole value so e.g. "사업자등록번호 검증 시스템 개발" stays valid.
PROJECT_NAME_FORM_FIELDS_RE = re.compile(
    r"^(?:(?:사업자\s*등록\s*번호|사업장\s*주소|회차|수행\s*(?:날짜|일자)|컨설팅\s*주제)"
    r"[\s,·ㆍ/|:：]*)+(?:등)?$"
)
PROJECT_NAME_INSTRUCTION_RE = re.compile(
    r"(?:\uc791\uc131\uc6d0\uce59\s*:|\u25a1?\s*\uc0ac\uc5c5\ub0b4\uc6a9\s*:|\"\uae30\uc220\s*\uc6b0\uc218\s*\uc81c\ud488\"\uc73c\ub85c\s*\uba85\uc2dc|\u25a1?\s*\uc5c6\uc74c$|기재\s*$|작성\s*$)",
    flags=re.IGNORECASE,
)


def _contains_three_or_more_markers(text: str, markers: tuple[str, ...]) -> bool:
    return sum(1 for marker in markers if marker in text) >= 3


def strip_project_name_heading_artifacts(value: object) -> str:
    text = normalize_space(value)
    if not text:
        return ""
    trimmed = text
    for pattern in PROJECT_NAME_TRAILING_SECTION_PATTERNS:
        trimmed = pattern.sub("", trimmed)
    return normalize_space(trimmed).strip(" -_:;/,")


def looks_low_value_project_name(value: object) -> bool:
    text = normalize_space(value)
    comparison_text = text.replace("_", " ")
    if "중점기술분야" in comparison_text and any(
        marker in comparison_text for marker in ("과제유형", "공모유형")
    ):
        return True
    if "해당여부" in comparison_text and any(mark in comparison_text for mark in ("□", "☑", "■")):
        return True
    if sum(comparison_text.count(mark) for mark in ("□", "☑", "■")) >= 2:
        return True
    if PROJECT_NAME_GENERIC_FRAGMENT_RE.fullmatch(comparison_text):
        return True
    if text in PROJECT_NAME_PLACEHOLDERS:
        return True
    if len(text) < 3:
        return True
    if text in PROJECT_NAME_EXACT_LOW_VALUE_KEYS or comparison_text in PROJECT_NAME_EXACT_LOW_VALUE_KEYS:
        return True
    if PROJECT_NAME_GUIDANCE_RE.search(text):
        return True
    if PROJECT_NAME_CONTACT_OR_META_RE.search(text):
        return True
    if PROJECT_NAME_SECTION_ONLY_RE.fullmatch(text):
        return True
    if len(text) <= 20 and any(marker in text for marker in ("\uc5f0\uad6c\uae30\uac04", "\uc9c0\uc6d0\uae30\uac04", "\ucd1d \uc5f0\uad6c\uae30\uac04")):
        return True
    if _contains_three_or_more_markers(text, PROJECT_NAME_TABLE_HEADER_MARKERS):
        return True
    if sum(1 for marker in PROJECT_NAME_TABLE_HEADER_MARKERS if marker in text) >= 2 and len(text) <= 40:
        return True
    if any(marker in comparison_text.lower() for marker in PROJECT_NAME_LOW_VALUE_MARKERS):
        return True
    if PROJECT_NAME_DOMAIN_RE.search(text):
        return True
    if PROJECT_NAME_RESPONSE_SCALE_RE.search(text):
        return True
    if PROJECT_NAME_DOCUMENT_STYLE_RE.search(text):
        return True
    if PROJECT_NAME_PORTAL_ACTION_RE.search(text):
        return True
    if PROJECT_NAME_SHORT_FRAGMENT_RE.fullmatch(text) and len(text) <= 6:
        return True
    if PROJECT_NAME_TABLE_HEADER_RE.search(text):
        return True
    if PROJECT_NAME_FORM_FIELDS_RE.fullmatch(comparison_text):
        return True
    if PROJECT_NAME_INSTRUCTION_RE.search(text):
        return True
    if len(text) >= 80 and sum(1 for marker in PROJECT_NAME_LONG_FORM_MARKERS if marker in comparison_text) >= 2:
        return True
    if re.fullmatch(r"[\d\W_]+", text):
        return True
    return False


def sanitize_project_period(value: object) -> str:
    text = normalize_space(value)
    if not text or text in PERIOD_PLACEHOLDERS:
        return ""

    for pattern in PERIOD_PATTERNS:
        match = re.search(pattern, text)
        if match:
            return normalize_space(match.group(0))

    normalized = re.sub(r"[^\w\u3131-\u318e\uac00-\ud7a3)~./ \-]+", " ", text)
    tokens = [token for token in normalized.split() if token]
    if tokens and len(tokens) <= 4 and all(token in PERIOD_BAD_TOKEN_KEYS for token in tokens):
        return ""
    if sum(1 for marker in PERIOD_TABLE_HEADER_MARKERS if marker in text) >= 2:
        return ""
    if any(marker in text for marker in PERIOD_NARRATIVE_MARKERS):
        return ""
    if re.search(r"[.!?]|(?:\u203b)|(?:\u2030)", text):
        return ""
    if len(text) > 60:
        return ""
    if not re.search(r"(\uae30\uac04|\uac1c\uc6d4|\ub144|~|-)", text):
        return ""
    return text


def _candidate_budget_chunks(text: str) -> list[str]:
    chunks = re.split(r"\n+|[|]|\u203b", text)
    return [normalize_space(chunk) for chunk in chunks if normalize_space(chunk)]


def sanitize_budget_text(value: object) -> str:
    text = normalize_space(clean(value).replace("\r", "\n"))
    if not text or text in BUDGET_PLACEHOLDERS:
        return ""

    for chunk in _candidate_budget_chunks(text):
        label_match = BUDGET_LABEL_RE.search(chunk)
        if label_match:
            candidate = normalize_space(label_match.group(0))
            if not any(marker in candidate for marker in BUDGET_REJECT_MARKERS):
                return candidate
        if len(chunk) <= 30 and BUDGET_AMOUNT_RE.fullmatch(chunk):
            return chunk

    if any(marker in text for marker in BUDGET_REJECT_MARKERS):
        return ""
    if len(text) > 90:
        return ""

    label_match = BUDGET_LABEL_RE.search(text)
    if label_match:
        return normalize_space(label_match.group(0))
    if BUDGET_AMOUNT_RE.fullmatch(text):
        return text
    return ""
