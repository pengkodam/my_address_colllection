"""Shared Malaysian address parsing and normalisation helpers."""

from __future__ import annotations

import re
from difflib import SequenceMatcher

# Normalise the abbreviations that appear in Malaysian addresses. Only map
# tokens where the expansion is unambiguous.
ABBREV = {
    "jln": "jalan",
    "lrg": "lorong",
    "kg": "kampung",
    "bt": "batu",
    "tmn": "taman",
    "pst": "persiaran",
    "lbh": "lebuh",
    "bdr": "bandar",
    "wly": "wilayah",
}

STREET_RE = re.compile(
    r"\b(jalan|jln|lorong|lrg|persiaran|lebuh)\b"
    r"|\boff\s+(jalan|jln)\b",
    re.IGNORECASE,
)
AREA_RE = re.compile(
    r"\b(taman|bandar|kampung|kg|batu|seksyen|section)\b",
    re.IGNORECASE,
)
LEADING_NUMBER_RE = re.compile(
    r"^\s*(?:no\.?\s*)?(\d+[A-Za-z]?(?:[-/]\d+[A-Za-z]?)?)\b",
    re.IGNORECASE,
)
LEADING_JUNK_RE = re.compile(
    r"^\s*(?:lot|pt|no|unit|blok|block|tingkat|level|floor|seksyen|section)"
    r"\s*[\dA-Za-z/-]*\s*",
    re.IGNORECASE,
)
MILE_MARKER_RE = re.compile(
    r"^\s*(?:batu|bt|km)\s*[\d/.]+\s*(?=(?:jalan|jln)\b)",
    re.IGNORECASE,
)
TRAILING_STATE_RE = re.compile(
    r"\b(?:kelantan|terengganu|selangor|johor|kedah|perak|pahang|sabah|"
    r"sarawak|melaka|negeri sembilan|perlis|pulau pinang|penang|"
    r"kuala lumpur|labuan|putrajaya)\s*$",
    re.IGNORECASE,
)
SPLIT_RE = re.compile(r"[,;，；、]")


def normalize_text(value: str | None) -> str:
    """Lowercase, strip punctuation and collapse whitespace."""
    text = str(value or "").lower()
    text = re.sub(r"[^a-z0-9]+", " ", text)
    return re.sub(r"\s+", " ", text).strip()


def normalize_street(value: str | None) -> str:
    """Lowercase, strip punctuation, expand common Malaysian abbreviations."""
    if value is None:
        return ""
    text = str(value).lower()
    text = re.sub(r"[^a-z0-9]+", " ", text)
    tokens = [ABBREV.get(t, t) for t in text.split()]
    return " ".join(t for t in tokens if t)


def clean_street_candidate(part: str) -> str:
    """Remove lot/unit prefixes, mile markers, postcode and trailing state."""
    text = MILE_MARKER_RE.sub("", part)
    previous = None
    while previous != text:
        previous = text
        text = LEADING_JUNK_RE.sub("", text)
        text = LEADING_NUMBER_RE.sub("", text)
    text = re.sub(r"\b\d{5}\b", " ", text)
    text = TRAILING_STATE_RE.sub("", text)
    return text.strip()


def address_type(address: str | None) -> str:
    if STREET_RE.search(address or ""):
        return "street"
    if AREA_RE.search(address or ""):
        return "area"
    return "other"


def parse_candidates(address: str | None) -> list[str]:
    """Return normalised street candidates from a freeform address."""
    parts = [p.strip() for p in SPLIT_RE.split(address or "") if p.strip()]
    candidates: list[str] = []
    for part in parts:
        if not STREET_RE.search(part):
            continue
        norm = normalize_street(clean_street_candidate(part))
        if norm:
            candidates.append(norm)
    if not candidates and address_type(address) != "area":
        norm = normalize_street(clean_street_candidate(address or ""))
        if norm:
            candidates.append(norm)
    return list(dict.fromkeys(candidates))


def parse_housenumber(address: str | None) -> str | None:
    first = SPLIT_RE.split(address or "")[0].strip()
    match = LEADING_NUMBER_RE.match(first)
    if not match:
        return None
    return re.sub(r"[^a-z0-9]", "", match.group(1).lower())


def normalize_housenumber(value: str | None) -> str:
    return re.sub(r"[^a-z0-9]", "", (value or "").lower())


def numeric_tokens(value: str | None) -> list[str]:
    return re.findall(r"\d+", value or "")


def compact_street(value: str | None) -> str:
    return re.sub(r"[^a-z0-9]+", "", (value or "").lower())


def street_match_level(a: str | None, b: str | None) -> str:
    """Classify two normalised street names.

    Returns `exact`, `format` (punctuation/spacing only), `suffix` (one name is
    a token-prefix of the other), `typo` (one changed token that is close) or
    `none`. Numeric suffix changes are deliberately not typos.
    """
    a = (a or "").strip()
    b = (b or "").strip()
    if not a or not b:
        return "none"
    if a == b:
        return "exact"
    if compact_street(a) == compact_street(b):
        return "format"
    ta, tb = a.split(), b.split()
    if ta == tb[: len(ta)] or tb == ta[: len(tb)]:
        return "suffix"
    if len(ta) == len(tb):
        diffs = [(x, y) for x, y in zip(ta, tb) if x != y]
        if len(diffs) == 1:
            x, y = diffs[0]
            if SequenceMatcher(None, x, y).ratio() >= 0.8:
                return "typo"
    return "none"


def numeric_compatible(a: str | None, b: str | None) -> bool:
    """True when two street names differ only outside their numeric parts.

    `Jalan Batu Nilam 34` and `Jalan Batu Nilam 4` are different streets, so
    they are not compatible. A missing suffix (`Jalan Kuchai Maju` vs
    `Jalan Kuchai Maju 8`) or formatting (`SS3/35` vs `SS 3/35`) is compatible.
    """
    ta = (a or "").split()
    tb = (b or "").split()
    if ta == tb:
        return True
    if ta == tb[: len(ta)] or tb == ta[: len(tb)]:
        return True
    return numeric_tokens(a) == numeric_tokens(b)
