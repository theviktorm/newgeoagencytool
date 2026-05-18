"""
Momentus AI — Brand Canonicalization + Language Detection.

Dedupes near-identical brand strings ("Pasarét Klinika" / "Pasaret Klinika" /
"pasarét  klinika") into one canonical row, and detects prompt language so
the per-locale keyword classifier can replace prompt_engine's EN-only rules.
Stdlib only; same `.database` pattern as the rest of the backend.
"""
from __future__ import annotations

import logging
import re
import unicodedata
from typing import Any, Dict, List, Tuple

from .database import execute, fetch_all, fetch_one, gen_id, get_db

logger = logging.getLogger("geo.brand_resolver")


_SCHEMA = """
CREATE TABLE IF NOT EXISTS brands (
    id              TEXT PRIMARY KEY,
    workspace_id    TEXT NOT NULL,
    canonical_name  TEXT NOT NULL,
    display_name    TEXT DEFAULT '',
    domain          TEXT DEFAULT '',
    is_us           INTEGER DEFAULT 0,
    created_at      TEXT DEFAULT (datetime('now'))
);
CREATE INDEX IF NOT EXISTS idx_brands_workspace ON brands(workspace_id);
CREATE INDEX IF NOT EXISTS idx_brands_us ON brands(workspace_id, is_us);

CREATE TABLE IF NOT EXISTS brand_aliases (
    canonical_id     TEXT NOT NULL,
    workspace_id     TEXT NOT NULL,
    alias            TEXT NOT NULL,
    normalized_alias TEXT NOT NULL,
    source           TEXT DEFAULT 'auto',
    confidence       REAL DEFAULT 1.0,
    created_at       TEXT DEFAULT (datetime('now')),
    PRIMARY KEY (workspace_id, normalized_alias)
);
CREATE INDEX IF NOT EXISTS idx_aliases_canonical ON brand_aliases(canonical_id);
"""


async def init() -> None:
    """Create brands + brand_aliases tables. Idempotent."""
    db = await get_db()
    await db.executescript(_SCHEMA)
    await db.commit()


# --- normalization + fuzzy matching ---

_PUNCT_RE = re.compile(r"[^\w\s]+", re.UNICODE)
_WS_RE = re.compile(r"\s+")


def normalize(name: str) -> str:
    """Lowercase, NFKD-strip accents, drop punctuation, collapse whitespace.
    "Pasarét Klinika!" -> "pasaret klinika"."""
    if not name:
        return ""
    s = unicodedata.normalize("NFKD", str(name))
    s = "".join(c for c in s if not unicodedata.combining(c))
    s = s.lower()
    s = _PUNCT_RE.sub(" ", s)
    s = _WS_RE.sub(" ", s).strip()
    return s


def levenshtein(a: str, b: str) -> int:
    """Iterative DP edit distance. O(len(a)*len(b)) time, O(min) space."""
    if a == b:
        return 0
    if not a:
        return len(b)
    if not b:
        return len(a)
    if len(a) < len(b):
        a, b = b, a
    prev = list(range(len(b) + 1))
    for i, ca in enumerate(a, 1):
        curr = [i] + [0] * len(b)
        for j, cb in enumerate(b, 1):
            cost = 0 if ca == cb else 1
            curr[j] = min(curr[j - 1] + 1, prev[j] + 1, prev[j - 1] + cost)
        prev = curr
    return prev[-1]


# --- canonicalization ---

async def canonicalize(workspace_id: str, raw_name: str) -> Dict[str, Any]:
    """Resolve raw brand string to canonical row.
    (a) exact normalized hit, (b) fuzzy Levenshtein<=2, (c) create new."""
    raw = (raw_name or "").strip()
    if not raw:
        raise ValueError("raw_name required")
    norm = normalize(raw)
    if not norm:
        raise ValueError("raw_name normalizes to empty")

    # (a) exact normalized hit
    hit = await fetch_one(
        "SELECT b.* FROM brand_aliases a JOIN brands b ON a.canonical_id = b.id "
        "WHERE a.workspace_id = ? AND a.normalized_alias = ?",
        (workspace_id, norm),
    )
    if hit:
        return hit

    # (b) fuzzy match against existing brands' normalized names
    candidates = await fetch_all(
        "SELECT id, canonical_name FROM brands WHERE workspace_id = ?",
        (workspace_id,),
    )
    best_id, best_dist = None, 99
    threshold = 2 if len(norm) >= 5 else 1  # avoid collapsing very short names
    for c in candidates:
        d = levenshtein(norm, normalize(c["canonical_name"]))
        if d < best_dist:
            best_dist, best_id = d, c["id"]
    if best_id and best_dist <= threshold:
        await _insert_alias(workspace_id, best_id, raw, norm, "fuzzy", 1.0 - best_dist * 0.1)
        return await fetch_one("SELECT * FROM brands WHERE id = ?", (best_id,))

    # (c) brand new
    bid = gen_id("br-")
    await execute(
        "INSERT INTO brands (id, workspace_id, canonical_name, display_name) "
        "VALUES (?, ?, ?, ?)",
        (bid, workspace_id, norm, raw),
    )
    await _insert_alias(workspace_id, bid, raw, norm, "seed", 1.0)
    return await fetch_one("SELECT * FROM brands WHERE id = ?", (bid,))


async def _insert_alias(
    workspace_id: str, canonical_id: str, alias: str,
    normalized: str, source: str, confidence: float,
) -> None:
    await execute(
        "INSERT OR IGNORE INTO brand_aliases "
        "(canonical_id, workspace_id, alias, normalized_alias, source, confidence) "
        "VALUES (?, ?, ?, ?, ?, ?)",
        (canonical_id, workspace_id, alias, normalized, source, confidence),
    )


async def list_brands(workspace_id: str) -> List[Dict[str, Any]]:
    """All brands in a workspace with their aliases attached."""
    brands = await fetch_all(
        "SELECT * FROM brands WHERE workspace_id = ? ORDER BY canonical_name", (workspace_id,),
    )
    if not brands:
        return []
    aliases = await fetch_all("SELECT * FROM brand_aliases WHERE workspace_id = ?", (workspace_id,))
    by_canon: Dict[str, List[Dict[str, Any]]] = {}
    for a in aliases:
        by_canon.setdefault(a["canonical_id"], []).append(a)
    for b in brands:
        b["aliases"] = by_canon.get(b["id"], [])
    return brands


async def merge_brands(workspace_id: str, keep_id: str, merge_id: str) -> Dict[str, Any]:
    """Point merge_id's aliases at keep_id, delete merge_id row."""
    if keep_id == merge_id:
        return {"merged": 0, "kept": keep_id}
    keep = await fetch_one("SELECT * FROM brands WHERE id = ? AND workspace_id = ?", (keep_id, workspace_id))
    drop = await fetch_one("SELECT * FROM brands WHERE id = ? AND workspace_id = ?", (merge_id, workspace_id))
    if not keep or not drop:
        raise ValueError("both brand ids must exist in this workspace")
    moved = await execute(
        "UPDATE brand_aliases SET canonical_id = ? WHERE canonical_id = ? AND workspace_id = ?",
        (keep_id, merge_id, workspace_id),
    )
    await execute("DELETE FROM brands WHERE id = ?", (merge_id,))
    logger.info("merged brand %s into %s (%d aliases moved)", merge_id, keep_id, moved)
    return {"merged": moved, "kept": keep_id, "dropped": merge_id}


async def set_canonical_for_us(workspace_id: str, name: str, domain: str = "") -> Dict[str, Any]:
    """Resolve `name` to a brand and mark it is_us=1 (clearing the flag on siblings)."""
    brand = await canonicalize(workspace_id, name)
    await execute("UPDATE brands SET is_us = 0 WHERE workspace_id = ? AND id != ?", (workspace_id, brand["id"]))
    await execute(
        "UPDATE brands SET is_us = 1, domain = COALESCE(NULLIF(?,''), domain) WHERE id = ?",
        (domain, brand["id"]),
    )
    return await fetch_one("SELECT * FROM brands WHERE id = ?", (brand["id"],))


# --- language detection ---
# Distinctive diacritics per language (most-unique first wins the lookup).
_DIACRITIC_HINTS: List[Tuple[str, str]] = [
    ("hu", "őűŐŰ"),
    ("pl", "łąęźżćńŁĄĘŹŻĆŃ"),
    ("cs", "řěůŘĚŮ"),
    ("sk", "ľĺŕĽĹŔ"),
    ("ro", "țșțȘŢŞ"),
    ("pt", "ãõÃÕ"),
    ("de", "ßÄÖÜäöü"),
    ("es", "ñ¿¡Ñ"),
    ("fr", "œŒæÆçÇ"),
    ("it", "àèìòùÀÈÌÒÙ"),
]

# Common stopwords; tiny sets, high signal. Looked up as whitespace-padded
# substrings against the lowercased text.
_STOPWORDS: Dict[str, Tuple[str, ...]] = {
    "hu": ("a ", "az ", "és ", "vagy ", "nem ", "van ", "egy ", "hogy ", "ez ", "mi "),
    "de": ("der ", "die ", "das ", "und ", "ist ", "nicht ", "ein ", "mit ", "von ", "für "),
    "es": ("el ", "la ", "los ", "las ", "que ", "para ", "con ", "por ", "una ", "del "),
    "fr": ("le ", "la ", "les ", "des ", "une ", "que ", "pour ", "avec ", "dans ", "est "),
    "it": ("il ", "la ", "gli ", "una ", "che ", "per ", "con ", "sono ", "del ", "alla "),
    "pl": ("jest ", "nie ", "się ", "tak ", "czy ", "jak ", "oraz ", "tylko ", "który "),
    "cs": ("je ", "není ", "jak ", "nebo ", "také ", "který ", "ale ", "pro "),
    "sk": ("je ", "nie ", "alebo ", "ako ", "ktorý ", "pre ", "tiež "),
    "ro": ("este ", "nu ", "sau ", "cum ", "pentru ", "care ", "din "),
    "pt": ("o ", "a ", "os ", "as ", "que ", "para ", "com ", "uma ", "não "),
    "en": ("the ", "and ", "is ", "of ", "to ", "for ", "with ", "what ", "how "),
}


def _has_chars(text: str, chars: str) -> bool:
    return any(c in text for c in chars)


def has_hungarian(text: str) -> bool:
    return _has_chars(text, "őűŐŰ") or _stopword_hits(text, "hu") >= 1


def has_german(text: str) -> bool:
    return _has_chars(text, "ßÄÖÜäöü") or _stopword_hits(text, "de") >= 1


def has_spanish(text: str) -> bool:
    return _has_chars(text, "ñ¿¡Ñ") or _stopword_hits(text, "es") >= 1


def has_french(text: str) -> bool:
    return _has_chars(text, "œŒæÆ") or _stopword_hits(text, "fr") >= 2


def _stopword_hits(text: str, lang: str) -> int:
    t = f" {text.lower()} "
    return sum(1 for w in _STOPWORDS.get(lang, ()) if f" {w}" in t)


def detect_language(text: str) -> str:
    """Best-effort 2-letter code; defaults to 'en'.
    Distinctive diacritics first, then stopword voting. Tuned for short prompts."""
    if not text:
        return "en"
    # Distinctive diacritics — checked in priority order (most unique first).
    for code, chars in _DIACRITIC_HINTS:
        if _has_chars(text, chars):
            return code
    # Stopword voting
    best_lang, best_hits = "en", 0
    for lang in _STOPWORDS:
        h = _stopword_hits(text, lang)
        if h > best_hits:
            best_hits, best_lang = h, lang
    return best_lang if best_hits >= 1 else "en"


# --- localized keyword rules ---
# Tuple shape mirrors prompt_engine._KEYWORD_RULES:
#   (matchers, prompt_type, buyer_stage, base_revenue)
# Implemented: en/hu/de/es. fr/it/pl/cs/sk/ro/pt fall back to en (TODO: localize).

_RULES_EN: List[Tuple[Tuple[str, ...], str, str, int]] = [
    (("best", "top", "near me", "nearest"), "decision", "trust", 75),
    (("vs", "versus", "compare", "comparison", "or"), "comparative", "comparison", 65),
    (("review", "rating", "experience", "reddit"), "comparative", "trust", 55),
    (("price", "cost", "how much", "buy", "book"), "purchase", "decision", 85),
    (("how to choose", "which", "should i", "is it worth"), "decision", "decision", 70),
    (("what is", "symptoms", "causes", "definition"), "informational", "awareness", 15),
    (("recovery", "side effect", "risk", "complication"), "informational", "objection", 40),
    (("alternative", "without", "instead of"), "comparative", "solution", 50),
]

_RULES_HU: List[Tuple[Tuple[str, ...], str, str, int]] = [
    (("legjobb", "top", "közelben", "legközelebbi"), "decision", "trust", 75),
    (("vs", "versus", "vagy", "összehasonlítás", "hasonlít"), "comparative", "comparison", 65),
    (("értékelés", "vélemény", "tapasztalat", "review"), "comparative", "trust", 55),
    (("ár", "költség", "mennyibe kerül", "vásárol", "foglal"), "purchase", "decision", 85),
    (("hogyan válasszak", "melyik", "érdemes", "kell-e"), "decision", "decision", 70),
    (("mi az", "jelentése", "tünetei", "okai", "definíció"), "informational", "awareness", 15),
    (("felépülés", "mellékhatás", "kockázat", "szövődmény"), "informational", "objection", 40),
    (("alternatíva", "helyett", "kiváltás"), "comparative", "solution", 50),
]

_RULES_DE: List[Tuple[Tuple[str, ...], str, str, int]] = [
    (("beste", "top", "in der nähe", "nächste"), "decision", "trust", 75),
    (("vs", "versus", "oder", "vergleich", "vergleichen"), "comparative", "comparison", 65),
    (("bewertung", "erfahrung", "rezension", "test"), "comparative", "trust", 55),
    (("preis", "kosten", "wie viel", "kaufen", "buchen"), "purchase", "decision", 85),
    (("wie wähle ich", "welche", "welcher", "lohnt sich"), "decision", "decision", 70),
    (("was ist", "symptome", "ursachen", "definition"), "informational", "awareness", 15),
    (("genesung", "nebenwirkung", "risiko", "komplikation"), "informational", "objection", 40),
    (("alternative", "statt", "anstelle"), "comparative", "solution", 50),
]

_RULES_ES: List[Tuple[Tuple[str, ...], str, str, int]] = [
    (("mejor", "top", "cerca de mí", "más cercano"), "decision", "trust", 75),
    (("vs", "versus", " o ", "comparación", "comparar"), "comparative", "comparison", 65),
    (("opinión", "reseña", "experiencia", "valoración"), "comparative", "trust", 55),
    (("precio", "costo", "cuesta", "cuánto", "comprar", "reservar"), "purchase", "decision", 85),
    (("cómo elegir", "cuál", "vale la pena"), "decision", "decision", 70),
    (("qué es", "síntomas", "causas", "definición"), "informational", "awareness", 15),
    (("recuperación", "efecto secundario", "riesgo", "complicación"), "informational", "objection", 40),
    (("alternativa", "en lugar de", "sin"), "comparative", "solution", 50),
]

LOCALIZED_RULES: Dict[str, List[Tuple[Tuple[str, ...], str, str, int]]] = {
    "en": _RULES_EN,
    "hu": _RULES_HU,
    "de": _RULES_DE,
    "es": _RULES_ES,
    # TODO: localize fr/it/pl/cs/sk/ro/pt. Falling back to EN for now means
    # those languages will under-score until proper rule sets are written.
    "fr": _RULES_EN,
    "it": _RULES_EN,
    "pl": _RULES_EN,
    "cs": _RULES_EN,
    "sk": _RULES_EN,
    "ro": _RULES_EN,
    "pt": _RULES_EN,
}

# Per-language location boosters (+10 revenue when present).
_LOCATION_BOOSTERS: Dict[str, Tuple[str, ...]] = {
    "en": (" budapest", " hungary", "near me", " london", " nyc"),
    "hu": (" budapest", " magyarországon", " magyarország", " közelben"),
    "de": (" berlin", " deutschland", " münchen", " hamburg", " in der nähe"),
    "es": (" madrid", " españa", " barcelona", " cerca"),
    "fr": (" paris", " france", " près de moi"),
    "it": (" roma", " milano", " italia", " vicino"),
    "pl": (" warszawa", " polska", " blisko"),
    "cs": (" praha", " česko"),
    "sk": (" bratislava", " slovensko"),
    "ro": (" bucurești", " românia"),
    "pt": (" lisboa", " portugal", " perto"),
}

_PREMIUM_BOOSTERS: Dict[str, Tuple[str, ...]] = {
    "en": ("private", "luxury", "premium"),
    "hu": ("magán", "prémium", "luxus"),
    "de": ("privat", "luxus", "premium"),
    "es": ("privado", "lujo", "premium"),
}


def keyword_rules_for(lang: str) -> List[Tuple[Tuple[str, ...], str, str, int]]:
    """Return the rule list for `lang`, falling back to en."""
    return LOCALIZED_RULES.get(lang, LOCALIZED_RULES["en"])


def classify_with_language(text: str) -> Dict[str, Any]:
    """Drop-in upgrade for prompt_engine.classify_prompt_heuristic.
    Returns same shape + `language`. Applies localized rules + location/premium boost."""
    t = (text or "").lower()
    lang = detect_language(text or "")
    rules = keyword_rules_for(lang)
    best = {
        "prompt_type": "informational",
        "buyer_stage": "awareness",
        "revenue_score": 25.0,
        "language": lang,
    }
    for matchers, ptype, stage, rev in rules:
        if any(m in t for m in matchers):
            if rev > best["revenue_score"]:
                best["prompt_type"] = ptype
                best["buyer_stage"] = stage
                best["revenue_score"] = float(rev)
    boosters = _LOCATION_BOOSTERS.get(lang, _LOCATION_BOOSTERS["en"])
    if any(loc in t for loc in boosters):
        best["revenue_score"] = min(100.0, best["revenue_score"] + 10)
    premium = _PREMIUM_BOOSTERS.get(lang, _PREMIUM_BOOSTERS["en"])
    if any(p in t for p in premium):
        best["revenue_score"] = min(100.0, best["revenue_score"] + 5)
    return best


__all__ = [
    "init",
    "normalize",
    "levenshtein",
    "canonicalize",
    "list_brands",
    "merge_brands",
    "set_canonical_for_us",
    "detect_language",
    "has_hungarian",
    "has_german",
    "has_spanish",
    "has_french",
    "LOCALIZED_RULES",
    "keyword_rules_for",
    "classify_with_language",
]
