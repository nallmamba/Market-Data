"""Canonical market-data codes and conservative cTrader symbol matching."""

CANONICAL_SYMBOL_CANDIDATES = {
    "us100": (
        "us100", "nas100", "us100.cash", "nas100.cash", "ustech", "ustech100", "ndx", "nasdaq100",
    ),
    "us30": ("us30", "us30.cash", "us30cash", "dj30", "dow30", "wallstreet30"),
    "us500": ("us500", "us500.cash", "us500cash", "sp500", "spx500"),
    "uk100": ("uk100", "ftse100", "ukx"),
    "de40": ("de40", "ger40", "dax40", "germany40"),
    "xauusd": ("xauusd", "gold"),
    "xagusd": ("xagusd", "silver"),
}


def apply_common_symbol_aliases(symbol_lookup):
    """Add a canonical code only when its cTrader name resolves unambiguously."""
    resolved = {}
    ambiguous = {}
    for canonical, candidates in CANONICAL_SYMBOL_CANDIDATES.items():
        if canonical in symbol_lookup:
            continue
        matching = {
            candidate: symbol_lookup[candidate]
            for candidate in candidates
            if candidate in symbol_lookup
        }
        ids = set(matching.values())
        if len(ids) == 1:
            symbol_lookup[canonical] = next(iter(ids))
            resolved[canonical] = next(iter(matching))
        elif len(ids) > 1:
            ambiguous[canonical] = sorted(matching)
    return resolved, ambiguous


def format_symbol_inventory(symbol_map):
    """Return unique, sorted cTrader names without exposing account identifiers."""
    names = {
        name.strip()
        for name in symbol_map.values()
        if isinstance(name, str) and name.strip()
    }
    return sorted(names, key=str.casefold)
