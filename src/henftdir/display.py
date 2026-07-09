"""Display-mapping layer: the directory itself.

Every HE collection encodes name/image/attributes in its own property
conventions; a display mapping is a small, versioned adapter from raw
instance properties to the normalized display object:

    {"name": ..., "image": ..., "collection": ..., "attributes": {...}}

Mapping spec (jsonb in display_mappings.mapping), all fields optional:

    {
      "name":  {"from": "properties.Name"} | {"template": "{symbol} #{nft_id}"},
      "image": {"from": "properties.image"} | {"template": "https://.../{nft_id}.png"},
      "attributes": ["Rarity", "Speed"]     # property names to surface
    }

`from` walks a dot path over {"symbol", "nft_id", "properties": {...}};
`template` formats over the same namespace (flat properties included).
Unmapped symbols serve `display: null` — the API never blocks on mapping
coverage, and coverage is a published /status metric.

NOTE: the display-object shape is a pre-release draft. This module makes
the shape a data question (adapters are rows), so revising it later is a
mapping migration, not a code change.
"""

from __future__ import annotations

import logging
from typing import Any

logger = logging.getLogger(__name__)


def _walk(namespace: dict, path: str) -> Any:
    node: Any = namespace
    for key in path.split("."):
        if not isinstance(node, dict):
            return None
        node = node.get(key)
    return node


def _render(rule: Any, namespace: dict) -> Any:
    if not isinstance(rule, dict):
        return None
    if "from" in rule:
        return _walk(namespace, rule["from"])
    if "template" in rule:
        flat = {**namespace.get("properties", {}), **{
            k: v for k, v in namespace.items() if not isinstance(v, dict)}}
        try:
            return rule["template"].format(**flat)
        except (KeyError, IndexError, ValueError):
            return None
    return None


def apply_mapping(
    mapping: dict | None,
    symbol: str,
    nft_id: int,
    properties: dict,
    collection_name: str | None = None,
) -> dict | None:
    """Normalized display object, or None when the symbol has no mapping."""
    if mapping is None:
        return None
    namespace = {"symbol": symbol, "nft_id": nft_id,
                 "properties": properties or {}}
    attributes = {
        key: (properties or {}).get(key)
        for key in mapping.get("attributes", [])
    }
    return {
        "name": _render(mapping.get("name"), namespace),
        "image": _render(mapping.get("image"), namespace),
        "collection": collection_name or symbol,
        "attributes": attributes,
    }


def load_mappings(conn) -> dict[str, dict]:
    """Latest mapping version per symbol (sync connection; API layer)."""
    rows = conn.execute(
        "SELECT DISTINCT ON (symbol) symbol, mapping FROM display_mappings "
        "ORDER BY symbol, version DESC"
    ).fetchall()
    return {r["symbol"]: r["mapping"] for r in rows}
