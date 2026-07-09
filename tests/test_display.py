from henftdir.display import apply_mapping

PROPS = {"Name": "Dragon", "Rarity": "rare", "image": "https://x/1.png"}


def test_unmapped_symbol_serves_null():
    assert apply_mapping(None, "CARD", 1, PROPS) is None


def test_from_paths():
    mapping = {"name": {"from": "properties.Name"},
               "image": {"from": "properties.image"},
               "attributes": ["Rarity", "Missing"]}
    d = apply_mapping(mapping, "CARD", 1, PROPS, "Card Game")
    assert d == {"name": "Dragon", "image": "https://x/1.png",
                 "collection": "Card Game",
                 "attributes": {"Rarity": "rare", "Missing": None}}


def test_templates():
    mapping = {"name": {"template": "{symbol} #{nft_id}"},
               "image": {"template": "https://cdn/{symbol}/{nft_id}.png"}}
    d = apply_mapping(mapping, "CARD", 7, {})
    assert d["name"] == "CARD #7"
    assert d["image"] == "https://cdn/CARD/7.png"
    assert d["collection"] == "CARD"


def test_bad_template_degrades_to_none():
    d = apply_mapping({"name": {"template": "{nope}"}}, "CARD", 1, PROPS)
    assert d["name"] is None
