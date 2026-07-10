-- Display-mapping seed data for he-nft-directory (src/henftdir/display.py).
--
-- Coverage as of 2026-07-10: every collection in HE's full catalog (152)
-- was audited with live on-chain property samples; 91 are mapped. The rest
-- are deliberately excluded:
--   - 37 have zero issued instances (nothing to display);
--   - 10 have instances but empty properties;
--   - test/junk data: ABCDEFGHIJ, ZINGALPHA (witness-sig plumbing),
--     TEST, DTEST, TESTNFT, IDLETEST, DCROPSTEST, STT, ACHVMNT ("null"
--     values), SCOOTER, CIMB;
--   - properties the mapping engine cannot reach (JSON-encoded strings /
--     CSV blobs): BHRIGA, ICSOUL, ZCOLISEUM, HEROAPI.
--
-- Built from real on-chain property samples plus live web research into
-- each project's official site/docs/source for an image source (see
-- this repository's git history for full per-collection research notes).
-- Most collections have NO confirmed public image source -- their
-- issuing game either never exposed one, or the project's domain is now
-- dead/parked/squatted. Those ship with name + attributes only (image
-- renders null, which the API always tolerates). Collections with a
-- verified *working* image URL at research time: PUNK, DEMONIC, CSHOTS,
-- CSPACKS, DOJO, COMPLOT, KATAN (partial). BHREQT, MINAVA, and ANIME
-- have a schema-correct image field but their sample host was
-- unreachable at research time -- included anyway since the mapping
-- reflects the real on-chain schema, not a guess.
--
-- Apply with: psql henftdir -f seed_display_mappings.sql
-- Safe to re-run: each INSERT is version 1 for a symbol not yet mapped;
-- re-running after a symbol already has version 1 will conflict on the
-- primary key (symbol, version) and should be skipped or bumped by hand.

INSERT INTO display_mappings (symbol, version, mapping) VALUES

-- Top-usage collections (real per-game research) -----------------------

('STAR', 1, '{"name": {"from": "properties.type"}, "attributes": ["class", "stats"]}'),
-- Rising Star Game. No public card-art API/CDN found (risingstargame.com
-- has no exposed asset endpoints; api.risingstargame.com does not
-- resolve). "type" is the only human-readable field present.

('LMS', 1, '{"name": {"from": "properties.Name"}, "attributes": ["Speed", "Rarity", "durability"]}'),
-- "Limitless" miners/tools collection (Hivereward). No image source
-- researched directly, but schema is clean and reliable.

('DCROPS', 1, '{"name": {"from": "properties.name"}, "attributes": []}'),
-- dcrops.com. Only "name" is a real top-level field; "nft"/"primary"/
-- "secondary" are JSON-encoded strings the mapping engine can't parse.

('CITY', 1, '{"name": {"from": "properties.name"}, "attributes": ["type", "income", "population", "workers", "popularity"]}'),
-- dCity. dcity.io/api.dcity.io were unreachable at research time
-- (DNS SERVFAIL); no image mapping possible to verify.

('HKFARM', 1, '{"name": {"from": "properties.NAME"}, "attributes": ["TYPE", "SEEDID", "OCCUPIED", "SUBDIVIDED"]}'),
-- Hashkings farm plots. A GitHub asset repo exists but predates the
-- current Hive Engine game and filenames don't match NAME casing.

('HMOTA', 1, '{"name": {"from": "properties.NAME"}, "attributes": ["TYPE", "PR", "SPT", "WATER"]}'),
-- Hashkings BETA assets (seeds/plots). Same publisher as HKFARM; no
-- seed-strain art found in the same asset repo.

('NFTHUESO', 1, '{"name": {"template": "{symbol} #{nft_id}"}, "attributes": ["ID", "POWER", "EDITION"]}'),
-- "Big Dog Bone" (Mundo Virtual). No name field on-chain; live app's JS
-- bundle has only generic branding art, no per-instance image API.

('WOO', 1, '{"name": {"template": "{symbol} #{nft_id}"}, "attributes": ["type", "edition", "foil"]}'),
('WOOGYMBAG', 1, '{"name": {"template": "{symbol} #{nft_id}"}, "attributes": ["type", "edition", "foil"]}'),
('WOOITEMS', 1, '{"name": {"template": "{symbol} #{nft_id}"}, "attributes": ["type", "edition"]}'),
('WOOLAND', 1, '{"name": {"template": "{symbol} #{nft_id}"}, "attributes": ["type", "edition", "foil"]}'),
-- Wrestling Organization Online. No public numeric-type-to-name/art
-- manifest found anywhere (site, GitBook docs, GitHub, white paper);
-- project is explicitly listed as not open-source.

('NFTSR', 1, '{"name": {"from": "properties.artSeries"}, "attributes": ["artId", "notes"]}'),
-- NFT Showroom. Their real CDN (cdn.nftshowroom.com) needs the
-- original CIDv1+filename+extension, which on-chain only stores as a
-- bare CIDv0 -- confirmed unresolvable via that CDN or any public IPFS
-- gateway (5 tested). Image omitted; artSeries confirmed reliable.

('IFCNFTS', 1, '{"name": {"from": "properties.p1"}, "attributes": ["p2", "p3", "p4", "p5", "p6", "p7", "p8", "p9", "p10"]}'),
-- Infernal Coliseum. infernalcoliseum.com is dead (NXDOMAIN); p1-p10
-- meanings inferred from ~15 sampled instances, not documentable.

('EQUIPMENT', 1, '{"name": {"from": "properties.Name"}, "attributes": ["Rarity", "Foil", "Level", "CCC"]}'),
('IMMORTAL', 1, '{"name": {"from": "properties.Name"}, "attributes": ["Rarity", "Foil", "Level", "CCC"]}'),
-- Immortal Creed -- confirmed shut down 2025-10-26 per its own archived
-- docs; immortalcreed.io now redirects to an unrelated squatted site.
-- No image API ever documented beyond generic CMS screenshots.

('STARDOM', 1, '{"name": {"template": "{category} {code}"}, "attributes": ["rarity", "stats"]}'),
-- Stardom Play. stardomplay.com is dead (NXDOMAIN); "code" is a cryptic
-- token with no public lookup table, so category+code is the best
-- available name.

('LIMITLESSX', 1, '{"name": {"from": "properties.NAME"}, "attributes": ["ATK", "DEF", "SPD", "HP"]}'),
-- "Limitless" fighters (hiverewardminer). A sibling collection (LMS)
-- has a working art CDN keyed by Name+Rarity, but Fighters has no
-- Rarity property and probing the same CDN with Fighter names failed.

('BHREQT', 1, '{"name": {"from": "properties.name"}, "image": {"from": "properties.image"}, "attributes": ["type"]}'),
-- Blockhorseracing. On-chain schema stores a literal full image URL
-- per instance -- correct mapping regardless of live state, but
-- blockhorseracing.com is currently parked/expired (images will 404
-- until/unless the domain is restored).

('TUNZ', 1, '{"name": {"from": "properties.series"}, "attributes": ["edition"]}'),
-- NFTTunz. Same schema family as the NFT-Showroom group below but no
-- "cid" present in sampled instances.

('INK', 1, '{"name": {"template": "{symbol} #{nft_id}"}, "attributes": ["type", "edition", "foil"]}'),
-- CraftInk. playcraftink.com blocks AI crawlers (robots.txt) and no
-- secondary source had card-name/art data.

('WEED', 1, '{"name": {"from": "properties.series"}, "attributes": ["edition"]}'),
('LISTNFT', 1, '{"name": {"from": "properties.series"}, "attributes": ["edition"]}'),
-- NFT-Showroom-family schema (see note before BTUNES below); no cid
-- confirmed usable for these two in sampling.

('HEROTEST', 1, '{"name": {"from": "properties.name"}, "attributes": ["tier"]}'),
-- Despite the name, on-chain data looks like a real, live equipment
-- collection (1279 instances, 4 holders); "perks" is a JSON-encoded
-- string the mapping engine can't parse, so left out of attributes.

('LENSY', 1, '{"name": {"template": "{photoSeries} #{photoId}"}, "attributes": ["notes"]}'),

('PUNK', 1, '{"name": {"template": "{symbol} #{nft_id}"}, "image": {"template": "https://gateway.pinata.cloud/ipfs/{hash}/image.png"}, "attributes": []}'),
-- Blockhead Games. "hash" is an IPFS folder CID confirmed (live fetch)
-- to contain image.png -- verified working via Pinata's gateway.

('AAA', 1, '{"name": {"from": "properties.name"}, "attributes": ["type", "rarity"]}'),

('SPTNFT', 1, '{"name": {"from": "properties.series"}, "attributes": ["edition"]}'),
-- Splintertalk Collectibles -- NFT-Showroom-family schema.

('MUT', 1, '{"name": {"from": "properties.name"}, "attributes": ["type", "foil"]}'),
-- MuTerra.

('BGUNS', 1, '{"name": {"from": "properties.Name"}, "attributes": ["Skin", "Rarity"]}'),
-- BANG Weapons (Choof Studios). A historical filename pattern was
-- found in archived GitBook docs, but it's inconsistent across cards
-- (old cards need Name+Skin+Rarity, newer ones use a different
-- lowercase+numeric-index scheme) and bangdefense.net no longer
-- resolves -- not safely template-able.

('VIRAL', 1, '{"name": {"from": "properties.series"}, "attributes": ["edition"]}'),
-- Meme NFT (MemeHive) -- NFT-Showroom-family schema.

('PSYCARD', 1, '{"name": {"template": "{symbol} #{nft_id}"}, "attributes": ["kills", "level", "rarityroll", "itemroll"]}'),
-- PSYBER X. The "ipfs" property holds placeholder values ("primero",
-- "segundo"), not real CIDs -- confirmed unreliable, not used.

('CBM', 1, '{"name": {"from": "properties.card"}, "attributes": []}'),
-- CryptoBrewMaster. Domain squatted since ~2021-2022; GitBook docs
-- confirm the ingredient+quality naming but use non-derivable file IDs
-- for art.

('KODNFT', 1, '{"name": {"template": "{symbol} #{nft_id}"}, "attributes": ["type", "edition", "foil"]}'),
-- King of Duels. A real type->name/image lookup table was found in an
-- archived game bundle, but image URLs have random per-upload
-- suffixes (Cloudinary) that can't be derived from "type" -- would
-- need a static lookup table shipped as data, not a template; left
-- for a future enhancement if this collection's usage grows.

('FARM', 1, '{"name": {"from": "properties.name"}, "attributes": ["edition"]}'),
-- FarmFarmer. A working CDN was found
-- (hive-engine.nyc3.cdn.digitaloceanspaces.com/farmfarmer/images/
-- {Name_With_Underscores}.jpg, verified live) but requires a
-- space-to-underscore slugify step the mapping engine's plain
-- .format() templates don't support -- not worth adding a transform
-- feature for one collection; image omitted.

('DEMONIC', 1, '{"name": {"template": "{subtype} {type} #{num}"}, "image": {"template": "https://d28h36qk0p1alw.cloudfront.net/images/nft/{type}/{subtype}/{num}.png"}, "attributes": ["type", "subtype", "num"]}'),
-- DemoniCore. Public card gallery at demonicore.com/cards confirms
-- this exact CDN pattern; verified live (200, image/png) for the
-- sampled instance.

('CSHOTS', 1, '{"name": {"from": "properties.name"}, "image": {"template": "https://gateway.pinata.cloud/ipfs/{img}"}, "attributes": ["schema"]}'),
('CSPACKS', 1, '{"name": {"from": "properties.name"}, "image": {"template": "https://gateway.pinata.cloud/ipfs/{img}"}, "attributes": ["schema", "description"]}'),
-- Crypto Shots. Confirmed via the live production JS bundle that the
-- app itself builds image URLs this way; verified live for CSPACKS
-- (CSHOTS' own sampled CID happened to be unpinned/stale, but the
-- pattern is the same code path for both collections).

('PAL', 1, '{"name": {"from": "properties.series"}, "attributes": ["edition"]}'),
-- PALnet (Minnow Support Project) -- NFT-Showroom-family schema.

('PEN', 1, '{"name": {"from": "properties.Title"}, "attributes": ["Type", "Stats"]}'),
-- Dreemport. "Properties" holds a JSON-encoded *array* containing a
-- real, live image URL, but the mapping engine has no way to parse an
-- array-encoded string -- would need a one-off transform, not added.

('POB', 1, '{"name": {"from": "properties.series"}, "attributes": ["edition"]}'),
-- Proof of Brain -- NFT-Showroom-family schema.

('PORT', 1, '{"name": {"from": "properties.Title"}, "attributes": ["Type", "Stats"]}'),
-- DreemPort -- same publisher/schema family as PEN, minus the
-- Properties field.

('GAMEOFLIFE', 1, '{"name": {"template": "{symbol} #{nft_id}"}, "attributes": ["originalOwner"]}'),

('DOJO', 1, '{"name": {"template": "{symbol} #{nft_id}"}, "image": {"template": "https://cdn.tribaldex.com/packmanager/DOJO/{edition}_{type}_{foil}.png"}, "attributes": ["type", "edition", "foil"]}'),
-- DefiDojo -- the Hive-Engine team's own packmanager demo. CDN pattern
-- found directly in the open-source frontend and verified live (200,
-- image/png) for both sampled instances. A real per-type name exists
-- via a separate packmanager RPC lookup, but that's a live external
-- call the mapping engine doesn't support -- image is the strong win
-- here regardless.

('TDEX', 1, '{"name": {"from": "properties.series"}, "attributes": ["edition"]}'),
-- Tribaldex -- NFT-Showroom-family schema; sampled instance had no cid
-- at all in its metadata.

('NFTLC', 1, '{"name": {"from": "properties.series"}, "attributes": ["edition"]}'),
-- NFT LasseCash -- NFT-Showroom-family schema.

('LPERK', 1, '{"name": {"template": "{symbol} #{nft_id}"}, "attributes": ["perkType", "founder"]}'),
-- PSYBER X land perks. Only boolean/string flags on-chain, no
-- image-bearing field exists.

('CINE', 1, '{"name": {"from": "properties.series"}, "attributes": ["edition"]}'),
-- Cine NFT -- NFT-Showroom-family schema.

('MINAVA', 1, '{"name": {"template": "{symbol} #{nft_id}"}, "image": {"from": "properties.Thumbnail"}, "attributes": ["Data", "Category"]}'),
-- Micronation Virtual Assets. On-chain schema stores a literal image
-- URL -- correct mapping, but the sampled host (a self-hosted
-- duckdns.org server) was unreachable at research time.

('ZINGACHMNT', 1, '{"name": {"from": "properties.name"}, "attributes": ["rarity", "claimed"]}'),

('KATAN', 1, '{"name": {"from": "properties.name"}, "image": {"template": "https://kalavia.github.io/cards/{name}/{variation}.jpg"}, "attributes": ["power", "variation"]}'),
-- Kalavian Tales. Verified live for both sampled instances (VaX the
-- Observer variations 0/1); other character names may use a different
-- directory layout per the source repo, so some entries may render a
-- broken image link -- acceptable since the API never blocks on this.

('DUBLUP', 1, '{"name": {"template": "{symbol} #{nft_id}"}, "attributes": ["market", "outcome"]}'),
('FISH', 1, '{"name": {"from": "properties.card"}, "attributes": []}'),
('NEONSTRIKE', 1, '{"name": {"from": "properties.name"}, "attributes": ["stats", "rarity"]}'),

('ALPHAPACK', 1, '{"name": {"template": "{symbol} #{nft_id}"}, "attributes": ["type", "edition", "foil"]}'),
-- Retzark. A real type->name lookup table (cards.json) exists on
-- GitHub, but retzark.com and every plausible asset subdomain are
-- dead -- no image source, and the name lookup is a static table the
-- mapping engine can't embed as a template.

('THIA', 1, '{"name": {"from": "properties.series"}, "attributes": ["edition"]}'),
-- ThiagoNFTs -- NFT-Showroom-family schema.

('CVL', 1, '{"name": {"template": "{symbol} #{nft_id}"}, "attributes": ["ID", "slot", "level"]}'),
('MYTHICAL', 1, '{"name": {"from": "properties.name"}, "attributes": ["edition"]}'),
('PSYLAND', 1, '{"name": {"template": "{symbol} #{nft_id}"}, "attributes": ["founder"]}'),

('PIRATESAGA', 1, '{"name": {"from": "properties.name"}, "attributes": []}'),
-- "nft"/"stats" are JSON-encoded strings the mapping engine can't
-- parse; "name" is a clean top-level field.

('DATA', 1, '{"name": {"template": "{symbol} #{nft_id}"}, "attributes": ["type", "edition", "foil"]}'),
-- Dataloft. dataloft.llc is a dead/placeholder domain with no card
-- gallery in any Wayback Machine snapshot since 2021.

('SPORTS', 1, '{"name": {"from": "properties.series"}, "attributes": ["edition"]}'),
-- SPORTS NFT -- NFT-Showroom-family schema.

('FOUNDER', 1, '{"name": {"template": "{symbol} #{nft_id}"}, "attributes": ["type"]}'),
-- PSYBER X founder kit. "ipfsHash" is a real, resolvable IPFS folder
-- containing one image, but the filename inside isn't derivable from
-- any on-chain field without guessing.

('ANIME', 1, '{"name": {"from": "properties.Name"}, "image": {"from": "properties.Image"}, "attributes": ["Description"]}'),
-- Schema-correct mapping (the collection literally has Name/Image/
-- Description fields on-chain) even though the only sampled instances
-- so far hold obvious placeholder/test values.

('COMPLOT', 1, '{"name": {"template": "{symbol} #{nft_id}"}, "image": {"template": "https://gateway.pinata.cloud/ipfs/{dbid}/CommercialPlot.png"}, "attributes": []}'),
-- PSYBER X. "dbid" is a real IPFS directory CID confirmed (live
-- fetch) to contain exactly one file, CommercialPlot.png -- verified
-- for this collection's only item type (4 instances total).

('TURF', 1, '{"name": {"template": "{symbol} #{nft_id}"}, "attributes": ["rarity", "attributes", "batch"]}'),
('MYTHICARD', 1, '{"name": {"template": "{symbol} #{nft_id}"}, "attributes": []}'),
-- "gp"/"dbp" hold the real name/rarity but as JSON-encoded strings the
-- mapping engine can't parse; "ipfsHash" is a placeholder ("\"\"").

('SNAPSHOT', 1, '{"name": {"from": "properties.series"}, "attributes": ["edition"]}'),
-- Hive Images NFT -- NFT-Showroom-family schema.

('BTUNES', 1, '{"name": {"from": "properties.series"}, "attributes": ["edition"]}');
-- BlockTunes -- the last of the NFT-Showroom-family schema collections
-- (BTUNES, CINE, NFTLC, POB, SNAPSHOT, SPORTS, THIA, VIRAL, WEED,
-- LISTNFT, PAL, TDEX, SPTNFT, TUNZ above all share it): "series" is a
-- reliable slug-format field; "metadata.cid" is a JSON-encoded-string
-- IPFS cid that (a) the mapping engine can't currently reach inside a
-- string-valued field, and (b) even decoded, is a bare CIDv0 that
-- neither NFT Showroom's own CDN (needs CIDv1+filename+ext) nor any
-- public IPFS gateway could resolve in testing (5 gateways tried) --
-- so this was deliberately NOT built as a speculative engine feature;
-- image is omitted for the whole family rather than guessed.

-- Display-mapping additions, researched 2026-07-10: every catalog collection
-- (152 total) was sampled live from HE; these are the ones with real,
-- engine-reachable properties that lacked a mapping. Per-instance images
-- where the chain carries one; a static unit icon only where every instance
-- is an identical unit (miner/land units).
INSERT INTO display_mappings (symbol, version, mapping) VALUES
-- dCITY API cards: game-state props (mostly JSON strings; reachable scalars only)
('API', 1, '{"name": {"template": "dCITY API #{nft_id}"}, "attributes": ["gov", "background", "x"]}'),
-- Crystal Miner: identical mining units; collection icon is the honest image
('CECM', 1, '{"name": {"template": "Crystal Miner #{nft_id}"}, "image": {"template": "https://cdn-icons-png.flaticon.com/128/2736/2736404.png"}, "attributes": ["Miner"]}'),
-- CONO registry: CARDBACK jpg URLs point at a dead host (dev1.cono.io, no
-- DNS 2026-07-10) -- name only rather than guaranteed-broken images
('CONONFT', 1, '{"name": {"template": "CONO Registry #{nft_id}"}}'),
-- CivilizationsLands: identical land units with a level
('CVZ', 1, '{"name": {"template": "CivilizationsLands #{nft_id}"}, "attributes": ["level"]}'),
('DROPCRATE', 1, '{"name": {"template": "Airdrop PSYBER Crate #{nft_id}"}, "attributes": ["opened"]}'),
('EMPIRE', 1, '{"name": {"from": "properties.name"}}'),
('ENTRYSHOP', 1, '{"name": {"from": "properties.name"}, "attributes": ["status", "price"]}'),
('HKPROJECT', 1, '{"name": {"from": "properties.PROP"}, "attributes": ["LVL", "VALID"]}'),
('HKUWORKER', 1, '{"name": {"template": "HK University Worker #{nft_id}"}, "attributes": ["Experience", "Energy", "Bio"]}'),
('HONNFT', 1, '{"name": {"from": "properties.cardname"}, "attributes": ["cardid", "cardmovemin", "cardmovemax", "cardtapmin", "cardtapmax"]}'),
('IDLE', 1, '{"name": {"from": "properties.name"}}'),
-- inji: cid is dag-cbor (metadata, not a direct image) -- name only, cid surfaced
('INJI', 1, '{"name": {"template": "inji #{nft_id}"}, "attributes": ["cid"]}'),
('MONKEY', 1, '{"name": {"template": "Guilty Monkey #{nft_id}"}, "attributes": ["Brewing", "Distribution", "Marketing"]}'),
-- Monster Swap: attributes prop is a CSV string (unreachable); scalars only
('MONSTER', 1, '{"name": {"template": "Monster #{nft_id}"}, "attributes": ["edition", "alive"]}'),
-- PSYBER X character: the ipfs CIDs 504 on multiple gateways (content
-- unpinned, checked 2026-07-10) -- no image; cid surfaced as an attribute
('PSYCHAR', 1, '{"name": {"template": "PSYBER X Character #{nft_id}"}, "attributes": ["dbid", "ipfs"]}'),
('PSYCRATE', 1, '{"name": {"template": "PSYBER Crate #{nft_id}"}, "attributes": ["opened"]}'),
-- Quodlibet: dls1 ipfs URLs 504 on multiple gateways (content unpinned,
-- checked 2026-07-10) -- name only
('QLB', 1, '{"name": {"template": "Quodlibet #{nft_id}"}}'),
-- rama nfts: image prop is scheme-less ("ipfs.io/ipfs/...")
('RAMANFTS', 1, '{"name": {"from": "properties.name"}, "image": {"template": "https://{image}"}, "attributes": ["dna"]}'),
('ROBO', 1, '{"name": {"template": "ROBO #{nft_id}"}, "attributes": ["gen"]}'),
('TOW', 1, '{"name": {"from": "properties.name"}, "attributes": ["first", "second"]}'),
('TRUSTEX', 1, '{"name": {"template": "Trust Exchange #{nft_id}"}, "attributes": ["series", "edition", "type", "foil"]}'),
-- Vibes: each NFT is a live-music event
('VIBES', 1, '{"name": {"from": "properties.EventName"}, "attributes": ["Date", "EventID"]}'),
('WOOVOUCHER', 1, '{"name": {"template": "WOO Alpha Pack Voucher #{nft_id}"}, "attributes": ["Redemption"]}')
ON CONFLICT (symbol, version) DO NOTHING;
