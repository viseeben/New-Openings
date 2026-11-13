#!/usr/bin/env python3
"""
Coffee shop openings lead pipeline for Dripos.

Pulls recent permit / license / inspection records from public open-data
portals (Socrata and ArcGIS), keeps the ones that look like coffee shops,
dedupes them into one row per business location, scores them, and writes
a SQLite database plus a CSV.

Standard library only. Python 3.9+.

Usage:
    python3 leads.py run                  # pull every enabled source (last 365 days)
    python3 leads.py run --days 90        # shorter window
    python3 leads.py run --only chicago_licenses,nyc_inspections
    python3 leads.py discover socrata data.cityofchicago.org r5kz-chrr
    python3 leads.py discover arcgis https://.../FeatureServer/0
    python3 leads.py stats
"""
import argparse
import csv
import datetime as dt
import hashlib
import json
import os
import re
import sqlite3
import sys
import time
import urllib.error
import urllib.parse
import urllib.request

HERE = os.path.dirname(os.path.abspath(__file__))
SOURCES_FILE = os.path.join(HERE, "sources.json")
DB_FILE = os.path.join(HERE, "coffee_leads.db")
CSV_FILE = os.path.join(HERE, "coffee_leads.csv")
UA = "coffee-leads-pipeline/1.0 (open data research)"
PAGE = 5000

# --------------------------------------------------------------------------
# Coffee classification
# --------------------------------------------------------------------------
# Strong: the name almost certainly means a coffee business.
STRONG = [
    r"coffee", r"\bespresso", r"\broaster", r"\broastery", r"\broasting",
    r"\bbarista", r"\blattes?\b", r"\bcappuccino", r"\bcold ?brew", r"\bkaffe",
    r"\bcaff[eè]\b", r"\bjava\b", r"\bbrew(ing)? bar\b", r"boba", r"\bcafecito", r"\broast\b",
    r"\btea ?house\b", r"\bbubble tea\b", r"\bmatcha\b", r"\bdrive ?thru brew",
]
# A "cafe" name is often a coffee shop, sometimes a diner. Scored lower than strong.
CAFE = [r"\bcaf[eé]s?\b", r"\bkafe\b", r"\bcafeteria\b"]
# Keyword: coffee-ish words that count on their own (no category needed), but
# score lower than "strong" because they're noisier ("Grounded Yoga", "Tea Party").
# Dropped if the dataset's category field says something clearly non-food.
KEYWORD = [
    r"\bbrews?\b", r"\bgrinds?\b", r"\bgrounds\b", r"\bgrounded\b", r"\bcups?\b",
    r"\btea\b", r"\bperks?\b",
]
# Weak: only counts when the category field also points at coffee/food service.
WEAK = [
    r"\bbeans?\b", r"\bgrind\b", r"\bmug\b", r"\bbakery\b", r"\bdonuts?\b", r"\bbagels?\b",
]
# Things that match the patterns above but are not coffee shops.
NEGATIVE = [
    r"brewery", r"brewing co", r"brewpub", r"\bbeer", r"taproom", r"winery",
    r"internet cafe", r"cyber cafe", r"\bliquor", r"cannabis", r"dispensary",
    r"\bvape", r"smoke shop", r"hookah", r"laundr", r"coffee table", r"vending",
    r"gas station", r"bookkeeping", r"\bplatte\b", r"cafeteria", r"\bjavascript",
    r"java (software|dev)", r"\bbeanie", r"bean bag", r"coffee ?break service",
    # Excluded on request (restaurant-type names), even if they also say coffee/cafe:
    r"\bsandwich(es)?\b", r"\bburgers?\b", r"\bbowls\b", r"\bplates\b",
]
# Extra noise filters that only apply to KEYWORD-level matches, so a real
# "Grounded Coffee" still gets through but "Grounds Maintenance LLC" doesn't.
KEYWORD_NEGATIVE = [
    r"landscap", r"\blawns?\b", r"grounds? ?(maintenance|keeping|care|crew|service)", r"playground",
    r"campground", r"fairground", r"underground", r"background", r"\bworld cup\b", r"\btea ?party\b",
    r"\bcounsel", r"therap", r"\byoga\b", r"\bfitness\b", r"\bchurch\b", r"ministr", r"electric",
    r"plumb", r"roofing", r"construct", r"\bgolf\b", r"\bhockey\b", r"\bsoccer\b", r"trophy",
    r"\bauto\b", r"towing", r"\bperkins\b",
]
# Category text that clearly isn't food (vetoes a KEYWORD match). Numeric NAICS
# codes are vetoed unless they're food-related (72, 311, 4452, 4459).
NONFOOD_CATEGORY = [
    r"landscap", r"construct", r"contractor", r"repair", r"cleaning", r"janitorial", r"real estate",
    r"consult", r"legal", r"account", r"insurance", r"salon", r"barber", r"beauty", r"nail",
    r"automotive", r"\bauto\b", r"transport", r"trucking", r"plumb", r"electric", r"roofing",
    r"medical", r"health care", r"dental", r"fitness", r"religious", r"software", r"computer",
    r"apparel", r"clothing", r"jewelry", r"tattoo", r"cannabis", r"tobacco", r"liquor",
]
NEGATIVE_CATEGORY = [r"coffee-break service", r"door-to-door", r"holding compan"]
# Category / activity text that confirms a coffee business (NYC cuisine, NAICS, etc.)
COFFEE_CATEGORY = [r"coffee", r"\btea\b"]
NAICS_CATEGORY = [r"722515", r"snack and nonalcoholic beverage"]  # snack bars: coffee, juice, donuts, ice cream...
FOOD_CATEGORY = [r"food", r"restaurant", r"bakery", r"bakeries", r"donut", r"beverage",
                 r"cafe", r"caf[eé]", r"eating", r"dining", r"7225"]
CHAINS = [
    "starbucks", "dunkin", "peet's", "peets coffee", "tim horton", "dutch bros", "caribou coffee",
    "scooter's coffee", "scooters coffee", "biggby", "ziggi", "7 brew",
    "seven brew", "black rifle", "philz", "blue bottle", "la colombe",
    "coffee bean & tea leaf", "coffee bean and tea leaf", "panera",
    "mcdonald", "krispy kreme", "einstein", "tropical smoothie", "jamba",
    "kung fu tea", "gong cha", "sharetea", "the human bean", "pj's coffee",
    "aroma joe", "crimson cup", "foxtail coffee", "summermoon", "ziggis",
    "bigby", "costa coffee", "joe & the juice", "joe and the juice",
]

def _c(ps):
    return [re.compile(p, re.I) for p in ps]


_strong, _cafe, _weak, _neg = _c(STRONG), _c(CAFE), _c(WEAK), _c(NEGATIVE)
_negcat, _coffeecat, _foodcat = _c(NEGATIVE_CATEGORY), _c(COFFEE_CATEGORY), _c(FOOD_CATEGORY)
_naicscat = _c(NAICS_CATEGORY)
_kw, _kwneg, _nonfood = _c(KEYWORD), _c(KEYWORD_NEGATIVE), _c(NONFOOD_CATEGORY)


def nonfood_category(cat):
    c = (cat or "").strip()
    if re.fullmatch(r"\d{2,6}", c):
        return not re.match(r"(72|311|4452|4459)", c)
    return any(p.search(c) for p in _nonfood)


def classify(name, category_text=""):
    """Return (match_level, chain). match_level is one of
    strong | category | cafe | keyword | naics | weak | None (not a coffee lead)."""
    text = (name or "").strip()
    cat = (category_text or "").strip()
    low = text.lower()
    if any(p.search(text) for p in _neg) or any(p.search(cat) for p in _negcat):
        return None, None
    chain = next((c for c in CHAINS if c in low), None)
    if chain or any(p.search(text) for p in _strong):
        return "strong", chain
    if cat and any(p.search(cat) for p in _coffeecat):
        return "category", None
    if any(p.search(text) for p in _cafe):
        return "cafe", None
    if any(p.search(text) for p in _kw) and not any(p.search(text) for p in _kwneg):
        # A category that's present but clearly not food (e.g. NAICS landscaping) vetoes it.
        if not nonfood_category(cat):
            return "keyword", None
    if cat and any(p.search(cat) for p in _naicscat):
        return "naics", None
    if cat and any(p.search(text) for p in _weak) and any(p.search(cat) for p in _foodcat):
        return "weak", None
    return None, None


# --------------------------------------------------------------------------
# HTTP helpers
# --------------------------------------------------------------------------
def http_json(url, params=None, retries=4, post=False):
    """GET (or form POST, for queries too long for a URL) and parse JSON."""
    body = None
    if params and post:
        body = urllib.parse.urlencode(params).encode()
    elif params:
        url = url + ("&" if "?" in url else "?") + urllib.parse.urlencode(params)
    headers = {"User-Agent": UA, "Accept": "application/json"}
    if body:
        headers["Content-Type"] = "application/x-www-form-urlencoded"
    token = os.environ.get("SOCRATA_APP_TOKEN")
    if token and "/resource/" in url:
        headers["X-App-Token"] = token
    last = None
    for i in range(retries):
        try:
            req = urllib.request.Request(url, data=body, headers=headers)
            with urllib.request.urlopen(req, timeout=90) as r:
                return json.loads(r.read().decode("utf-8"))
        except urllib.error.HTTPError as e:
            body = e.read().decode("utf-8", "replace")[:500]
            last = f"HTTP {e.code}: {body}"
            if e.code in (400, 403, 404):
                break
        except Exception as e:  # network blips
            last = repr(e)
        time.sleep(2 ** i)
    raise RuntimeError(f"{last} <- {url[:300]}")


# --------------------------------------------------------------------------
# Connectors
# --------------------------------------------------------------------------
def socrata_like(fields, words):
    parts = []
    for f in fields:
        for w in words:
            parts.append(f"upper({f}) like '%{w.upper()}%'")
    return "(" + " OR ".join(parts) + ")"


# Server-side pre-filter (substring match, so ROAST also catches ROASTERS, GROUND
# catches GROUNDS/GROUNDED, TEA catches TEA HOUSE). classify() then decides what
# actually counts, so a word added here must also be in one of the lists above.
DEFAULT_WORDS = ["COFFEE", "ESPRESSO", "CAFE", "CAFÉ", "ROAST", "BREW", "JAVA",
                 "LATTE", "BEAN", "BARISTA", "BOBA", "TEA", "GRIND", "GROUND", "CUP", "PERK",
                 "MATCHA", "KAFFE",
                 "STARBUCKS", "DUNKIN", "DUTCH BROS", "PEET", "CARIBOU",
                 "SCOOTER", "BIGGBY", "7 BREW", "PHILZ"]


def mapped_fields(src):
    out = []
    for v in src["fields"].values():
        for part in (v if isinstance(v, list) else str(v).split("|")):
            if part and part not in out:
                out.append(part)
    return out


def socrata_queries(src, since):
    """Return (base_url, params) for a source. Plain sources pull matching rows;
    mode=first_appearance sources group by facility and keep the ones whose
    FIRST record falls inside the window (for inspection-only datasets)."""
    base = f"https://{src['domain']}/resource/{src['dataset']}.json"
    df = src["date_field"]
    name_fields = src.get("filter_fields") or [src["fields"]["name"]]
    where = []
    if src.get("server_filter", True):
        where.append(socrata_like(name_fields, src.get("words", DEFAULT_WORDS)))
    if src.get("extra_where"):
        where.append(f"({src['extra_where']})")
    if src.get("mode") == "first_appearance":
        group = src["group_fields"]
        params = {"$select": ",".join(group) + f", min({df}) as first_date, count(*) as n_records",
                  "$group": ",".join(group),
                  "$having": f"first_date >= '{since.isoformat()}T00:00:00'",
                  "$order": "first_date DESC"}
    else:
        where.insert(0, f"{df} >= '{since.isoformat()}T00:00:00'")
        params = {"$select": ",".join(mapped_fields(src) + ([df] if df not in mapped_fields(src) else [])),
                  "$order": f"{df} DESC"}
    if where:
        params["$where"] = " AND ".join(where)
    return base, params


CACHE = {}


def fetch_socrata(src, since):
    base, params = socrata_queries(src, since)
    params = dict(params, **{"$limit": 50000})
    url = base + "?" + urllib.parse.urlencode(params)
    if src["id"] in CACHE:  # pre-fetched rows, keyed by source id
        return CACHE[src["id"]]
    out, offset = [], 0
    while True:
        params["$offset"] = offset
        rows = http_json(base, params)
        out.extend(rows)
        if len(rows) < 50000:
            break
        offset += 50000
    return out


def fetch_arcgis(src, since):
    url = src["url"].rstrip("/") + "/query"
    df = src["date_field"]
    if src.get("date_is_string"):
        where = [f"{df} >= '{since.isoformat()}'"]
    else:
        where = [f"{df} >= DATE '{since.isoformat()}'"]
    if src.get("server_filter", True):
        nf = src.get("filter_fields") or [src["fields"]["name"]]
        where.append(socrata_like(nf, src.get("words", DEFAULT_WORDS)))
    if src.get("extra_where"):
        where.append(f"({src['extra_where']})")
    out, offset = [], 0
    while True:
        params = {"where": " AND ".join(where), "outFields": "*", "f": "json",
                  "resultOffset": offset, "resultRecordCount": 2000,
                  "returnGeometry": "true", "outSR": 4326}
        # ArcGIS servers reject long GET URLs; POST the same query when it's long.
        long_q = len(urllib.parse.urlencode(params)) > 1500
        data = http_json(url, params, post=long_q)
        if "error" in data:
            raise RuntimeError(json.dumps(data["error"])[:500])
        feats = data.get("features", [])
        for f in feats:
            row = dict(f.get("attributes", {}))
            g = f.get("geometry") or {}
            if "x" in g:
                row["_lon"], row["_lat"] = g["x"], g["y"]
            out.append(row)
        if not data.get("exceededTransferLimit") or not feats:
            break
        offset += len(feats)
    return out


CONNECTORS = {"socrata": fetch_socrata, "arcgis": fetch_arcgis}


# --------------------------------------------------------------------------
# Normalization
# --------------------------------------------------------------------------
def get(row, spec):
    """spec may be a field name, a list of fields (joined with space), or None."""
    if not spec:
        return ""
    if isinstance(spec, list):
        return " ".join(str(get(row, s)) for s in spec if get(row, s)).strip()
    if "|" in spec:  # "a|b|c" = first non-empty
        for s in spec.split("|"):
            v = get(row, s)
            if v:
                return v
        return ""
    v = row.get(spec)
    if isinstance(v, dict):  # socrata location objects
        if "human_address" in v:
            try:
                h = json.loads(v["human_address"])
                return h.get("address", "")
            except Exception:
                return ""
        return ""
    return "" if v is None else str(v).strip()


def parse_date(v):
    if v in (None, ""):
        return None
    if isinstance(v, (int, float)) or re.fullmatch(r"\d{11,13}", str(v)):
        return dt.datetime.utcfromtimestamp(int(v) / 1000).date().isoformat()
    s = str(v)
    m = re.match(r"(\d{4})-(\d{2})-(\d{2})", s)
    if m:
        return f"{m.group(1)}-{m.group(2)}-{m.group(3)}"
    m = re.match(r"(\d{1,2})/(\d{1,2})/(\d{4})", s)
    if m:
        return f"{m.group(3)}-{int(m.group(1)):02d}-{int(m.group(2)):02d}"
    return None


SUFFIX = {"street": "st", "avenue": "ave", "av": "ave", "boulevard": "blvd",
          "road": "rd", "drive": "dr", "lane": "ln", "place": "pl",
          "court": "ct", "highway": "hwy", "parkway": "pkwy", "suite": "ste",
          "north": "n", "south": "s", "east": "e", "west": "w"}


def norm_addr(a):
    a = (a or "").lower()
    a = re.sub(r"[.,#]", " ", a)
    a = re.sub(r"\b(ste|suite|unit|apt|fl|floor|rm|room)\s*\S+", " ", a)
    toks = [SUFFIX.get(t, t) for t in a.split()]
    return " ".join(toks)


def norm_name(n):
    n = (n or "").lower()
    n = re.sub(r"\b(llc|inc|corp|co|ltd|the|dba|l l c)\b", " ", n)
    n = re.sub(r"[^a-z0-9 ]", "", n)
    return " ".join(n.split())


def lead_key(name, addr, zipc, city):
    a = norm_addr(addr)
    m = re.match(r"(\d+[a-z]?)\s+(.*)", a)
    if m:
        num, rest = m.group(1), m.group(2).split()
        street = next((t for t in rest if t not in ("n", "s", "e", "w")), "")
        addr_key = f"{num} {street}"
    else:
        addr_key = a[:20]
    place = (zipc or "")[:5] or (city or "").lower()
    if not addr_key.strip():
        addr_key = norm_name(name)
    return hashlib.sha1(f"{addr_key}|{place}".encode()).hexdigest()[:16]


def normalize(src, row):
    f = src["fields"]
    name = get(row, f.get("name"))
    alt = get(row, f.get("alt_name"))
    if not name and alt:
        name, alt = alt, ""
    category = get(row, f.get("category"))
    level, chain = classify(name, category)
    if not level and alt:
        level, chain = classify(alt, category)
    if not level:
        return None
    zipc = re.sub(r"[^0-9]", "", get(row, f.get("zip")))[:5]
    city = get(row, f.get("city")) or src.get("default_city", "")
    state = get(row, f.get("state")) or src.get("state", "")
    addr = get(row, f.get("address"))
    if src.get("address_has_city"):  # "1900 Aldrich St Austin, TX 78723"
        m = re.match(r"(.*?\d.*?(?:\b(?:st|ave|rd|dr|ln|blvd|pkwy|hwy|way|ct|pl|cv|trl|loop|cir|sq|row|pass|run|bend|path|plz|xing|ste \S+|bldg \S+|unit \S+|#\s?\S+)\.?))\s+([A-Za-z .'-]+),\s*([A-Z]{2})\s+(\d{5})", addr, re.I)
        if m:
            addr, city, state = m.group(1), m.group(2).strip(), m.group(3).upper()
        else:
            m = re.match(r"(.*)\s+([A-Za-z.'-]+),\s*([A-Z]{2})\s+\d{5}", addr)
            if m:
                addr, city, state = m.group(1), m.group(2), m.group(3)
    if src.get("exclude_status") and re.search(src["exclude_status"], get(row, f.get("status")), re.I):
        return None
    name = re.sub(r",?\s*(dissolved|delinquent|withdrawn)\b.*$", "", name, flags=re.I)
    if src.get("drop_unless_state") and state.upper() != src["drop_unless_state"]:
        return None
    lat = get(row, f.get("lat")) or row.get("_lat") or ""
    lon = get(row, f.get("lon")) or row.get("_lon") or ""
    return {
        "source_id": src["id"],
        "jurisdiction": src["jurisdiction"],
        "signal": src["signal"],
        "record_id": get(row, f.get("id")) or hashlib.sha1(json.dumps(row, sort_keys=True, default=str).encode()).hexdigest()[:16],
        "name": name.title() if name.isupper() else name,
        "alt_name": (alt.title() if alt.isupper() else alt) if alt and alt.lower() != name.lower() else "",
        "address": addr.title() if addr.isupper() else addr,
        "city": city.title() if city.isupper() else city,
        "state": state.upper()[:2],
        "zip": zipc,
        "event_date": parse_date(get(row, f.get("date")) or row.get(src["date_field"])),
        "event_type": get(row, f.get("event_type")) or src.get("event_label", ""),
        "category": category,
        "status": get(row, f.get("status")),
        "phone": get(row, f.get("phone")),
        "lat": str(lat), "lon": str(lon),
        "match_level": level,
        "chain": chain or "",
        "url": src.get("portal_url", ""),
        "raw": json.dumps(row, default=str)[:4000],
    }


# --------------------------------------------------------------------------
# Database
# --------------------------------------------------------------------------
SCHEMA = """
CREATE TABLE IF NOT EXISTS records (
  source_id TEXT, record_id TEXT, lead_key TEXT, jurisdiction TEXT, signal TEXT,
  name TEXT, alt_name TEXT, address TEXT, city TEXT, state TEXT, zip TEXT,
  event_date TEXT, event_type TEXT, category TEXT, status TEXT, phone TEXT,
  lat TEXT, lon TEXT, match_level TEXT, chain TEXT, url TEXT, raw TEXT,
  first_seen TEXT, PRIMARY KEY (source_id, record_id));
CREATE TABLE IF NOT EXISTS leads (
  lead_key TEXT PRIMARY KEY, name TEXT, alt_names TEXT, address TEXT, city TEXT,
  state TEXT, zip TEXT, phone TEXT, lat TEXT, lon TEXT,
  first_event_date TEXT, last_event_date TEXT, signals TEXT, sources TEXT,
  event_types TEXT, record_count INTEGER, match_level TEXT, chain TEXT,
  score INTEGER, first_seen TEXT, status TEXT DEFAULT 'new', notes TEXT DEFAULT '',
  open_est_start TEXT, open_est_end TEXT, open_status TEXT, open_conf TEXT, open_basis TEXT);
CREATE TABLE IF NOT EXISTS runs (
  run_at TEXT, source_id TEXT, fetched INTEGER, matched INTEGER, error TEXT);
"""

SIGNAL_WEIGHT = {"pre_opening_inspection": 40, "new_food_permit": 35,
                 "plan_review": 35, "building_permit": 25,
                 "new_business_license": 25, "business_registration": 15,
                 "inspection": 10}


# --------------------------------------------------------------------------
# Estimated opening window
# --------------------------------------------------------------------------
# Rules of thumb, in days relative to the record's event date: (earliest, latest,
# confidence, why). These are typical timelines, not measured ones; tune them as
# real opening dates come in. A source can override with an "opening" block in
# sources.json, and a source that maps fields.open_date (an actual reported
# opening / first-sales date) beats any estimate.
OPENING_RULES = {
    "pre_opening_inspection": (0, 42, "high", "Pre-opening inspections happen in the last few weeks before a shop opens."),
    "new_food_permit": (0, 30, "high", "A new food permit is usually issued right around opening."),
    "plan_review": (60, 150, "medium", "Health plan review typically comes 2 to 5 months before opening."),
    "building_permit": (60, 180, "medium", "Build-out permits typically come 2 to 6 months before opening."),
    "new_business_license": (-14, 45, "medium", "A new business license is usually issued within weeks of opening."),
    "business_registration": (90, 270, "low", "A new LLC or trade name usually comes 3 to 9 months before opening, and some never open."),
    "inspection": (-90, 0, "medium", "Routine inspections happen after a shop is open."),
}
FIRST_APPEARANCE_RULE = (-60, 14, "medium",
                         "First inspection on record. These are often the first routine inspection, so the shop is likely already open.")
CONF_RANK = {"high": 3, "medium": 2, "low": 1}


def opening_window(recs, src_by_id, today):
    """Pick the most trustworthy opening estimate across a lead's records.
    Returns dict(open_est_start, open_est_end, open_status, open_conf, open_basis)."""
    best = None
    for r in recs:
        src = src_by_id.get(r["source_id"], {})
        d = r.get("event_date")
        actual = ""
        if src.get("fields", {}).get("open_date"):
            try:
                actual = parse_date(get(json.loads(r.get("raw") or "{}"), src["fields"]["open_date"])) or ""
            except Exception:
                actual = ""
        if actual:
            lo = hi = dt.date.fromisoformat(actual)
            conf, why = "high", "Opening / first-sales date reported in the public record."
            rank = 4
        elif d:
            o = src.get("opening")
            if o:
                rule = (o["lo"], o["hi"], o["conf"], o["basis"])
            elif src.get("mode") == "first_appearance":
                rule = FIRST_APPEARANCE_RULE
            else:
                rule = OPENING_RULES.get(r["signal"], (0, 90, "low", "Unknown signal type."))
            ev = dt.date.fromisoformat(d)
            lo, hi = ev + dt.timedelta(days=rule[0]), ev + dt.timedelta(days=rule[1])
            conf, why = rule[2], rule[3]
            rank = CONF_RANK[conf]
        else:
            continue
        key = (rank, -(hi - lo).days, hi)  # most confident, then narrowest, then latest
        if best is None or key > best[0]:
            best = (key, lo, hi, conf, why)
    if not best:
        return {"open_est_start": "", "open_est_end": "", "open_status": "", "open_conf": "", "open_basis": ""}
    _, lo, hi, conf, why = best
    status = "Upcoming" if today < lo else "Opening now" if today <= hi else "Probably open"
    if lo == hi and 0 <= (today - hi).days <= 30:  # a reported opening date in the last month
        status = "Opening now"
    return {"open_est_start": lo.isoformat(), "open_est_end": hi.isoformat(),
            "open_status": status, "open_conf": conf, "open_basis": why}


def score(lead, today):
    s = {"strong": 40, "category": 35, "cafe": 22, "keyword": 18, "naics": 15, "weak": 12}.get(lead["match_level"], 0)
    s += max(SIGNAL_WEIGHT.get(x, 5) for x in lead["signals"].split(","))
    if len(set(lead["signals"].split(","))) > 1:
        s += 10
    try:
        age = (today - dt.date.fromisoformat(lead["first_event_date"])).days
        s += 20 if age <= 60 else 12 if age <= 180 else 5 if age <= 365 else 0
    except Exception:
        pass
    if lead["chain"]:
        s -= 45  # corporate chains buy POS centrally; independents are the target
    if lead["phone"]:
        s += 3
    return max(0, min(100, s))


LEAD_ADDED_COLS = ["open_est_start", "open_est_end", "open_status", "open_conf", "open_basis"]


def migrate(con):
    """Add columns introduced after a database was first created."""
    have = [r[1] for r in con.execute("PRAGMA table_info(leads)")]
    for c in LEAD_ADDED_COLS:
        if c not in have:
            con.execute(f"ALTER TABLE leads ADD COLUMN {c} TEXT")


def rebuild_leads(con):
    today = dt.date.today()
    migrate(con)
    src_by_id = {s["id"]: s for s in load_sources()}
    rows = con.execute("SELECT * FROM records ORDER BY event_date").fetchall()
    cols = [d[0] for d in con.execute("SELECT * FROM records LIMIT 0").description]
    groups = {}
    for r in rows:
        r = dict(zip(cols, r))
        groups.setdefault(r["lead_key"], []).append(r)
    existing = {k: (st, nt, fs) for k, st, nt, fs in
                con.execute("SELECT lead_key, status, notes, first_seen FROM leads")}
    con.execute("DELETE FROM leads")
    rank = {"strong": 6, "category": 5, "cafe": 4, "keyword": 3, "naics": 2, "weak": 1}
    for k, recs in groups.items():
        dates = sorted(r["event_date"] for r in recs if r["event_date"])
        best = max(recs, key=lambda r: (rank.get(r["match_level"], 0), len(r["address"] or "")))
        names = sorted({r["name"] for r in recs} | {r["alt_name"] for r in recs if r["alt_name"]})
        lead = {
            "lead_key": k, "name": best["name"],
            "alt_names": "; ".join(n for n in names if n and n != best["name"]),
            "address": best["address"], "city": best["city"], "state": best["state"],
            "zip": best["zip"] or next((r["zip"] for r in recs if r["zip"]), ""),
            "phone": next((r["phone"] for r in recs if r["phone"]), ""),
            "lat": next((r["lat"] for r in recs if r["lat"]), ""),
            "lon": next((r["lon"] for r in recs if r["lon"]), ""),
            "first_event_date": dates[0] if dates else "",
            "last_event_date": dates[-1] if dates else "",
            "signals": ",".join(sorted({r["signal"] for r in recs})),
            "sources": ",".join(sorted({r["source_id"] for r in recs})),
            "event_types": "; ".join(sorted({r["event_type"] for r in recs if r["event_type"]}))[:500],
            "record_count": len(recs),
            "match_level": best["match_level"],
            "chain": next((r["chain"] for r in recs if r["chain"]), ""),
        }
        lead["score"] = score(lead, today)
        lead.update(opening_window(recs, src_by_id, today))
        st, nt, fs = existing.get(k, ("new", "", None))
        lead["status"], lead["notes"] = st, nt
        lead["first_seen"] = fs or min(r["first_seen"] for r in recs)
        con.execute(f"INSERT INTO leads ({','.join(lead)}) VALUES ({','.join('?'*len(lead))})",
                    list(lead.values()))
    con.commit()


def export_csv(con, path=CSV_FILE):
    cur = con.execute("""SELECT name, alt_names, address, city, state, zip, phone,
        first_event_date, last_event_date, signals, event_types, sources, record_count,
        open_status, open_est_start, open_est_end, open_conf,
        match_level, chain, score, lat, lon, first_seen, status, lead_key
        FROM leads ORDER BY score DESC, first_event_date DESC""")
    with open(path, "w", newline="") as fh:
        w = csv.writer(fh)
        w.writerow([d[0] for d in cur.description])
        w.writerows(cur)


# --------------------------------------------------------------------------
# Commands
# --------------------------------------------------------------------------
def load_sources():
    with open(SOURCES_FILE) as fh:
        return [s for s in json.load(fh)["sources"]]


def cmd_run(args):
    since = dt.date.today() - dt.timedelta(days=args.days)
    only = set(args.only.split(",")) if args.only else None
    if args.cache:
        import gzip
        opener = gzip.open if args.cache.endswith(".gz") else open
        with opener(args.cache, "rt") as fh:
            for k, v in json.load(fh).items():
                CACHE[k] = [dict(zip(v["cols"], r)) for r in v["rows"]] if isinstance(v, dict) else v
    con = sqlite3.connect(DB_FILE)
    con.executescript(SCHEMA)
    now = dt.datetime.now().isoformat(timespec="seconds")
    for src in load_sources():
        if only and src["id"] not in only:
            continue
        if not only and not src.get("enabled", True):
            continue
        t0 = time.time()
        try:
            rows = CONNECTORS[src["type"]](src, since)
            matched = 0
            for row in rows:
                rec = normalize(src, row)
                if not rec:
                    continue
                if rec["event_date"] and rec["event_date"] < since.isoformat():
                    continue
                rec["lead_key"] = lead_key(rec["name"], rec["address"], rec["zip"], rec["city"])
                rec["first_seen"] = now
                cols = list(rec)
                con.execute(
                    f"INSERT INTO records ({','.join(cols)}) VALUES ({','.join('?'*len(cols))}) "
                    f"ON CONFLICT(source_id, record_id) DO UPDATE SET "
                    + ",".join(f"{c}=excluded.{c}" for c in cols if c not in ("first_seen", "source_id", "record_id")),
                    [rec[c] for c in cols])
                matched += 1
            con.execute("INSERT INTO runs VALUES (?,?,?,?,?)", (now, src["id"], len(rows), matched, None))
            print(f"  {src['id']:<28} fetched {len(rows):>6}  coffee {matched:>5}  ({time.time()-t0:.1f}s)")
        except Exception as e:
            con.execute("INSERT INTO runs VALUES (?,?,?,?,?)", (now, src["id"], 0, 0, str(e)[:500]))
            print(f"  {src['id']:<28} ERROR {str(e)[:200]}")
        con.commit()
    rebuild_leads(con)
    export_csv(con)
    n = con.execute("SELECT count(*) FROM leads").fetchone()[0]
    ind = con.execute("SELECT count(*) FROM leads WHERE chain=''").fetchone()[0]
    print(f"\n{n} leads ({ind} independent) -> {DB_FILE}\n                    -> {CSV_FILE}")


def cmd_plan(args):
    """Print the exact query URLs (one page, $limit 50000) so they can be fetched
    by another client (e.g. a browser) and fed back with run --cache."""
    since = dt.date.today() - dt.timedelta(days=args.days)
    plan = []
    for src in load_sources():
        if src.get("enabled", True) and src["type"] == "socrata":
            base, params = socrata_queries(src, since)
            params = dict(params, **{"$limit": 50000})
            plan.append({"id": src["id"], "url": base + "?" + urllib.parse.urlencode(params)})
    print(json.dumps(plan, indent=1))


def cmd_discover(args):
    if args.kind == "socrata":
        meta = http_json(f"https://{args.target}/api/views/{args.dataset}.json")
        print(meta.get("name"), "|", meta.get("rowsUpdatedAt"))
        for c in meta.get("columns", []):
            print(f"  {c['fieldName']:<35} {c['dataTypeName']:<12} {c.get('name')}")
        print(json.dumps(http_json(f"https://{args.target}/resource/{args.dataset}.json", {"$limit": 2}), indent=1)[:3000])
    else:
        meta = http_json(args.target.rstrip("/"), {"f": "json"})
        print(meta.get("name"))
        for c in meta.get("fields", []):
            print(f"  {c['name']:<35} {c['type']}")


def cmd_stats(args):
    con = sqlite3.connect(DB_FILE)
    for q in ["SELECT state, count(*) FROM leads GROUP BY state ORDER BY 2 DESC",
              "SELECT sources, count(*) FROM leads GROUP BY sources ORDER BY 2 DESC",
              "SELECT match_level, count(*) FROM leads GROUP BY 1",
              "SELECT count(*), sum(chain='') FROM leads"]:
        print(q)
        for r in con.execute(q):
            print("  ", r)


def main():
    p = argparse.ArgumentParser()
    sp = p.add_subparsers(dest="cmd", required=True)
    r = sp.add_parser("run")
    r.add_argument("--days", type=int, default=365)
    r.add_argument("--only")
    r.add_argument("--cache", help="JSON(.gz) file {source_id: rows} or {source_id: {cols, rows}} of pre-fetched responses")
    pl = sp.add_parser("plan")
    pl.add_argument("--days", type=int, default=365)
    d = sp.add_parser("discover")
    d.add_argument("kind", choices=["socrata", "arcgis"])
    d.add_argument("target")
    d.add_argument("dataset", nargs="?")
    sp.add_parser("stats")
    a = p.parse_args()
    {"run": cmd_run, "plan": cmd_plan, "discover": cmd_discover, "stats": cmd_stats}[a.cmd](a)


if __name__ == "__main__":
    main()
