#!/usr/bin/env python3
"""
Enrich coffee leads with owner names, Instagram, website, email and more.

Runs after `leads.py run` and adds an `enrichment` table to coffee_leads.db
(one row per lead_key; kept across runs). Steps, in order:

  owners  Owner / officer / registered-agent names and emails straight from the
          public records: fields mapped as owner/owner_org/email in sources.json,
          plus lookups in companion datasets (Chicago Business Owners, CT
          principals, Oregon associated names, Texas sales-tax outlets).
  osm     OpenStreetMap cafes (Overpass API): website, Instagram, Facebook,
          phone, opening hours, matched by address or location + name.
  search  Optional web search (Brave Search API, free tier) for the top
          independent leads with no Instagram yet. Only runs if BRAVE_API_KEY
          is set. Never fetches instagram.com itself.
  web     Visits each known website's home page (and its About / Our Story
          page) once a month and pulls Instagram / Facebook / TikTok links,
          emails, phone numbers and the owner/founder's name.
  people  Optional (BRAVE_API_KEY): one search per top lead for its owner or
          founder in news, local blogs and LinkedIn result titles. Every name
          found keeps a link to where it was found (web_owner_url).
  manual  enrichment_manual.json (hand-checked values, keyed by lead_key) wins
          over everything above.

Standard library only.

Usage:
    python3 enrich.py                       # all steps
    python3 enrich.py --steps owners,osm    # some steps
    python3 enrich.py --search-limit 100    # more Brave queries this run
    python3 enrich.py stats
"""
import argparse
import concurrent.futures as cf
import csv
import datetime as dt
import html
import json
import math
import os
import re
import sqlite3
import sys
import time
import urllib.error
import urllib.parse
import urllib.request

import leads as L

import functools
print = functools.partial(print, flush=True)  # live logs in GitHub Actions

HERE = L.HERE
MANUAL_FILE = os.path.join(HERE, "enrichment_manual.json")
BROWSER_UA = ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
              "(KHTML, like Gecko) Chrome/130.0 Safari/537.36 coffee-leads-enrichment")
NOW = dt.datetime.now(dt.timezone.utc).replace(microsecond=0).isoformat()[:19]

FIELDS = ["owner", "owner_role", "owner_source", "owners_all", "email", "website",
          "instagram", "instagram_conf", "facebook", "tiktok", "osm_phone", "hours",
          "shop_address", "enrich_sources"]

# Found-online owner, kept across runs (the steps that find it don't re-run daily).
PERSIST = ["web_owner", "web_owner_role", "web_owner_url", "web_owner_source", "owner_checked"]

SCHEMA = """
CREATE TABLE IF NOT EXISTS enrichment (
  lead_key TEXT PRIMARY KEY,
  owner TEXT, owner_role TEXT, owner_source TEXT, owners_all TEXT,
  email TEXT, website TEXT, instagram TEXT, instagram_conf TEXT,
  facebook TEXT, tiktok TEXT, osm_phone TEXT, hours TEXT, shop_address TEXT,
  enrich_sources TEXT,
  osm_id TEXT, web_checked TEXT, search_checked TEXT, search_note TEXT, updated_at TEXT,
  web_owner TEXT, web_owner_role TEXT, web_owner_url TEXT, web_owner_source TEXT, owner_checked TEXT);
CREATE TABLE IF NOT EXISTS lookup_cache (
  dataset TEXT, key TEXT, payload TEXT, fetched_at TEXT, PRIMARY KEY (dataset, key));
CREATE TABLE IF NOT EXISTS osm_cache (
  state TEXT PRIMARY KEY, payload TEXT, fetched_at TEXT);
"""

# --------------------------------------------------------------------------
# Helpers
# --------------------------------------------------------------------------
CORP = re.compile(r"\b(llc|l\.l\.c|inc|corp|corporation|company|co\b|ltd|lp|llp|pllc|pc|trust|"
                  r"holdings?|group|enterprises?|partners|services|management|ventures|"
                  r"incorporated|limited|foundation|association|agency|registered agent)\b", re.I)
AGENT_SERVICES = re.compile(
    r"legalzoom|registered agents?|northwest registered|corporation service|\bcsc\b|ct corporation|"
    r"incorp|zenbusiness|harbor compliance|cogency|national registered|united states corporation agents|"
    r"business filings|vcorp|inc ?authority|swyft|bizee|law offices?|\besq\b|attorney|cpa\b|"
    r"registered agent solutions|a registered agent|spiegel|rocket ?lawyer|\bnrai\b|tailor brands|"
    r"wyoming agents|corpnet|govdocfiling|united agent|sundoc|\bagent\b", re.I)
STOP = {"llc", "inc", "the", "co", "company", "corp", "and", "of", "l", "c", "ltd", "dba", "a", "&"}
GENERIC = {"coffee", "cafe", "caffe", "espresso", "roasters", "roastery", "roasting", "tea", "shop",
           "house", "bar", "kitchen", "bakery", "and", "co", "company", "llc", "inc", "the"}


def tokens(s):
    s = html.unescape(str(s or "")).lower().replace("&", " and ")
    s = re.sub(r"[’'`]", "", s)
    return [t for t in re.findall(r"[a-z0-9]+", s) if t not in STOP]


def sim(a, b):
    """Token overlap between two business names, 0..1 (Dice on tokens + squashed match)."""
    ta, tb = set(tokens(a)), set(tokens(b))
    if not ta or not tb:
        return 0.0
    d = 2 * len(ta & tb) / (len(ta) + len(tb))
    sa, sb = "".join(sorted(ta)), "".join(sorted(tb))
    squash_a, squash_b = "".join(tokens(a)), "".join(tokens(b))
    if squash_a and squash_a == squash_b:
        return 1.0
    return d


def distinctive(name):
    """Tokens that identify the business (not 'coffee', 'cafe', 'llc'...)."""
    return [t for t in tokens(name) if t not in GENERIC and len(t) > 1]


def handle_matches(handle, name):
    """How well a social handle matches a business name: high / medium / low."""
    h = re.sub(r"[^a-z0-9]", "", handle.lower())
    dist = distinctive(name)
    if not h or not dist:
        return "low"
    squash = "".join(dist)
    if len(squash) >= 4 and squash in h:
        return "high"
    hits = sum(1 for t in dist if len(t) >= 3 and t in h)
    if hits and hits >= max(1, len([t for t in dist if len(t) >= 3]) - 1):
        return "high" if hits >= 2 or len(dist) == 1 else "medium"
    return "medium" if hits else "low"


def is_person(name):
    n = (name or "").strip()
    if not n or len(n) < 4 or any(ch.isdigit() for ch in n):
        return False
    if CORP.search(n) or AGENT_SERVICES.search(n):
        return False
    return 2 <= len(n.split()) <= 5


def nice(name):
    n = re.sub(r"\s+", " ", str(name or "")).strip(" ,")
    return n.title() if n.isupper() or n.islower() else n


def norm_ig(v):
    """'@foo', 'instagram.com/foo/', 'https://www.instagram.com/foo?hl=en' -> 'foo'."""
    v = str(v or "").strip()
    m = re.search(r"instagram\.com/([A-Za-z0-9_.]{1,30})", v)
    h = m.group(1) if m else v.lstrip("@").split("/")[0].split("?")[0]
    h = h.strip(".").lower()
    if not re.fullmatch(r"[a-z0-9_.]{2,30}", h) or h in IG_RESERVED:
        return ""
    return h


IG_RESERVED = {"p", "reel", "reels", "explore", "accounts", "stories", "tv", "about", "developer",
               "legal", "directory", "web", "instagram", "share", "sharer", "privacy", "help", "tags",
               "locations", "challenge", "graphql", "static", "emails", "direct", "ar", "embed.js"}
FB_RESERVED = {"sharer", "sharer.php", "share", "tr", "plugins", "dialog", "login", "groups", "events",
               "profile.php", "pages", "watch", "help", "policies", "privacy", "home.php", "facebook",
               "2008", "photo", "photo.php", "story.php", "permalink.php", "people", "hashtag", "l.php"}


def http_get(url, headers=None, timeout=15, max_bytes=1_500_000):
    req = urllib.request.Request(url, headers=dict({"User-Agent": BROWSER_UA,
                                                    "Accept": "text/html,application/json;q=0.9,*/*;q=0.8",
                                                    "Accept-Language": "en-US,en;q=0.8"}, **(headers or {})))
    with urllib.request.urlopen(req, timeout=timeout) as r:
        raw = r.read(max_bytes)
        cs = r.headers.get_content_charset() or "utf-8"
        return r.geturl(), raw.decode(cs, "replace")


def haversine_m(lat1, lon1, lat2, lon2):
    p = math.pi / 180
    a = (math.sin((lat2 - lat1) * p / 2) ** 2 +
         math.cos(lat1 * p) * math.cos(lat2 * p) * math.sin((lon2 - lon1) * p / 2) ** 2)
    return 12742000 * math.asin(math.sqrt(a))


def fnum(v):
    try:
        f = float(v)
        return f if f else None
    except (TypeError, ValueError):
        return None


# --------------------------------------------------------------------------
# State: one in-memory dict per lead, saved at the end
# --------------------------------------------------------------------------
class Store:
    def __init__(self, con):
        self.con = con
        con.executescript(SCHEMA)
        cols = [r[1] for r in con.execute("PRAGMA table_info(enrichment)")]
        for c in FIELDS + ["osm_id", "web_checked", "search_checked", "search_note", "updated_at"] + PERSIST:
            if c not in cols:
                con.execute(f"ALTER TABLE enrichment ADD COLUMN {c} TEXT")
        self.cols = [r[1] for r in con.execute("PRAGMA table_info(enrichment)")]
        self.rows = {}
        for r in con.execute("SELECT * FROM enrichment"):
            self.rows[r[0]] = dict(zip(self.cols, r))
        self.leads = {}
        lcols = [d[0] for d in con.execute("SELECT * FROM leads LIMIT 0").description]
        for r in con.execute("SELECT * FROM leads"):
            d = dict(zip(lcols, r))
            self.leads[d["lead_key"]] = d
        self.records = {}
        rcols = [d[0] for d in con.execute("SELECT * FROM records LIMIT 0").description]
        for r in con.execute("SELECT * FROM records"):
            d = dict(zip(rcols, r))
            try:
                d["raw"] = json.loads(d["raw"] or "{}")
            except Exception:
                d["raw"] = {}
            self.records.setdefault(d["lead_key"], []).append(d)
        self.found = {}  # lead_key -> field -> list of (priority, value, source, extra)

    def row(self, k):
        if k not in self.rows:
            self.rows[k] = {c: None for c in self.cols}
            self.rows[k]["lead_key"] = k
        return self.rows[k]

    def add(self, k, field, value, source, prio, extra=None):
        if value in (None, ""):
            return
        self.found.setdefault(k, {}).setdefault(field, []).append((prio, value, source, extra))

    def resolve(self):
        """Pick the best value per field (highest priority) and write rows."""
        for k, fields in self.found.items():
            row = self.row(k)
            used = set(filter(None, (row.get("enrich_sources") or "").split(",")))
            for field, cands in fields.items():
                cands.sort(key=lambda c: -c[0])
                prio, value, source, extra = cands[0]
                if field == "owner":
                    row["owner"], row["owner_role"], row["owner_source"] = value, extra or "", source
                    allnames = []
                    for _, v, _, ex in cands:
                        s = f"{v} ({ex})" if ex else v
                        if v not in [a.split(" (")[0] for a in allnames]:
                            allnames.append(s)
                    row["owners_all"] = "; ".join(allnames)[:400]
                elif field == "instagram":
                    row["instagram"], row["instagram_conf"] = value, extra or ""
                else:
                    row[field] = value
                used.add(source)
            row["enrich_sources"] = ",".join(sorted(used))
            row["updated_at"] = NOW

    def save(self):
        for k, row in self.rows.items():
            if k not in self.leads:
                continue
            cols = list(row)
            self.con.execute(
                f"INSERT INTO enrichment ({','.join(cols)}) VALUES ({','.join('?' * len(cols))}) "
                f"ON CONFLICT(lead_key) DO UPDATE SET " + ",".join(f"{c}=excluded.{c}" for c in cols if c != "lead_key"),
                [row[c] for c in cols])
        self.con.commit()

    # cached JSON lookups -------------------------------------------------
    def cache_get(self, dataset, key, max_age_days):
        r = self.con.execute("SELECT payload, fetched_at FROM lookup_cache WHERE dataset=? AND key=?",
                             (dataset, key)).fetchone()
        if not r:
            return None
        payload = json.loads(r[0])
        age = (dt.datetime.fromisoformat(NOW) - dt.datetime.fromisoformat(r[1][:19])).days
        if not payload and age > 7:  # retry empty answers weekly
            return None
        if age > max_age_days:
            return None
        return payload

    def cache_put(self, dataset, key, payload):
        self.con.execute("INSERT OR REPLACE INTO lookup_cache VALUES (?,?,?,?)",
                         (dataset, key, json.dumps(payload), NOW))


# --------------------------------------------------------------------------
# Step: owners
# --------------------------------------------------------------------------
# Officer titles from best (most likely the owner) to worst.
ROLE_RANK = ["sole proprietor", "owner", "individual", "managing member", "member", "registrant",
             "president", "ceo", "chairman", "general partner", "partner", "principal", "manager",
             "principal officer", "authorized representative", "director", "vice president",
             "shareholder", "secretary", "treasurer", "officer", "process address",
             "individual with direct knowledge", "registered agent", "other"]


def role_prio(role):
    r = (role or "").lower()
    for i, x in enumerate(ROLE_RANK):
        if r == x or r.startswith(x):
            return 60 - i
    return 30


def socrata_in(domain, dataset, key_field, keys, select=None, extra_where=None):
    """Query a Socrata dataset for rows whose key_field is in keys (batched)."""
    out = []
    keys = sorted(set(k for k in keys if k))
    for i in range(0, len(keys), 60):
        chunk = keys[i:i + 60]
        where = f"{key_field} in (" + ",".join("'" + k.replace("'", "''") + "'" for k in chunk) + ")"
        if extra_where:
            where += f" AND ({extra_where})"
        params = {"$where": where, "$limit": 50000}
        if select:
            params["$select"] = select
        out.extend(L.http_json(f"https://{domain}/resource/{dataset}.json", params))
    return out


def cached_lookup(store, dataset_key, keys, fetch, max_age_days=60):
    """fetch(missing_keys) -> {key: payload}. Returns {key: payload} for all keys."""
    res, missing = {}, []
    for k in set(filter(None, keys)):
        v = store.cache_get(dataset_key, k, max_age_days)
        if v is None:
            missing.append(k)
        else:
            res[k] = v
    if missing:
        got = fetch(missing)
        for k in missing:
            store.cache_put(dataset_key, k, got.get(k, []))
            res[k] = got.get(k, [])
        store.con.commit()
    return res


def step_owners(store, sources):
    by_src = {s["id"]: s for s in sources}
    n0 = sum(1 for k in store.found if "owner" in store.found[k])

    # 1. owner / email columns that ride along in the main pull (fields.owner etc.)
    for k, recs in store.records.items():
        lead_name = store.leads.get(k, {}).get("name", "")
        for r in recs:
            src = by_src.get(r["source_id"])
            if not src:
                continue
            f, raw = src["fields"], r["raw"]
            role = src.get("owner_role", "owner")
            for key, prio_bonus in (("owner", 0), ("owner_org", -15)):
                v = nice(L.get(raw, f.get(key)))
                if not v or sim(v, lead_name) >= 0.8 or sim(v, r["name"]) >= 0.8 or AGENT_SERVICES.search(v):
                    continue
                if key == "owner" and not is_person(v) and CORP.search(v):
                    prio_bonus -= 15
                store.add(k, "owner", v, r["source_id"], role_prio(role) + prio_bonus, role)
            for key in ("email",):
                v = L.get(raw, f.get(key)).lower()
                if re.fullmatch(r"[^@\s]+@[^@\s]+\.[a-z]{2,}", v):
                    store.add(k, "email", v, r["source_id"], 50)

    # 2. companion datasets
    def recs_of(source_id):
        for k, recs in store.records.items():
            for r in recs:
                if r["source_id"] == source_id:
                    yield k, r

    def add_people(k, people, source):
        lead_name = store.leads.get(k, {}).get("name", "")
        for name, role in people:
            name = nice(name)
            if not name or sim(name, lead_name) >= 0.8 or AGENT_SERVICES.search(name):
                continue
            p = role_prio(role) + (0 if is_person(name) else -15)
            store.add(k, "owner", name, source, p, (role or "").lower())

    # Chicago: license -> account_number -> Business Owners (ezma-pppn)
    try:
        lic_recs = list(recs_of("chicago_new_business_licenses"))
        insp_recs = list(recs_of("chicago_food_license_inspections"))
        acct = {k_r[1]["record_id"]: L.get(k_r[1]["raw"], "account_number") for k_r in lic_recs}
        lic_nums = [L.get(r["raw"], "license_") for _, r in insp_recs]
        lic2acct = cached_lookup(
            store, "chi_license_account", lic_nums,
            lambda ks: {str(x["license_number"]): [x.get("account_number", "")] for x in socrata_in(
                "data.cityofchicago.org", "r5kz-chrr", "license_number", ks,
                select="license_number,account_number")})
        pairs = [(k, acct.get(r["record_id"])) for k, r in lic_recs] + \
                [(k, (lic2acct.get(L.get(r["raw"], "license_")) or [""])[0]) for k, r in insp_recs]
        owners = cached_lookup(
            store, "chi_owners", [a for _, a in pairs],
            lambda ks: _group(socrata_in("data.cityofchicago.org", "ezma-pppn", "account_number", ks,
                                         select="account_number,owner_name,owner_first_name,owner_last_name,owner_title"),
                              "account_number",
                              lambda x: [" ".join(filter(None, [x.get("owner_first_name"), x.get("owner_last_name")]))
                                         or x.get("owner_name", ""), x.get("owner_title", "")]))
        for k, a in pairs:
            add_people(k, owners.get(a, []), "chicago_business_owners")
        print(f"  owners: chicago accounts {len(set(a for _, a in pairs if a))}")
    except Exception as e:
        print(f"  owners: chicago lookup failed: {str(e)[:200]}")

    # Connecticut: Business Master id -> Principals (ka36-64k6)
    try:
        ct = [(k, L.get(r["raw"], "id")) for k, r in recs_of("ct_business_registrations")]
        princ = cached_lookup(
            store, "ct_principals", [b for _, b in ct],
            lambda ks: _group(socrata_in("data.ct.gov", "ka36-64k6", "business_id", ks,
                                         select="business_id,name__c,firstname,lastname,designation,type"),
                              "business_id",
                              lambda x: [x.get("name__c") or " ".join(filter(None, [x.get("firstname"), x.get("lastname")])),
                                         x.get("designation") or x.get("type") or "principal"]))
        for k, b in ct:
            add_people(k, princ.get(b, []), "ct_principals")
        print(f"  owners: connecticut businesses {len(ct)}")
    except Exception as e:
        print(f"  owners: connecticut lookup failed: {str(e)[:200]}")

    # Oregon: registry_number -> all associated names (same dataset, one row per name)
    try:
        orr = [(k, r["record_id"]) for k, r in recs_of("oregon_new_businesses")]
        people_types = ("MEMBER", "MANAGER", "PRESIDENT", "REGISTRANT", "AUTHORIZED REPRESENTATIVE",
                        "GENERAL PARTNER", "PARTNER", "SECRETARY", "REGISTERED AGENT")
        assoc = cached_lookup(
            store, "or_associated", [b for _, b in orr],
            lambda ks: _group(socrata_in("data.oregon.gov", "esjy-u4fc", "registry_number", ks,
                                         select="registry_number,associated_name_type,first_name,last_name,entity_of_record_name"),
                              "registry_number",
                              lambda x: [" ".join(filter(None, [x.get("first_name"), x.get("last_name")]))
                                         or x.get("entity_of_record_name", ""), x.get("associated_name_type", "")]
                              if x.get("associated_name_type") in people_types else None),
            max_age_days=3650)
        for k, b in orr:
            add_people(k, assoc.get(b, []), "oregon_sos")
        print(f"  owners: oregon businesses {len(orr)}")
    except Exception as e:
        print(f"  owners: oregon lookup failed: {str(e)[:200]}")

    # Texas: franchise taxpayer -> sales tax outlets (actual storefront address + first sales date)
    try:
        tx = [(k, r["record_id"]) for k, r in recs_of("texas_new_franchise_taxpayers")]
        outlets = cached_lookup(
            store, "tx_outlets", [b for _, b in tx],
            lambda ks: _group(socrata_in("data.texas.gov", "jrea-zgmq", "taxpayer_number", ks,
                                         select="taxpayer_number,outlet_name,outlet_address,outlet_city,outlet_zip_code,outlet_naics_code,outlet_first_sales_date"),
                              "taxpayer_number", lambda x: x))
        for k, b in tx:
            ols = outlets.get(b, [])
            if not ols:
                continue
            ols.sort(key=lambda o: (o.get("outlet_naics_code") != "722515", o.get("outlet_first_sales_date") or ""))
            o = ols[0]
            addr = ", ".join(filter(None, [nice(o.get("outlet_address")), nice(o.get("outlet_city")),
                                           "TX " + (o.get("outlet_zip_code") or "")[:5]]))
            fs = (o.get("outlet_first_sales_date") or "")[:10]
            label = f"{addr} ({nice(o.get('outlet_name'))}{', first sales ' + fs if fs else ''})"
            store.add(k, "shop_address", label, "tx_sales_tax_outlets", 50)
        print(f"  owners: texas taxpayers {len(tx)}")
    except Exception as e:
        print(f"  owners: texas lookup failed: {str(e)[:200]}")

    n1 = sum(1 for k in store.found if "owner" in store.found[k])
    print(f"  owners: {n1 - n0} leads with an owner/officer name")


def _group(rows, key, fn):
    out = {}
    for x in rows:
        v = fn(x)
        if v:
            out.setdefault(str(x.get(key, "")), []).append(v)
    return out


# --------------------------------------------------------------------------
# Step: OpenStreetMap
# --------------------------------------------------------------------------
OVERPASS = ["https://overpass-api.de/api/interpreter",
            "https://overpass.private.coffee/api/interpreter",
            "https://maps.mail.ru/osm/tools/overpass/api/interpreter"]


def overpass_state(store, state):
    r = store.con.execute("SELECT payload, fetched_at FROM osm_cache WHERE state=?", (state,)).fetchone()
    if r and (dt.datetime.fromisoformat(NOW) - dt.datetime.fromisoformat(r[1][:19])).days < 7:
        return json.loads(r[0])
    q = (f'[out:json][timeout:180];area["ISO3166-2"="US-{state}"]->.a;'
         f'(nwr(area.a)[amenity=cafe];nwr(area.a)[shop=coffee];nwr(area.a)[shop=tea];'
         f'nwr(area.a)[amenity~"^(restaurant|fast_food|ice_cream)$"][cuisine~"coffee|tea|bubble|juice|donut|bagel",i];);'
         f'out center tags;')
    last = None
    for url in OVERPASS:
        try:
            req = urllib.request.Request(url, data=urllib.parse.urlencode({"data": q}).encode(),
                                         headers={"User-Agent": L.UA})
            with urllib.request.urlopen(req, timeout=120) as resp:
                data = json.loads(resp.read().decode("utf-8"))
            keep = []
            for e in data.get("elements", []):
                t = e.get("tags", {})
                c = e.get("center") or {}
                keep.append({"id": f"{e['type']}/{e['id']}", "lat": e.get("lat", c.get("lat")),
                             "lon": e.get("lon", c.get("lon")), "tags": {
                                 k: v for k, v in t.items() if k in OSM_KEEP or k.startswith("addr:")}})
            store.con.execute("INSERT OR REPLACE INTO osm_cache VALUES (?,?,?)", (state, json.dumps(keep), NOW))
            store.con.commit()
            return keep
        except Exception as e:
            last = e
            time.sleep(5)
    raise RuntimeError(f"overpass failed for {state}: {last}")


OSM_KEEP = {"name", "brand", "website", "contact:website", "url", "contact:instagram", "instagram",
            "contact:facebook", "facebook", "contact:tiktok", "phone", "contact:phone", "email",
            "contact:email", "opening_hours", "amenity", "shop", "cuisine", "operator"}


def step_osm(store):
    states = sorted({l["state"] for l in store.leads.values() if re.fullmatch(r"[A-Z]{2}", l["state"] or "")})
    matched = 0
    t_end = time.time() + float(os.environ.get("OSM_BUDGET_S", "600"))
    for st in states:
        if time.time() > t_end:
            print(f"  osm: time budget used; {st} and later states wait for the next run")
            break
        try:
            pois = overpass_state(store, st)
        except Exception as e:
            print(f"  osm: {st} failed: {str(e)[:160]}")
            continue
        by_key = {}
        for p in pois:
            t = p["tags"]
            if t.get("addr:housenumber") and t.get("addr:street"):
                key = L.lead_key("", f"{t['addr:housenumber']} {t['addr:street']}",
                                 (t.get("addr:postcode") or "")[:5], t.get("addr:city", ""))
                by_key.setdefault(key, []).append(p)
        named = [p for p in pois if p["tags"].get("name")]
        for k, l in store.leads.items():
            if l["state"] != st:
                continue
            names = [l["name"]] + [n for n in (l.get("alt_names") or "").split("; ") if n]
            best, best_s = None, 0
            # same address (lead_key) + some name overlap
            for p in by_key.get(k, []):
                s = max(sim(n, p["tags"].get("name", "")) for n in names)
                if s >= 0.34 and s > best_s:
                    best, best_s = p, s + 1
            # nearby + similar name
            la, lo = fnum(l.get("lat")), fnum(l.get("lon"))
            if not best and la and lo:
                for p in named:
                    if p["lat"] is None or abs(p["lat"] - la) > 0.003 or abs(p["lon"] - lo) > 0.004:
                        continue
                    d = haversine_m(la, lo, p["lat"], p["lon"])
                    s = max(sim(n, p["tags"]["name"]) for n in names)
                    if (d < 150 and s >= 0.5) or (d < 50 and s >= 0.34):
                        if s > best_s:
                            best, best_s = p, s
            # same city/zip + near-identical distinctive name (state registrations w/o coords)
            if not best and l.get("zip") or not best and l.get("city"):
                for p in named:
                    t = p["tags"]
                    same_place = ((l.get("zip") and (t.get("addr:postcode") or "")[:5] == l["zip"]) or
                                  (l.get("city") and t.get("addr:city", "").lower() == (l["city"] or "").lower()))
                    if not same_place:
                        continue
                    dn = distinctive(l["name"])
                    if not dn:
                        continue
                    s = max(sim(n, t["name"]) for n in names)
                    if s >= 0.8 and set(dn) <= set(tokens(t["name"])):
                        if s > best_s:
                            best, best_s = p, s
            if not best:
                continue
            matched += 1
            t = best["tags"]
            store.row(k)["osm_id"] = best["id"]
            src = "openstreetmap"
            web = t.get("website") or t.get("contact:website") or t.get("url")
            if web and not re.search(r"instagram\.com|facebook\.com", web):
                store.add(k, "website", web, src, 40)
            ig = norm_ig(t.get("contact:instagram") or t.get("instagram") or
                         (web if web and "instagram.com" in web else ""))
            if ig:
                store.add(k, "instagram", ig, src, 70, "high")
            fb = t.get("contact:facebook") or t.get("facebook") or (web if web and "facebook.com" in web else "")
            if fb:
                store.add(k, "facebook", fb if fb.startswith("http") else f"https://www.facebook.com/{fb}", src, 40)
            if t.get("contact:tiktok"):
                store.add(k, "tiktok", t["contact:tiktok"], src, 40)
            ph = t.get("phone") or t.get("contact:phone")
            if ph:
                store.add(k, "osm_phone", ph, src, 40)
            em = t.get("email") or t.get("contact:email")
            if em:
                store.add(k, "email", em.lower(), src, 45)
            if t.get("opening_hours"):
                store.add(k, "hours", t["opening_hours"], src, 40)
        time.sleep(2)  # be polite to the public Overpass servers
    print(f"  osm: {matched} leads matched to OpenStreetMap places in {len(states)} states")


# --------------------------------------------------------------------------
# Step: web search (Brave Search API, optional)
# --------------------------------------------------------------------------
DIRECTORY = re.compile(
    r"yelp\.|facebook\.|instagram\.|tiktok\.|twitter\.|x\.com|linkedin\.|tripadvisor\.|mapquest\.|"
    r"bizapedia\.|opencorporates\.|bbb\.org|yellowpages\.|manta\.|dnb\.com|zoominfo\.|google\.|apple\.com|"
    r"doordash\.|ubereats\.|grubhub\.|postmates\.|seamless\.|toasttab\.com/local|restaurantji|sirved|"
    r"menupix|allmenus|wikipedia\.|reddit\.|nextdoor\.|foursquare\.|chamberofcommerce|corporationwiki|"
    r"sos\.|\.gov\b|buzzfile|opengovus|bisprofiles|northdata|indeed\.|glassdoor\.|eater\.com|"
    r"timeout\.com|secretnyc|patch\.com|youtube\.|pinterest\.|threads\.net|beanhunter|wanderlog|"
    r"top-rated\.online|cylex|hotfrog|loc8nearme|zaubee|waze\.|roadtrippers|ezcater|squareup\.com/directory|"
    r"order\.online|clover\.com|tryotter|slice|menu\.|bringmethat|dripos", re.I)


def brave(q, key):
    url = "https://api.search.brave.com/res/v1/web/search?" + urllib.parse.urlencode(
        {"q": q, "count": 10, "country": "us", "safesearch": "moderate"})
    req = urllib.request.Request(url, headers={"Accept": "application/json", "X-Subscription-Token": key,
                                               "User-Agent": L.UA})
    with urllib.request.urlopen(req, timeout=20) as r:
        data = json.loads(r.read().decode("utf-8"))
    return [(x.get("url", ""), html.unescape(re.sub("<[^>]+>", "", x.get("title", ""))),
             html.unescape(re.sub("<[^>]+>", "", x.get("description", ""))))
            for x in (data.get("web") or {}).get("results", [])]


CITY_ALIASES = {"new york": ["nyc", "ny", "newyork", "manhattan", "brooklyn", "queens", "bronx"],
                "manhattan": ["nyc", "newyork", "new york"], "brooklyn": ["nyc", "bk", "bklyn"],
                "queens": ["nyc", "astoria", "lic"], "chicago": ["chi", "chitown"],
                "los angeles": ["la", "losangeles"], "austin": ["atx"], "denver": ["den"]}


def pick_instagram(igs, name, city, state):
    """igs: [(url, title)] from a search. Returns (handle, conf) or None.
    Search engines return lots of same-name shops in other cities, so a handle only
    counts as 'high' when the name matches AND the city shows up in the title or
    handle (Instagram titles look like 'thrdcoffeeco · Chicago, IL'). A lone
    name match with no location is 'medium'; several with no location = skip."""
    city = (city or "").lower().strip()
    places = [city] + CITY_ALIASES.get(city, []) if city else []
    scored = []
    for u, t in igs:
        h = norm_ig(u)
        if not h:
            continue
        conf = handle_matches(h, name)
        title = (t or "").split("•")[0]
        if conf == "low" and sim(re.sub(r"\(@.*?\)|·.*$", "", title), name) >= 0.6:
            conf = "medium"
        if conf == "low":
            continue
        tl, hl = title.lower(), h.replace("_", "").replace(".", "")
        loc_hit = any(p and (re.search(r"\b" + re.escape(p) + r"\b", tl) or (len(p) >= 3 and p.replace(" ", "") in hl))
                      for p in places)
        loc_other = bool(re.search(r"·\s*[A-Za-z .]+", title)) and not loc_hit  # title names another city
        if loc_other:
            continue
        scored.append((h, conf, loc_hit))
    if not scored:
        return None
    if city and city in (name or "").lower():
        scored = [x for x in scored if x[2]]  # "Sapo Coffee House Chicago": city must be confirmed
        if not scored:
            return None
    located = [x for x in scored if x[2]]
    if located:
        located.sort(key=lambda x: x[1] != "high")
        return located[0][0], "high" if located[0][1] == "high" else "medium"
    if len(scored) == 1 or (scored[0][1] == "high" and all(x[1] != "high" for x in scored[1:])):
        return scored[0][0], "medium"
    return None


def clean_query_name(name):
    n = re.sub(r"\b(llc|l\.l\.c\.?|inc\.?|corp\.?|corporation|holdings?|ltd\.?|co\.)\s*$", "", name or "", flags=re.I)
    return re.sub(r"[,\s]+$", "", n).strip()


def step_search(store, limit):
    key = os.environ.get("BRAVE_API_KEY", "").strip()
    if not key:
        print("  search: skipped (set BRAVE_API_KEY to enable; free at brave.com/search/api)")
        return
    todo = []
    for k, l in store.leads.items():
        row = store.row(k)
        have_ig = row.get("instagram") or "instagram" in store.found.get(k, {})
        if l["chain"] or have_ig or row.get("search_checked"):
            continue
        if not distinctive(l["name"]):
            continue  # "Coffee Shop LLC" can't be searched meaningfully
        todo.append(l)
    todo.sort(key=lambda l: (-int(l["score"] or 0), l["first_event_date"] or ""), reverse=False)
    todo = todo[:limit]
    found_ig = found_web = 0
    for l in todo:
        k = l["lead_key"]
        name = clean_query_name(l["name"])
        place = " ".join(filter(None, [l.get("city"), l.get("state")]))
        try:
            # Instagram-only search first: its result titles carry the profile's city.
            igres = brave(f'site:instagram.com {name} {place}', key)
            time.sleep(1.1)
            igs = [(u, t) for u, t, _ in igres if "instagram.com/" in u]
            results = brave(f'"{name}" {place} coffee', key)
            time.sleep(1.1)
            igs += [(u, t) for u, t, _ in results if "instagram.com/" in u]
        except urllib.error.HTTPError as e:
            if e.code == 429:
                print("  search: rate limited, stopping for today")
                break
            print(f"  search: {name}: HTTP {e.code}")
            continue
        except Exception as e:
            print(f"  search: {name}: {str(e)[:120]}")
            continue
        note = []
        if not store.row(k).get("web_owner"):  # owner mentions in the same results, no extra search
            hit = owner_from_results([(u, t, d) for u, t, d in igres + results], l["name"], l.get("city"))
            if hit:
                remember_owner(store, k, hit[0], hit[1], hit[2], "web_search")
                note.append("owner:" + hit[0])
        best = pick_instagram(igs, name, l.get("city"), l.get("state"))
        if best:
            store.add(k, "instagram", best[0], "web_search", 60 if best[1] == "high" else 45, best[1])
            note.append("ig:" + best[0])
            found_ig += 1
        dn = distinctive(name)
        for u, t, d in results:
            host = urllib.parse.urlparse(u).netloc.lower().removeprefix("www.")
            if not host or DIRECTORY.search(host + "/" + urllib.parse.urlparse(u).path[:20]):
                continue
            squash = re.sub(r"[^a-z0-9]", "", host.split(".")[0])
            hit = sum(1 for x in dn if len(x) >= 3 and x in squash)
            if hit and hit >= max(1, len([x for x in dn if len(x) >= 3]) - 1):
                store.add(k, "website", f"https://{host}/", "web_search", 35)
                note.append("web:" + host)
                found_web += 1
                break
        row = store.row(k)
        row["search_checked"] = NOW
        row["search_note"] = " ".join(note)[:200]
    print(f"  search: {len(todo)} leads searched, {found_ig} Instagram, {found_web} websites")


# --------------------------------------------------------------------------
# Step: website scrape
# --------------------------------------------------------------------------
EMAIL_RE = re.compile(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}")
BAD_EMAIL = re.compile(r"\.(png|jpe?g|gif|svg|webp|css|js)$|sentry|wixpress|example\.|domain\.com|"
                       r"godaddy|squarespace|shopify|yourname|email@|user@|name@|@sentry|core-js|"
                       r"\d+x\.|@2x|webflow|cloudflare", re.I)


def scrape(url):
    final, page = http_get(url, timeout=12)
    out = {"final": final}
    igs = [norm_ig(m) for m in re.findall(r"instagram\.com/[A-Za-z0-9_.]{1,30}", page)]
    igs = [h for h in igs if h]
    if igs:
        out["instagram"] = max(set(igs), key=igs.count)
    fbs = [m for m in re.findall(r"facebook\.com/([A-Za-z0-9_.\-]{2,80})", page) if m.lower() not in FB_RESERVED]
    if fbs:
        out["facebook"] = "https://www.facebook.com/" + max(set(fbs), key=fbs.count)
    tts = re.findall(r"tiktok\.com/@([A-Za-z0-9_.]{2,30})", page)
    if tts:
        out["tiktok"] = "@" + max(set(tts), key=tts.count)
    mails = [m for m in re.findall(r"mailto:([^\"'?>\s]+)", page)] + EMAIL_RE.findall(page)
    mails = [urllib.parse.unquote(m).lower().strip(".") for m in mails]
    mails = [m for m in mails if EMAIL_RE.fullmatch(m) and not BAD_EMAIL.search(m)]
    if mails:
        host = urllib.parse.urlparse(final).netloc.lower().removeprefix("www.")
        mails.sort(key=lambda m: (not m.endswith("@" + host), mails.count(m) * -1))
        out["email"] = mails[0]
    tels = re.findall(r"tel:\+?1?[\s.\-()]*(\d{3})[\s.\-()]*(\d{3})[\s.\-]*(\d{4})", page)
    if tels:
        out["phone"] = "".join(tels[0])
    out["text"] = page_text(page)
    # Links to an About / Our Story / Team page on the same site (for owner names)
    base = urllib.parse.urlparse(final)
    abouts = []
    for href in re.findall(r"href=[\"']([^\"'#]+)[\"']", page):
        u = urllib.parse.urljoin(final, href)
        pu = urllib.parse.urlparse(u)
        if pu.netloc.removeprefix("www.") != base.netloc.removeprefix("www.") or u in abouts:
            continue
        if ABOUT_PATH.search(pu.path):
            abouts.append(u)
    out["about"] = abouts[:2] or [urllib.parse.urljoin(final, "/about")]
    return out


ABOUT_PATH = re.compile(r"/(about|our-?story|story|team|meet|who-we-are|our-people|founders?|owners?)", re.I)


def page_text(page):
    page = re.sub(r"(?is)<(script|style|noscript|svg)[^>]*>.*?</\1>", " ", page)
    page = re.sub(r"(?i)<br\s*/?>|</(p|div|h\d|li|section)>", ". ", page)
    return html.unescape(re.sub(r"<[^>]+>", " ", page))


def step_web(store, limit):
    todo = []
    for k in store.leads:
        row = store.row(k)
        cands = store.found.get(k, {}).get("website") or []
        site = (sorted(cands, key=lambda c: -c[0])[0][1] if cands else None) or row.get("website")
        if not site:
            continue
        last = row.get("web_checked")
        if last and (dt.datetime.fromisoformat(NOW) - dt.datetime.fromisoformat(last[:19])).days < 30:
            continue
        if not site.startswith("http"):
            site = "https://" + site
        todo.append((k, site))
    todo = todo[:limit]
    ok = owners = 0

    def work(item):
        k, site = item
        try:
            res = scrape(site)
        except Exception as e:
            return k, site, None, str(e)[:100]
        # Owner names: home page first, then the About / Our Story page.
        lead = store.leads[k]
        res["owner"] = None
        pages = [(res["final"], res.get("text", ""))]
        for u in res.get("about", []):
            try:
                fu, pg = http_get(u, timeout=10)
                pages.append((fu, page_text(pg)))
            except Exception:
                pass
        for url, text in pages:
            found = extract_owners(text, lead["name"], lead.get("city"))
            if found:
                res["owner"] = (found[0][0], found[0][1], url)
                break
        return k, site, res, None

    with cf.ThreadPoolExecutor(8) as ex:
        for k, site, res, err in ex.map(work, todo):
            row = store.row(k)
            row["web_checked"] = NOW
            if not res:
                continue
            ok += 1
            name = store.leads[k]["name"]
            if res.get("instagram"):
                conf = handle_matches(res["instagram"], name)
                store.add(k, "instagram", res["instagram"], "website", 65, "high" if conf != "low" else "medium")
            for f in ("facebook", "tiktok", "email"):
                if res.get(f):
                    store.add(k, f, res[f], "website", 50)
            if res.get("phone"):
                store.add(k, "osm_phone", res["phone"], "website", 30)
            if res.get("owner"):
                remember_owner(store, k, *res["owner"], "website")
                owners += 1
    print(f"  web: {len(todo)} websites checked, {ok} reachable, {owners} owner names found")


# --------------------------------------------------------------------------
# Owner names found online (website About pages, web search, LinkedIn titles)
# --------------------------------------------------------------------------
# Public filings often name a registered agent or a lawyer. These steps look for
# the person the shop itself (or the press) calls its owner or founder:
#   - the shop's website: home page plus its About / Our Story / Team page
#   - Instagram / web search results already fetched by the search step (free)
#   - step_people: one extra Brave search per lead, '"Shop" City owner OR founder',
#     reading news snippets and LinkedIn result titles ("Jane Doe - Owner - Shop")
# Every name found keeps a link to where it was found, so it can be checked.
# Instagram and LinkedIn pages themselves are never fetched.
_NM = r"[A-Z][a-z]+(?:[-'’][A-Z]?[a-z]+)?"
NAME_RE = rf"({_NM}(?: (?:[A-Z]\.|{_NM})){{1,2}})"
ROLE_RE = r"((?i:co-?owners?|owner[- ]operators?|owners?|co-?founders?|founders?|proprietors?))"
OWNER_PATTERNS = [
    # "Owner Jane Doe", "Founder: Jane Doe", "co-owners Jane Doe and..."
    (re.compile(rf"\b{ROLE_RE}\s*(?:[,:–—-]\s*)?{NAME_RE}"), 2, 1),
    # "Jane Doe, owner of", "Jane Doe, the founder"
    (re.compile(rf"{NAME_RE}\s*,\s*(?:the\s+|our\s+)?{ROLE_RE}\b"), 1, 2),
    # "Jane Doe is the owner"
    (re.compile(rf"{NAME_RE}\s+(?:is|was|are)\s+(?:the\s+)?(?:proud\s+)?{ROLE_RE}\b"), 1, 2),
    # "founded by Jane Doe", "owned and operated by Jane Doe"
    (re.compile(rf"\b(?i:(founded|owned|opened|started|launched)(?: and operated)?\s+by)\s+{NAME_RE}"), 2, 1),
]
# LinkedIn search result titles: "Jane Doe - Owner - Blue Door Coffee | LinkedIn"
LINKEDIN_TITLE = re.compile(rf"^{NAME_RE}\s*[-–|]\s*(?:[^-–|]*?\b)?{ROLE_RE}\b")
NOT_NAME = GENERIC | {
    "street", "st", "avenue", "ave", "road", "rd", "instagram", "facebook", "linkedin", "tiktok", "our",
    "we", "meet", "about", "team", "story", "menu", "hours", "contact", "home", "store", "owner",
    "owners", "founder", "founders", "barista", "chef", "manager", "city", "north", "south", "east",
    "west", "new", "york", "san", "los", "saint", "mount", "lake", "park", "center", "market",
    "brewing", "latte", "donut", "pizza", "grill", "family", "local", "best", "open", "opening", "now",
    "coming", "soon", "grand", "welcome", "follow", "us", "all", "rights", "reserved", "copyright",
    "privacy", "policy", "terms", "order", "online", "gift", "card", "catering", "events", "jobs",
    "careers", "location", "locations", "visit", "find", "monday", "tuesday", "wednesday", "thursday",
    "friday", "saturday", "sunday", "january", "february", "march", "april", "may", "june", "july",
    "august", "september", "october", "november", "december", "llc", "inc", "chief", "executive",
    "officer", "head", "general", "director", "president", "local", "news", "daily", "times", "post",
    "magazine", "journal", "eater", "business", "small", "black", "woman", "women", "veteran", "mom",
    "husband", "wife", "brothers", "sisters", "partners", "friends", "roaster", "shop", "and", "the"}
ROLE_WORD = {"coowner": "co-owner", "ownersoperator": "owner-operator", "owneroperator": "owner-operator",
             "cofounder": "co-founder", "proprietor": "owner", "proprietors": "owner"}


def clean_role(r):
    k = re.sub(r"[^a-z]", "", (r or "").lower())
    k = k[:-1] if k.endswith("s") and k not in ("proprietors",) else k
    return ROLE_WORD.get(k, k or "owner")


def extract_owners(text, shop_name="", city=""):
    """Find 'owner/founder <Name>' style mentions. Returns [(name, role)], best first."""
    text = re.sub(r"\s+", " ", html.unescape(text or ""))[:200_000]
    shop_toks = set(distinctive(shop_name))
    city_toks = set(tokens(city))
    out = []
    for pat, ni, ri in OWNER_PATTERNS:
        for m in pat.finditer(text):
            name = m.group(ni).strip()
            role = clean_role(m.group(ri))
            if role in ("founded", "owned", "opened", "started", "launched"):
                role = "owner" if role == "owned" else "founder"
            toks = [t.lower().strip(".") for t in name.split()]
            if any(t in NOT_NAME for t in toks) or set(toks) & (shop_toks | city_toks):
                continue
            if not is_person(name) or len(name) > 40:
                continue
            if name not in [n for n, _ in out]:
                out.append((name, role))
    return out


def owner_from_results(results, shop_name, city):
    """results: [(url, title, description)] from a web search. Returns (name, role, url) or None.
    Only uses results that actually mention the shop (its distinctive words)."""
    dist = [t for t in distinctive(shop_name) if len(t) >= 3]
    if not dist:
        return None
    for u, t, d in results:
        hay = " ".join(tokens(f"{t} {d} {u}"))
        hits = sum(1 for x in dist if x in hay)
        if hits < max(1, (len(dist) + 1) // 2):
            continue
        if "linkedin.com/in/" in u:
            m = LINKEDIN_TITLE.search(t or "")
            if m:
                name = m.group(1).strip()
                toks = [x.lower() for x in name.split()]
                if is_person(name) and not any(x in NOT_NAME for x in toks):
                    return name, clean_role(m.group(2)), u
        found = extract_owners(f"{t}. {d}", shop_name, city)
        if found:
            return found[0][0], found[0][1], u
    return None


def remember_owner(store, k, name, role, url, source):
    """Save an online-found owner on the lead (kept across runs) and offer it as a candidate."""
    row = store.row(k)
    row["web_owner"], row["web_owner_role"], row["web_owner_url"] = name, role, url
    row["web_owner_source"] = source
    store.add(k, "owner", name, source, 58, role)


def step_saved_owners(store):
    """Re-offer owners found online on earlier runs (those steps don't re-run every day)."""
    for k, row in store.rows.items():
        if row.get("web_owner") and k in store.leads:
            store.add(k, "owner", row["web_owner"], row.get("web_owner_source") or "website", 58,
                      row.get("web_owner_role") or "owner")


def step_people(store, limit):
    """One web search per lead for its owner/founder: news, local blogs, LinkedIn titles."""
    key = os.environ.get("BRAVE_API_KEY", "").strip()
    if not key:
        print("  people: skipped (set BRAVE_API_KEY to enable)")
        return
    todo = []
    for k, l in store.leads.items():
        row = store.row(k)
        if l["chain"] or row.get("web_owner") or not distinctive(l["name"]):
            continue
        last = row.get("owner_checked")
        if last and (dt.datetime.fromisoformat(NOW) - dt.datetime.fromisoformat(last[:19])).days < 60:
            continue
        todo.append(l)
    todo.sort(key=lambda l: -int(l["score"] or 0))
    todo = todo[:limit]
    found = 0
    for l in todo:
        k = l["lead_key"]
        name = clean_query_name(l["name"])
        place = " ".join(filter(None, [l.get("city"), l.get("state")]))
        try:
            results = brave(f'"{name}" {place} owner OR founder', key)
            time.sleep(1.1)
        except urllib.error.HTTPError as e:
            if e.code == 429:
                print("  people: rate limited, stopping for today")
                break
            print(f"  people: {name}: HTTP {e.code}")
            continue
        except Exception as e:
            print(f"  people: {name}: {str(e)[:120]}")
            continue
        store.row(k)["owner_checked"] = NOW
        hit = owner_from_results(results, l["name"], l.get("city"))
        if hit:
            remember_owner(store, k, hit[0], hit[1], hit[2], "owner_search")
            found += 1
    print(f"  people: {len(todo)} leads searched, {found} owner names found")


# --------------------------------------------------------------------------
# Step: manual overrides
# --------------------------------------------------------------------------
def step_manual(store):
    if not os.path.exists(MANUAL_FILE):
        return
    with open(MANUAL_FILE) as fh:
        data = json.load(fh)
    n = 0
    for k, vals in data.items():
        if k.startswith("_") or k not in store.leads:
            continue
        n += 1
        src = vals.get("source", "manual")
        for f, v in vals.items():
            if f == "instagram":
                h = norm_ig(v)
                if h:
                    store.add(k, "instagram", h, src, 100, vals.get("instagram_conf", "verified"))
            elif f == "owner":
                store.add(k, "owner", v, src, 100, vals.get("owner_role", "owner"))
            elif f in FIELDS and f not in ("instagram_conf", "owner_role"):
                store.add(k, f, v, src, 100)
    print(f"  manual: {n} hand-checked leads applied")


# --------------------------------------------------------------------------
# Export
# --------------------------------------------------------------------------
def export_csv(con, path=L.CSV_FILE):
    ecols = ["owner", "owner_role", "owners_all", "web_owner", "web_owner_role", "web_owner_url", "email", "website", "instagram", "instagram_conf",
             "facebook", "tiktok", "osm_phone", "hours", "shop_address", "enrich_sources"]
    cur = con.execute(f"""SELECT l.name, l.alt_names, l.address, l.city, l.state, l.zip, l.phone,
        {", ".join("e." + c for c in ecols)},
        l.first_event_date, l.last_event_date, l.open_status, l.open_est_start, l.open_est_end, l.open_conf,
        l.signals, l.event_types, l.sources, l.record_count,
        l.match_level, l.chain, l.score, l.lat, l.lon, l.first_seen, l.status, l.lead_key
        FROM leads l LEFT JOIN enrichment e USING (lead_key)
        ORDER BY l.score DESC, l.first_event_date DESC""")
    with open(path, "w", newline="") as fh:
        w = csv.writer(fh)
        w.writerow([d[0] for d in cur.description])
        w.writerows(cur)


def cmd_stats(con):
    con.executescript(SCHEMA)
    q = """SELECT count(*), sum(owner<>''), sum(instagram<>''), sum(website<>''), sum(email<>''),
           sum(facebook<>''), sum(hours<>''), sum(shop_address<>'')
           FROM leads l LEFT JOIN enrichment e USING (lead_key) WHERE l.chain=''"""
    names = ["independent leads", "owner", "instagram", "website", "email", "facebook", "hours", "TX shop address"]
    for n, v in zip(names, con.execute(q).fetchone()):
        print(f"  {n:<18} {v or 0}")
    for r in con.execute("""SELECT owner_source, count(*) FROM enrichment WHERE owner<>'' GROUP BY 1 ORDER BY 2 DESC"""):
        print("   owner from", r)


def main():
    p = argparse.ArgumentParser()
    p.add_argument("cmd", nargs="?", default="run", choices=["run", "stats"])
    p.add_argument("--steps", default="owners,osm,search,web,people,manual")
    p.add_argument("--search-limit", type=int, default=int(os.environ.get("SEARCH_LIMIT", "45")))
    p.add_argument("--web-limit", type=int, default=300)
    p.add_argument("--owner-search-limit", type=int, default=int(os.environ.get("OWNER_SEARCH_LIMIT", "8")))
    a = p.parse_args()
    con = sqlite3.connect(L.DB_FILE)
    if a.cmd == "stats":
        return cmd_stats(con)
    store = Store(con)
    steps = a.steps.split(",")
    sources = L.load_sources()
    t0 = time.time()
    if "owners" in steps:
        step_owners(store, sources)
        store.resolve(); store.save()
    if "osm" in steps:
        step_osm(store)
        store.resolve(); store.save()
    if "manual" in steps:
        step_manual(store)  # before search, so hand-checked leads aren't searched
    if "search" in steps:
        store.resolve()
        step_search(store, a.search_limit)
    if "web" in steps:
        step_web(store, a.web_limit)
    if "people" in steps:
        step_people(store, a.owner_search_limit)
    step_saved_owners(store)
    store.resolve()
    store.save()
    export_csv(con)
    print(f"enrichment done in {time.time() - t0:.0f}s")
    cmd_stats(con)


if __name__ == "__main__":
    main()
