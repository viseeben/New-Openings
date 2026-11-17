#!/usr/bin/env python3
"""
Find coffee shops announcing they're "coming soon" online, before they show up in any
government dataset: Instagram and Facebook posts/pages, and local news coverage
(What Now, Eater, Block Club, neighborhood blogs...).

How it works: for each city in discovery.json it runs three Brave searches (needs
BRAVE_API_KEY), reads only the search results (titles and snippets; Instagram and Facebook
pages themselves are never fetched), and keeps results that

  - say "coming soon", "opening soon", "set to open", "now hiring", etc.,
  - are about a coffee / tea / matcha / boba business (name or snippet), and
  - aren't a listicle ("10 new cafes...") or a chain.

From each it takes the shop's name, the Instagram / Facebook / article link, a street address
if one is mentioned, an owner or founder if one is named, and an opening date hint
("opening in December"). Each becomes a record with signal "coming_soon"; leads.py merges it
with the shop's permit or license record when that arrives (same address, or same business
name in the same state).

Runs at most once a week (it checks when it last ran). Cost: 3 searches per city per week,
about 450 a month for the default 36 cities.

    python3 discover.py            # weekly run (skips if run in the last 6 days)
    python3 discover.py --force    # run now
    python3 discover.py --dry-run  # print what it would add, write nothing
"""
import argparse
import datetime as dt
import hashlib
import math
import json
import os
import re
import sqlite3
import sys
import time
import urllib.error
import urllib.parse
import urllib.request

import leads as L
import enrich as E
import budget as B

HERE = os.path.dirname(os.path.abspath(__file__))
CONFIG = os.path.join(HERE, "discovery.json")
RUN_ID = "web_discovery"

COMING = re.compile(r"coming soon|opening soon|soft[- ]open|grand opening|set to open|plans? to open|"
                    r"will open|to open (?:in|on|this|next|later|soon)|opening (?:this|next|in|on|day|date)|"
                    r"now hiring|is opening|readies|debuts?\b|lease signed|signed a lease|under construction|"
                    r"(?:preparing|getting ready|expected|slated|hop(?:es|ing)|aim(?:s|ing)|set|planning|gearing up) to open|"
                    r"set to debut|forthcoming|in the works|first brick-and-mortar|brick-and-mortar|coming to|heading to|"
                    r"opening date|expanding (?:to|into)|new location", re.I)
COFFEE = re.compile(r"\b(coffee|espresso|caf[eé]|caffe|kafe|koffee|matcha|boba|bubble tea|roaster[sy]?|latte|tea house|"
                    r"teahouse|kaffe|coffeehouse)\b", re.I)
LISTICLE = re.compile(r"\b(best|top \d+|\d+ (?:new|hot|great|best)|guide|where to|openings|closings|"
                      r"round-?up|restaurants|things to do|what's new|this week|this month)\b", re.I)
JUNK_NAME = re.compile(r"^(instagram|facebook|home|photos?|videos?|posts?|reels?|coming soon|new|the)$", re.I)
MONTHS = r"(?:jan|feb|mar|apr|may|jun|jul|aug|sep|sept|oct|nov|dec)[a-z]*\.?"
OPEN_HINT = re.compile(rf"(?:open(?:s|ing)?|coming|debut(?:s|ing)?|launch(?:es|ing)?)\s+(?:on|in|by|before|this|early|late|mid-?)?\s*"
                       rf"((?:early |late |mid-?)?(?:{MONTHS}(?:\s+\d{{1,2}}(?:st|nd|rd|th)?)?(?:,?\s+20\d\d)?|"
                       rf"(?:this|next) (?:week|month|spring|summer|fall|winter)|spring|summer|fall|winter))", re.I)
ADDRESS = re.compile(r"\b(\d{2,5}\s+(?:[NSEW]\.?\s+)?(?:[A-Z0-9][\w'.]*\s+){1,3}"
                     r"(?:St|Street|Ave|Avenue|Rd|Road|Blvd|Boulevard|Dr|Drive|Ln|Lane|Way|Pkwy|Parkway|Pl|Place|"
                     r"Ct|Court|Hwy|Highway|Sq|Square|Ter|Terrace|Cir|Circle)\b\.?)")

DEFAULT_CITIES = [
    ["New York", "NY"], ["Brooklyn", "NY"], ["Queens", "NY"], ["Chicago", "IL"], ["Los Angeles", "CA"],
    ["San Francisco", "CA"], ["San Diego", "CA"], ["Seattle", "WA"], ["Portland", "OR"], ["Denver", "CO"],
    ["Austin", "TX"], ["Houston", "TX"], ["Dallas", "TX"], ["San Antonio", "TX"], ["Phoenix", "AZ"],
    ["Atlanta", "GA"], ["Nashville", "TN"], ["Charlotte", "NC"], ["Raleigh", "NC"], ["Durham", "NC"],
    ["Miami", "FL"], ["Orlando", "FL"], ["Tampa", "FL"], ["Philadelphia", "PA"], ["Pittsburgh", "PA"],
    ["Boston", "MA"], ["Washington", "DC"], ["Baltimore", "MD"], ["Minneapolis", "MN"], ["Kansas City", "MO"],
    ["St. Louis", "MO"], ["New Orleans", "LA"], ["Columbus", "OH"], ["Cincinnati", "OH"], ["Salt Lake City", "UT"],
    ["Boise", "ID"],
]


# name -> (parser, query, freshness: pd/pw/pm/py = past day/week/month/year)
SEARCHES = {
    "news": ("news", 'new coffee shop "coming soon" OR "set to open" OR "opening soon" {city}', "pm"),
    "whatnow": ("news", 'site:whatnow.com coffee {city}', "pm"),
    "instagram": ("instagram", 'site:instagram.com "coming soon" coffee {city}', "py"),
    "facebook": ("facebook", 'site:facebook.com "coming soon" coffee shop {city}', "py"),
    "tiktok": ("tiktok", 'site:tiktok.com coffee shop "coming soon" {city}', "py"),
}


def load_config():
    try:
        cfg = json.load(open(CONFIG))
    except Exception:
        cfg = {}
    cfg.setdefault("cities", DEFAULT_CITIES)
    cfg.setdefault("every_days", 7)
    cfg.setdefault("searches", ["news", "whatnow", "instagram", "facebook"])
    return cfg


def brave(q, key, freshness=None):
    params = {"q": q, "count": 20, "country": "us", "safesearch": "moderate"}
    if freshness:
        params["freshness"] = freshness
    req = urllib.request.Request("https://api.search.brave.com/res/v1/web/search?" + urllib.parse.urlencode(params),
                                 headers={"Accept": "application/json", "X-Subscription-Token": key, "User-Agent": L.UA})
    with urllib.request.urlopen(req, timeout=20) as r:
        data = json.loads(r.read().decode("utf-8"))
    clean = lambda v: E.html.unescape(re.sub("<[^>]+>", "", v or ""))
    return [{"url": x.get("url", ""), "title": clean(x.get("title")), "desc": clean(x.get("description")),
             "age": x.get("page_age") or ""} for x in (data.get("web") or {}).get("results", [])]


HEAD_VERB = (r"to open|opening|is opening|will open|set to open|sets|coming|plans|planned|brings|bringing|debuts?|"
             r"debuting|lands|opens|readies|signs|eyes|heads|heading|expands|expanding|takes over|launch(?:es|ing)?|"
             r"joins|joining|targets|arrives|arriving|brews up|brewing|preparing|gears up|nears|nearing|slated|returns")
LEAD_JUNK = re.compile(r"^(?:(?:new|local|beloved|popular|upcoming|viral|family-owned|black-owned|woman-owned|"
                       r"[\w]+-(?:inspired|based|style|owned)|the)\s+)+", re.I)
COFFEE_WORD = r"(?:Coffee|Café|Cafe|Caffe|Kafe|Koffee|Espresso|Roasters?|Roastery|Roasting|Tea|Teahouse|Boba|Matcha|Coffeehouse|Kaffe)"
GENERIC = re.compile(rf"^(?:(?:a|an|the|new|cuban|coffee|shop|specialty|local|independent|drive-thru|neighborhood|"
                     rf"cafe|café|tea|boba|bar|house|spot|concept|chain|brand|first|second|third)\s*)+$", re.I)
SENT_PATTERNS = [
    r"(?i:called|named|dubbed)\s+([A-Z][\w'’&.\-]*(?:\s+(?:&|and|[A-Z0-9][\w'’&.\-]*)){0,4})",
    r"([A-Z][\w'’&.\-]*(?:\s+(?:&|and|of|the|[A-Z0-9][\w'’&.\-]*)){0,4}),?\s+(?:is|are)\s+(?:preparing|set|setting up|getting ready|"
    r"planning|expected|slated|gearing up|expanding|heading|coming|opening|launching|bringing|making plans)",
    r"([A-Z][\w'’&.\-]*(?:\s+(?:&|and|[A-Z0-9][\w'’&.\-]*)){0,4})\s+will\s+(?:open|be opening|join|debut|soon open)",
    rf"([A-Z][\w'’.\-]*(?:\s+[A-Z][\w'’.\-]*){{0,3}}\s+{COFFEE_WORD}(?:\s+(?:&\s+[A-Z][\w'’]*|and\s+[A-Z][\w'’]*|Co\.?(?!\w)|Company|House|Bar|Lab|Shop|Kitchen|Collective))*)",
    rf"([A-Z][\w'’.\-]*\s+{COFFEE_WORD})",
]


MONTH_YEAR = re.compile(rf"\b((?:early |late |mid-?)?{MONTHS}\s+20\d\d)\b(?!\s*[,]?\s*\d)", re.I)
PERSON = r"([A-Z][a-z]+(?:\s+[A-Z]\.)?(?:\s+[A-Z][a-z'’\-]+){1,2})"
OWNER_PATTERNS = [
    (rf"(?:founded|owned|run|started|launched|opened)\s+by\s+(?:[\w\-]+\s+){{0,4}}?{PERSON}", "founder"),
    (rf"\b(?:[Cc]o-)?(?:[Ff]ounder|[Oo]wner|[Pp]roprietor)s?(?:\s+and\s+[\w\- ]{{0,30}}?)?,?\s+{PERSON}", "owner"),
    (rf"{PERSON},\s+(?:the\s+)?(?:co-)?(?:owner|founder)\b", "owner"),
    (r"\b(?:[Oo]wners|[Ff]ounders|couple)\s+([A-Z][a-z]+)\s+and\s+[A-Z][a-z]+\s+([A-Z][a-z'’\-]+)", "owner"),
]


def article_owners(text):
    """Owner / founder named in a news snippet: 'founded by ... Emma Layman', 'owners Bryan and Reghan Haven'."""
    out = []
    for pat, role in OWNER_PATTERNS:
        for m in re.finditer(pat, text):
            name = " ".join(m.groups()) if len(m.groups()) > 1 else m.group(1)
            name = re.sub(r"\s+(?:told|says|said|is|was|has|got|and|who|of)$", "", name.strip())
            if E.is_person(name) and not re.search(COFFEE_WORD, name, re.I) and not re.search(r"\b(?:What Now|Instagram|Facebook)\b", name):
                out.append((name, role))
    return out


def clean_name(n):
    n = re.sub(r"\s+", " ", (n or "").strip(" ,.:;'\"“”‘’-–"))
    n = LEAD_JUNK.sub("", n).strip()
    n = re.sub(r"^(?:coffee shop|cafe|café|coffee brand|coffee company|specialty coffee shop)\s+(?=[A-Z])", "", n)
    return n


def good_name(n):
    if not n or len(n) < 3 or len(n) > 60 or JUNK_NAME.match(n) or GENERIC.match(n):
        return False
    if not E.distinctive(n) or (E.is_person(n) and not re.search(COFFEE_WORD, n, re.I)):
        return False
    return not re.match(r"^(?:owner|founder|co-owner|according|the company|the brand|the shop|the cafe|the café|"
                        r"this|it|they|we|what now|axios|eater|instagram|facebook|tiktok)\b", n, re.I)


def name_from(kind, title, url, desc=""):
    """Pull the business name out of a search result."""
    t = title.strip()
    if kind == "instagram":
        m = re.match(r"^(.+?)\s+\(@[\w.]+\)", t) or re.match(r"^(.+?)\s+on Instagram\b", t)
        if m:
            return m.group(1).strip(" @")
        m = re.search(r"instagram\.com/([A-Za-z0-9_.]+)/?", url)
        return m.group(1).replace("_", " ").replace(".", " ").title() if m and m.group(1) not in E.IG_RESERVED else ""
    if kind == "facebook":
        return re.split(r"\s+[|\-–]\s+", t)[0].strip()
    if kind == "tiktok":
        m = re.match(r"^(.+?)\s+\(@[\w.]+\)", t)
        if m:
            return m.group(1).strip()
        m = re.search(r"tiktok\.com/@([A-Za-z0-9_.]+)", url)
        return m.group(1).replace("_", " ").replace(".", " ").title() if m else ""
    # News: try the headline, then the article snippet; a name found in both wins.
    cands = []
    m = re.match(rf"^(?:(?:new|local|beloved|popular|family-owned)\s+)?([A-Z0-9][\w'&.’ \-]{{2,60}}?)\s+(?:{HEAD_VERB})\b", t, re.I)
    if m:
        cands.append(clean_name(m.group(1)))
    for pat in SENT_PATTERNS:
        for mm in re.finditer(pat, desc):
            cands.append(clean_name(mm.group(1)))
    cands = [c for c in cands if good_name(c)]
    if not cands:
        return ""
    tl = t.lower()
    dl = desc.lower()
    # prefer: named in both headline and snippet, then has a coffee word, then shorter (less junk)
    cands.sort(key=lambda c: (not (c.lower() in tl and c.lower() in dl), c.lower() not in tl,
                              not re.search(COFFEE_WORD, c, re.I), len(c)))
    return cands[0]


def handle_from(url, kind):
    host = "instagram.com" if kind == "instagram" else "facebook.com"
    m = re.search(rf"{host}/([A-Za-z0-9_.\-]+)/?", url)
    if not m:
        return ""
    h = m.group(1)
    reserved = E.IG_RESERVED if kind == "instagram" else E.FB_RESERVED
    return "" if h.lower() in reserved else h


def parse(kind, res, city, state):
    text = f"{res['title']} {res['desc']}"
    if not COMING.search(text) or LISTICLE.search(res["title"]):
        return None
    name = re.sub(r"\s+", " ", name_from(kind, res["title"], res["url"], res["desc"]))[:80]
    if not name or JUNK_NAME.match(name) or len(name) < 3:
        return None
    level, chain = L.classify(name, "")
    if chain or any(p.search(name) for p in L._nonshop):
        return None
    if not level and not COFFEE.search(f"{res['title']} {res['desc']}"):
        return None  # the name, headline or snippet has to be about coffee / tea / boba / matcha
    if kind == "news":
        place = f"{res['title']} {res['desc']} {res['url']}".lower()
        if city.lower() not in place and city.lower().replace(" ", "") not in place:
            return None
    addr = (ADDRESS.search(res["desc"]) or ADDRESS.search(res["title"]))
    hint = OPEN_HINT.search(text) or MONTH_YEAR.search(text)
    owners = E.extract_owners(text, name, city) or article_owners(text)
    date = (res["age"] or "")[:10] or dt.date.today().isoformat()
    raw = {"title": res["title"][:300], "snippet": res["desc"][:600], "url": res["url"], "kind": kind,
           "opening_hint": hint.group(1).strip(" .,;") if hint else "",
           "owner": owners[0][0] if owners else "", "owner_role": owners[0][1] if owners else ""}
    if kind in ("instagram", "facebook"):
        raw[kind] = handle_from(res["url"], kind)
    if kind == "tiktok":
        m = re.search(r"tiktok\.com/@([A-Za-z0-9_.]+)", res["url"])
        raw["tiktok"] = m.group(1) if m else ""
    label = {"instagram": "Instagram", "facebook": "Facebook", "news": "News", "tiktok": "TikTok"}[kind]
    rec = {
        "source_id": f"web_{kind}", "jurisdiction": f"{city}, {state}", "signal": "coming_soon",
        "record_id": hashlib.sha1(f"{kind}|{L.norm_name(name)}|{city.lower()}".encode()).hexdigest()[:16],
        "name": name, "alt_name": "", "address": addr.group(1) if addr else "", "city": city, "state": state,
        "zip": "", "event_date": date,
        "event_type": f"{label}: coming soon" + (f" (opening {raw['opening_hint']})" if raw["opening_hint"] else ""),
        "category": "", "status": "", "phone": "", "lat": "", "lon": "",
        "match_level": level or "category", "chain": "", "url": res["url"], "raw": json.dumps(raw),
    }
    rec["lead_key"] = L.lead_key(rec["name"], rec["address"], "", city)
    return rec


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--force", action="store_true")
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--cities", help="comma-separated subset, e.g. 'Chicago,Austin'")
    a = ap.parse_args()
    key = os.environ.get("BRAVE_API_KEY", "").strip()
    if not key:
        print("discover: skipped (set BRAVE_API_KEY to enable)")
        return
    cfg = load_config()
    con = sqlite3.connect(L.DB_FILE)
    con.executescript(L.SCHEMA)
    last = con.execute("SELECT max(run_at) FROM runs WHERE source_id=?", (RUN_ID,)).fetchone()[0]
    if last and not a.force and (dt.datetime.now() - dt.datetime.fromisoformat(last[:19])).days < cfg["every_days"]:
        print(f"discover: last run {last[:10]}, next in {cfg['every_days']} days (use --force to run now)")
        return
    cities = cfg["cities"]
    # 36 searches a week in total, rotating through the cities (4 searches per city = 9 cities a week)
    n = max(1, B.DISCOVERY_PER_WEEK // max(1, len([k for k in cfg["searches"] if k in SEARCHES])))
    groups = math.ceil(len(cities) / n)
    week = dt.date.today().isocalendar()[1]
    if not a.cities:
        cities = cities[(week % groups) * n:(week % groups) * n + n]
    if a.cities:
        want = {c.strip().lower() for c in a.cities.split(",")}
        cities = [c for c in cities if c[0].lower() in want]
    found, seen, searches, tally, results_seen = [], set(), 0, {}, {}
    for city, state in cities:
        queries = [(k, SEARCHES[k][0], SEARCHES[k][1].format(city=city), SEARCHES[k][2]) for k in cfg["searches"] if k in SEARCHES]
        for skey, kind, q, fresh in queries:
            if not B.spend(con, "coming_soon"):
                print("  stopped: this month's Brave search allowance is used up")
                break
            try:
                results = brave(q, key, fresh)
                searches += 1
                time.sleep(1.1)
            except urllib.error.HTTPError as e:
                print(f"  {city} {kind}: HTTP {e.code}" + (" (rate limited, stopping)" if e.code == 429 else ""))
                if e.code == 429:
                    break
                continue
            except Exception as e:
                print(f"  {city} {kind}: {str(e)[:120]}")
                continue
            for res in results:
                rec = parse(kind, res, city, state)
                if rec and rec["record_id"] not in seen:
                    seen.add(rec["record_id"])
                    found.append(rec)
                    tally[skey] = tally.get(skey, 0) + 1
                results_seen[skey] = results_seen.get(skey, 0) + 1
    print("  " + B.report(con))
    print(f"discover: {searches} searches, {len(found)} coming-soon shops found")
    for k in results_seen:
        print(f"  {k}: {results_seen[k]} results read, {tally.get(k, 0)} kept")
    for r in found[:200]:
        print(f"  {r['jurisdiction']:<22} {r['name'][:40]:<40} {r['event_type'][:50]}")
    if a.dry_run:
        return
    now = dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds")
    cols = list(found[0].keys()) + ["first_seen"] if found else []
    for rec in found:
        rec["first_seen"] = now
        con.execute(f"INSERT INTO records ({','.join(cols)}) VALUES ({','.join('?' * len(cols))}) "
                    f"ON CONFLICT(source_id, record_id) DO UPDATE SET "
                    + ",".join(f"{c}=excluded.{c}" for c in cols if c not in ("first_seen", "source_id", "record_id")),
                    [rec[c] for c in cols])
    con.execute("INSERT INTO runs VALUES (?,?,?,?,?)", (now, RUN_ID, searches, len(found), None))
    con.commit()
    L.rebuild_leads(con)
    L.export_csv(con)


if __name__ == "__main__":
    main()
