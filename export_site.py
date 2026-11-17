#!/usr/bin/env python3
"""Write site/leads.json from coffee_leads.db for the web page."""
import datetime as dt
import json
import os
import sqlite3

HERE = os.path.dirname(os.path.abspath(__file__))
DB = os.path.join(HERE, "coffee_leads.db")
OUT = os.path.join(HERE, "_site", "leads.json")

COLS = ["lead_key", "name", "alt_names", "address", "city", "state", "zip", "phone",
        "first_event_date", "last_event_date", "open_est_start", "open_est_end", "open_status",
        "open_conf", "open_basis", "brand_sites", "lead_type", "signals", "sources", "event_types",
        "record_count", "match_level", "chain", "score", "lat", "lon", "first_seen"]
# Filled by enrich.py (owner, Instagram, website...). Blank if enrich.py hasn't run.
ECOLS = ["owner", "owner_role", "owner_source", "owners_all", "email", "website", "instagram",
         "instagram_conf", "facebook", "tiktok", "osm_phone", "hours", "shop_address", "enrich_sources",
         "web_owner", "web_owner_role", "web_owner_url", "already_open", "open_evidence",
         "replaced_business", "review_count", "places_url"]


def main():
    con = sqlite3.connect(DB)
    has_e = con.execute("SELECT 1 FROM sqlite_master WHERE name='enrichment'").fetchone()
    ecols = [c for c in ECOLS if has_e and c in [r[1] for r in con.execute("PRAGMA table_info(enrichment)")]]
    lcols = [r[1] for r in con.execute("PRAGMA table_info(leads)")]
    cols = [c for c in COLS if c in lcols]  # older databases may lack the opening columns
    sel = ",".join([f"l.{c}" for c in cols] + [f"e.{c}" for c in ecols])
    join = " LEFT JOIN enrichment e USING (lead_key)" if has_e else ""
    if has_e and "lead_type" in [r[1] for r in con.execute("PRAGMA table_info(enrichment)")]:
        sel += ", e.lead_type"
    rows = [list(r) for r in con.execute(f"SELECT {sel} FROM leads l{join}").fetchall()]
    # Apply the "already open" verdict from enrich.py: it beats the estimate from the record type.
    ix = {c: i for i, c in enumerate(cols + ecols)}
    for r in rows:
        e_type = r.pop() if len(r) > len(ix) else None
        if e_type and "lead_type" in ix and r[ix["lead_type"]] != "Chain / franchise":
            r[ix["lead_type"]] = e_type
        verdict = r[ix["already_open"]] if "already_open" in ix else ""
        if verdict == "yes" and "open_status" in ix:
            r[ix["open_status"]] = "Already open"
            r[ix["score"]] = max(0, int(r[ix["score"]] or 0) - 35)
        elif verdict == "likely":
            r[ix["score"]] = max(0, int(r[ix["score"]] or 0) - 15)
    rows.sort(key=lambda r: str(r[ix["first_event_date"]] or ""), reverse=True)  # newest first...
    rows.sort(key=lambda r: -int(r[ix["score"]] or 0))  # ...within each score
    last = con.execute("SELECT max(run_at) FROM runs").fetchone()[0]
    runs = [dict(zip(["source_id", "fetched", "matched", "error"], r)) for r in
            con.execute("SELECT source_id, fetched, matched, error FROM runs WHERE run_at=?", (last,))]
    with open(os.path.join(HERE, "sources.json")) as fh:
        srcs = {s["id"]: {"jurisdiction": s["jurisdiction"], "url": s.get("portal_url", "")}
                for s in json.load(fh)["sources"]}
    for r in runs:
        r.update(srcs.get(r["source_id"], {}))
    os.makedirs(os.path.dirname(OUT), exist_ok=True)
    with open(OUT, "w") as fh:
        json.dump({"generated_at": dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds"),
                   "last_run": last, "runs": runs, "sources": srcs,
                   "cols": cols + ecols, "rows": [["" if v is None else v for v in r] for r in rows]},
                  fh, separators=(",", ":"))
    print(f"wrote {len(rows)} leads -> {OUT}")


if __name__ == "__main__":
    main()
