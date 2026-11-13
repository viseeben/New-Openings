# Coffee shop openings: lead database for Dripos

Finds newly opening coffee shops in the US from public permit, license,
inspection and business-registration data, and writes them to a SQLite
database (`coffee_leads.db`) and a spreadsheet-friendly CSV (`coffee_leads.csv`).

Pure Python standard library (3.9+). No API keys needed.

## Run it

```bash
python3 leads.py run                 # last 365 days, all enabled sources
python3 leads.py run --days 60       # shorter window
python3 leads.py run --only nyc_prepermit_inspections,chicago_food_license_inspections
python3 leads.py stats               # counts by state / source / match type
```

Re-running is safe: records upsert on `(source_id, record_id)`, and the
`status` and `notes` you set on a lead are kept across runs. Run it weekly
(cron / launchd) and sort by `first_seen` to see what's new since last time.

If you hit Socrata rate limits, get a free app token and
`export SOCRATA_APP_TOKEN=...`.

## Live page (GitHub Pages)

`index.html` is a sortable, filterable page over the leads. A GitHub
Actions job (`.github/workflows/daily.yml`) runs every morning at 11:00 UTC:

1. restores `coffee_leads.db` from the `data` branch (so `first_seen` carries over),
2. runs `python3 leads.py run`,
3. writes `_site/leads.json` with `export_site.py`,
4. force-pushes the new database and CSV to the `data` branch (one commit, no history bloat),
5. publishes `index.html` + `leads.json` to GitHub Pages.

Run it by hand from the Actions tab ("Run workflow"). A push to `main` just
rebuilds the page from the saved data. Status and notes on the web page are
saved in your browser; "Download CSV" exports them.

GitHub pauses scheduled workflows in a repo with no activity for 60 days; if
that happens, re-enable it from the Actions tab.

## Sources (first build, Nov 2026)

| id | Where | Evidence | Why it's a good opening signal |
|---|---|---|---|
| `nyc_prepermit_inspections` | New York City | DOHMH **pre-permit** inspections | Only happens before a new food permit is issued. Best source here. Includes phone and "Coffee/Tea" cuisine tag. |
| `chicago_food_license_inspections` | Chicago | CDPH **License** inspections | Required before a new retail food license is issued. |
| `chicago_new_business_licenses` | Chicago | Business licenses, `application_type = ISSUE` | New license (not a renewal). |
| `la_new_business_tax_registrations` | Los Angeles | Active businesses by `location_start_date` | New business tax registration; NAICS tells you the type. Some dates are in the future (planned openings). |
| `austin_first_inspections` | Austin, TX | Health inspections, **first ever inspection** per facility | Austin doesn't label opening inspections, so "first time this facility shows up" stands in. |
| `cincinnati_first_inspections` | Cincinnati | Food safety inspections, first per license | Same trick. |
| `ct_business_registrations` | Connecticut | New business registrations (+ NAICS) | Earliest signal (LLC formed), noisier. |
| `colorado_business_entities` | Colorado | New business entities | Earliest signal, noisier. |
| `ny_state_new_corporations` | New York State | New DOS filings | Address is the service-of-process address, often the owner or an agent, not the shop. |
| `oregon_new_businesses` | Oregon | New registrations (rolling: last month only) | Run monthly to accumulate. |
| `texas_new_franchise_taxpayers` | Texas | New franchise taxpayers | Mailing address, often the owner. |

City health and license data mean "a shop is about to open at this address."
State registrations mean "someone just formed a coffee company," which is
earlier but needs a quick check (Google/Instagram) to find the actual location.

## How a lead is built

1. **Pull**: each source is queried server-side for the date window plus a
   name filter (coffee, espresso, cafe, roast, brew, boba, matcha, the big
   chains, etc.), so only a few hundred rows per source come back.
2. **Classify** (`classify()` in `leads.py`):
   - `strong`: name clearly means coffee (coffee, espresso, roaster, latte, boba, matcha, chains)
   - `category`: the dataset's category field says coffee/tea (NYC "Coffee/Tea", NAICS text)
   - `cafe`: name has "cafe" (often coffee, sometimes a diner)
   - `keyword`: name has brew(s), grind(s), grounds, grounded, cup, tea or perk. Counts on
     its own, unless the dataset's category is clearly not food (landscaping, construction...)
     or the name hits a noise filter (`KEYWORD_NEGATIVE`: "Grounds Maintenance", "Grounded Yoga")
   - `naics`: only the NAICS code says snack/beverage bar (722515)
   - `weak`: words like bean/bakery/donut plus a food category
   - negative patterns drop breweries, internet cafes, bookkeeping firms named "Platte", and
     any name with sandwich(es), burger, bowls or plates

   To add a search word, put it in `DEFAULT_WORDS` (what the portals are asked for) **and**
   in `STRONG`, `CAFE` or `KEYWORD` (what counts), all in `leads.py`. Six ArcGIS sources have
   their own `words` list in `sources.json`. To throw a word out, add it to `NEGATIVE`.
3. **Dedupe**: records collapse into one lead per location, keyed on street
   number + street name + ZIP. A Chicago shop with a new license *and* a license
   inspection becomes one lead with both signals.
4. **Score** 0 to 100: match strength + best signal (pre-opening inspection >
   new food permit > new license > state registration) + a bonus for multiple
   signals + recency, minus 45 for corporate chains (they buy POS centrally,
   so independents float to the top; chains stay in the table, flagged in `chain`).

## Estimated opening (`open_status`, `open_est_start`, `open_est_end`, `open_conf`)

Each lead gets a label (**Upcoming**, **Opening now**, **Probably open**) and a date range,
estimated from its best record: pre-opening inspection 0 to 6 weeks after the inspection
(high confidence), new food permit 0 to 30 days, new business license 2 weeks before to
6 weeks after, first-ever inspection 2 months before to 2 weeks after (usually already
open), state registration 3 to 9 months after (low confidence). An actual reported date
(Texas first-sales date) wins. These are rules of thumb in `OPENING_RULES` in `leads.py`;
a source can override with an `"opening": {"lo", "hi", "conf", "basis"}` block or map a real
date with `fields.open_date`. Tune them once real opening dates come in.

## Owner names, Instagram and contact info (`enrich.py`)

After each pull, `python3 enrich.py` fills an `enrichment` table (kept across
runs) and adds these columns to the CSV and the page: `owner`, `owner_role`,
`owners_all`, `email`, `website`, `instagram`, `instagram_conf`, `facebook`,
`tiktok`, `osm_phone`, `hours`, `shop_address`, `enrich_sources`.

| Step | Where it looks | What it gets |
|---|---|---|
| owners | Columns mapped as `owner` / `owner_org` / `email` in `sources.json` (CO registered agent, NY chairman / process name, CT business email) | owner or agent name, email |
| owners | Chicago **Business Owners** dataset (via license → account number) | owner name + title (president, managing member, sole proprietor…) |
| owners | CT **Business Registry – Principals** | principal names |
| owners | Oregon new-business filings (all associated names) | members, managers, registrant |
| owners | TX **Active Sales Tax Permit Holders** (by taxpayer number) | the actual storefront address and first sales date for TX state filings |
| osm | OpenStreetMap cafes via the Overpass API (cached a week per state) | website, Instagram, Facebook, phone, hours, matched by address or location + name |
| search | Brave Search API, **only if `BRAVE_API_KEY` is set** (free tier, ~45 leads a day) | Instagram handle + website for the top independent leads |
| web | The shop's home page and its About / Our Story / Team page, at most once a month | Instagram / Facebook / TikTok links, email, phone, owner or founder named on the site |
| people | Brave Search, **only if `BRAVE_API_KEY` is set** (8 leads a day by default) | owner / founder from news, local blogs and LinkedIn result titles |
| manual | `enrichment_manual.json` | anything you checked by hand; wins over the rest |

`instagram_conf`: **high** = handle matches the name and the city (or came
from the shop's own site / OpenStreetMap), **medium** = name matches but the
city wasn't confirmed (shown as "likely"), **verified** = checked by hand.
Instagram itself is never fetched.

Owner names from public filings are often a registered agent, an officer or a
filing contact rather than the day-to-day owner. A name the shop's own site, a
news story or a LinkedIn title calls "owner" or "founder" is ranked above
officers and agents, and is saved with a link to where it was found
(`web_owner`, `web_owner_role`, `web_owner_url`). Verify before outreach.
Daily Brave search volume is set by `SEARCH_LIMIT` and `OWNER_SEARCH_LIMIT` in
`.github/workflows/daily.yml` (defaults fit Brave's $5/month free credit). To turn on search: get a free key at
https://brave.com/search/api/, then add it as a repo secret named
`BRAVE_API_KEY` (Settings → Secrets and variables → Actions).

```bash
python3 enrich.py                      # all steps
python3 enrich.py --steps owners,osm   # just some
python3 enrich.py stats
```

When adding a source, map an owner / applicant / registrant column to
`fields.owner` (and a company name to `fields.owner_org`, an email to
`fields.email`) and set `"owner_role"` on the source, e.g. `"applicant"`.

## Database

- `leads`: one row per business location. Key columns: `name`, `address`,
  `city`, `state`, `zip`, `phone`, `first_event_date`, `signals`, `sources`,
  `match_level`, `chain`, `score`, `status` (yours to edit: new / contacted /
  won / not a fit), `notes`.
- `records`: every raw matching record, with the original row in `raw` (JSON).
- `runs`: what each run fetched, and any errors.

```sql
-- hottest independent leads
SELECT name, address, city, state, phone, first_event_date, signals, score
FROM leads WHERE chain = '' ORDER BY score DESC LIMIT 50;
```

## Add a jurisdiction

Most US cities, counties and states publish on **Socrata** (`data.<place>.gov`)
or **ArcGIS Hub**. Look for datasets named like *Food Establishment
Inspections*, *Health Permits*, *Business Licenses*, *New Business
Registrations*, or *Building Permits*.

1. Find the dataset and its 4x4 id (the `xxxx-xxxx` in the URL).
2. See its columns:
   ```bash
   python3 leads.py discover socrata data.cityofboston.gov abcd-1234
   python3 leads.py discover arcgis https://services.arcgis.com/.../FeatureServer/0
   ```
3. Add an entry to `sources.json`. Minimal Socrata example:
   ```json
   {
     "id": "denver_food_licenses",
     "jurisdiction": "Denver, CO", "state": "CO", "level": "city",
     "type": "socrata", "domain": "data.denvergov.org", "dataset": "abcd-1234",
     "signal": "new_food_permit",
     "date_field": "issue_date",
     "extra_where": "license_type = 'Retail Food Establishment'",
     "filter_fields": ["business_name"],
     "fields": {"id": "license_id", "name": "business_name", "address": "address",
                "city": "city", "zip": "zip", "phone": "phone", "date": "issue_date"}
   }
   ```
4. `python3 leads.py run --only denver_food_licenses` and check the counts.

Source keys:

| key | meaning |
|---|---|
| `type` | `socrata` or `arcgis` (ArcGIS uses `url` = the FeatureServer layer instead of domain/dataset) |
| `signal` | `pre_opening_inspection`, `new_food_permit`, `plan_review`, `building_permit`, `new_business_license`, `business_registration`, `inspection` (drives the score) |
| `date_field` | the date that means "opened / applied / registered" |
| `extra_where` | extra SoQL/SQL filter, e.g. only new licenses or only opening inspections |
| `filter_fields` | fields the coffee name filter runs on (add a category field if there is one) |
| `fields` | map of our columns to theirs. A list joins fields (`["building","street"]`); `"a\|b"` takes the first non-empty |
| `mode: "first_appearance"` + `group_fields` | for inspection datasets with no "opening" label: keeps facilities whose *first* record is inside the window |
| `address_has_city` | address string contains "City, ST 12345" and should be split |
| `drop_unless_state` | drop rows whose state isn't this (state filings with out-of-state mailing addresses) |
| `exclude_status` | regex on the status field to drop (dissolved, revoked...) |
| `enabled: false` | keep the entry but skip it |

### Good next sources

- **Building/tenant-improvement permits** (Seattle, SF, Austin, Denver,
  Nashville): the earliest storefront signal, 2 to 6 months before opening.
  Filter on description text like "coffee" or "cafe".
- **County health plan reviews** (Maricopa, King, Miami-Dade, Harris): new
  food establishment plan review is the earliest health signal.
- **State liquor/food sales tax permits** (e.g. TX Comptroller sales tax
  permits with NAICS 722515).
- More state business registries on Socrata: Iowa, Washington, Delaware
  (paid), Florida Sunbiz (bulk files, not an API).

## Caveats

- Name matching misses shops with no coffee word in the name ("Sightglass",
  "Blue Door"). NYC's cuisine tag and LA/CT NAICS codes catch some of these.
- State registrations include ghost kitchens, roasters, mobile carts and
  people who never open. Score reflects that.
- San Francisco's portal blocked browser requests during the first build, so
  it's not in yet.
