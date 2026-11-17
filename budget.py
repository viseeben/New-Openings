"""
Hard monthly limit on Brave searches, so the tool stays inside Brave's free monthly credit
($5, about 1,000 searches).

Every Brave search the tool makes (Instagram/web search, owner search, coming-soon discovery)
is counted in the database, by calendar month. Once the month's count reaches the cap, all
searching stops until the 1st of next month. Default cap: 900, leaving a 100-search cushion.
Change it with the BRAVE_MONTHLY_CAP setting in the workflow (never above 1000 to stay free).

The month's searches are spread evenly: each day gets (what's left, minus what the weekly
coming-soon search still needs this month) / (days left in the month).
"""
import datetime as dt
import math
import os

CAP = min(int(os.environ.get("BRAVE_MONTHLY_CAP", "900") or 900), 1000)
DISCOVERY_PER_WEEK = 36  # 12 cities x 3 searches; discover.py rotates through all cities

SCHEMA = """CREATE TABLE IF NOT EXISTS brave_usage (month TEXT, step TEXT, n INTEGER, PRIMARY KEY (month, step));"""


def _month(today=None):
    return (today or dt.date.today()).strftime("%Y-%m")


def used(con, today=None):
    con.executescript(SCHEMA)
    return con.execute("SELECT coalesce(sum(n),0) FROM brave_usage WHERE month=?", (_month(today),)).fetchone()[0]


def left(con, today=None):
    return max(0, CAP - used(con, today))


def days_left(today=None):
    today = today or dt.date.today()
    nxt = dt.date(today.year + (today.month == 12), today.month % 12 + 1, 1)
    return (nxt - today).days  # includes today


def daily_allowance(con, today=None):
    """Searches today's daily run may use (Instagram/web + owner searches together)."""
    d = days_left(today)
    reserve = DISCOVERY_PER_WEEK * math.ceil(d / 7)
    return max(0, min(left(con, today) - reserve, (left(con, today) - reserve) // d if d else 0))


def spend(con, step, n=1, today=None):
    """Record searches, saved right away. Returns False (and records nothing) if over the cap."""
    con.executescript(SCHEMA)
    if left(con, today) < n:
        return False
    con.execute("INSERT INTO brave_usage VALUES (?,?,?) ON CONFLICT(month, step) DO UPDATE SET n = n + ?",
                (_month(today), step, n, n))
    con.commit()
    return True


def report(con, today=None):
    con.executescript(SCHEMA)
    rows = con.execute("SELECT step, n FROM brave_usage WHERE month=? ORDER BY step", (_month(today),)).fetchall()
    detail = ", ".join(f"{s} {n}" for s, n in rows) or "none yet"
    return f"Brave searches this month: {used(con, today)} of {CAP} allowed ({detail})"
