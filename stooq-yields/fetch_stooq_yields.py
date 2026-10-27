#!/usr/bin/env python3
"""Fetch non-US sovereign 10Y benchmark yield history CSVs from Stooq.

Runs on a GitHub Actions runner (off-VM): Stooq bot-checks script
fetches from the news VM's network, so this job lives here and
commits the raw CSVs back to the repo. The VM-side receiver
(scripts/receive_stooq_yields.py in the news-site tree) pulls them
via the GitHub API and merges them into Global Markets.

Symbol list is the one recorded in the news fetcher's COUNTRIES
table (scripts/fetch_global_markets.py), censused 2026-10-26.
Output: stooq-yields/csv/<symbol>.csv + stooq-yields/manifest.json.
Exit 1 only when every symbol failed (a total block must fail
loudly); per-symbol HTTP statuses are always in the manifest.
"""
import csv
import datetime
import io
import json
import os
import sys
import urllib.error
import urllib.request

SYMBOLS = {
    "canada": "10cay.b",
    "united-kingdom": "10gby.b",
    "france": "10fry.b",
    "germany": "10dey.b",
    "italy": "10ity.b",
    "spain": "10esy.b",
    "switzerland": "10chy.b",
    "japan": "10jpy.b",
    "china": "10cny.b",
    "india": "10iny.b",
    "south-korea": "10kry.b",
    "australia": "10auy.b",
}

OUT_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                       "stooq-yields")
CSV_DIR = os.path.join(OUT_DIR, "csv")
MAX_STALE_DAYS = 10


def fetch_one(symbol):
    url = f"https://stooq.com/q/d/l/?s={symbol}&i=d"
    req = urllib.request.Request(
        url, headers={"User-Agent": "Mozilla/5.0 (X11; Linux x86_64) "
                                   "AppleWebKit/537.36"})
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            status = resp.status
            raw = resp.read()
    except urllib.error.HTTPError as e:
        return {"http_status": e.code, "ok": False,
                "error": f"HTTP {e.code}"}
    except Exception as e:  # noqa: BLE001
        return {"http_status": None, "ok": False,
                "error": f"{type(e).__name__}: {e}"}
    text = raw.decode("utf-8", errors="replace")
    rows = []
    try:
        rows = [r for r in csv.DictReader(io.StringIO(text))
                if r.get("Date") and r.get("Close")]
        closes = [float(r["Close"]) for r in rows]
    except Exception as e:  # noqa: BLE001
        return {"http_status": status, "ok": False,
                "error": f"unparseable body ({len(text)} bytes): {e}",
                "body_head": text[:120]}
    if not rows:
        return {"http_status": status, "ok": False,
                "error": "no data rows", "body_head": text[:120]}
    last_date = rows[-1]["Date"]
    try:
        age = (datetime.date.today()
               - datetime.date.fromisoformat(last_date)).days
    except ValueError:
        return {"http_status": status, "ok": False,
                "error": f"bad last date {last_date!r}"}
    ok = age <= MAX_STALE_DAYS and len(rows) >= 5
    return {"http_status": status, "ok": ok, "rows": len(rows),
            "last_date": last_date, "last_close": closes[-1],
            "error": None if ok else f"stale or short: age {age}d, "
                                     f"{len(rows)} rows",
            "_raw": raw}


def main():
    os.makedirs(CSV_DIR, exist_ok=True)
    manifest = {
        "fetched_at": datetime.datetime.now(
            datetime.timezone.utc).isoformat(),
        "source": "stooq daily csv via github actions",
        "symbols": {},
    }
    ok_count = 0
    for slug, symbol in SYMBOLS.items():
        res = fetch_one(symbol)
        raw = res.pop("_raw", None)
        if res["ok"] and raw is not None:
            with open(os.path.join(CSV_DIR, f"{symbol}.csv"),
                      "wb") as f:
                f.write(raw)
            ok_count += 1
        manifest["symbols"][slug] = {"symbol": symbol, **res}
        print(slug, symbol, res)
    with open(os.path.join(OUT_DIR, "manifest.json"), "w") as f:
        json.dump(manifest, f, indent=1)
    print(f"{ok_count}/{len(SYMBOLS)} symbols ok")
    if ok_count == 0:
        print("ALL SYMBOLS FAILED - treating as a block",
              file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
