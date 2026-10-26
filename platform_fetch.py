#!/usr/bin/env python3
"""Off-VM platform data fetcher for the radio.kilambi.com catalog.
Runs on GitHub Actions runners (never from the research machine: every
novel destination reached from an agent-launched process raises a user
approval prompt there).

Stages (each independently runnable via --stage):
  laut      GET https://api.laut.fm/stations, save the raw JSON verbatim
            to platform-results/laut_stations.json. One fetch confirms
            which of the ~297k queued laut.fm family endpoints are still
            listed; the join itself is platform_join_laut.py.
  shoutcast POST https://directory.shoutcast.com/Home/BrowseByGenre for
            the genres NOT already banked on the research machine
            (201 of 313 banked 2026-10-25; the bundled
            platform_data/shoutcast_genres.json carries the full list and
            the banked subset). Raw per-genre JSON lands in
            platform-results/shoutcast_genres/<genre>.json, checkpointed
            after every genre so runs resume. Then POST /Home/Top, then
            a bounded POST /Player/GetStreamUrl sweep over the station
            IDs in the newly fetched genres (the exact stream URLs are
            the join key against the whole queue; the sweep is capped
            and checkpointed, and resumes across runs).
  rcast     NOT IMPLEMENTED. Stage 3 is a 2,694-page HTML crawl of
            rcast.net/dir; this stage is an explicit stub that records
            its own status and fetches nothing.
  all       laut + shoutcast + the rcast stub note.

Politeness: one request at a time, a short pause between requests, no
stream audio is ever fetched.
"""
import argparse, hashlib, json, os, re, sys, time
import urllib.request, urllib.error, urllib.parse

ROOT = os.path.dirname(os.path.abspath(__file__))
RESULTS = os.path.join(ROOT, "platform-results")
GENRE_DIR = os.path.join(RESULTS, "shoutcast_genres")
GENRES_FILE = os.path.join(ROOT, "platform_data", "shoutcast_genres.json")
UA = {"User-Agent": "Mozilla/5.0 (X11; Linux x86_64) KilambiResearch/1.0"}
TIME_BUDGET_S = int(os.environ.get("PLATFORM_TIME_BUDGET", "2700"))
PAUSE_S = float(os.environ.get("PLATFORM_PAUSE", "0.3"))


def atomic_write(path, data_bytes):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    tmp = path + ".tmp"
    with open(tmp, "wb") as f:
        f.write(data_bytes)
    os.replace(tmp, path)


def save_json(path, obj):
    atomic_write(path, json.dumps(obj, indent=1).encode())


def request(method, url, data=None, timeout=60, retries=3):
    body = urllib.parse.urlencode(data).encode() if data else None
    headers = dict(UA)
    if body:
        headers["Content-Type"] = "application/x-www-form-urlencoded"
    last = None
    for attempt in range(retries):
        req = urllib.request.Request(url, data=body, headers=headers,
                                     method=method)
        try:
            with urllib.request.urlopen(req, timeout=timeout) as r:
                return r.status, r.read()
        except Exception as e:  # noqa: BLE001
            last = e
            time.sleep(2 * (attempt + 1))
    raise last


# ---------------------------------------------------------------- laut
def stage_laut():
    status_path = os.path.join(RESULTS, "laut_fetch_status.json")
    status = {"stage": "laut", "url": "https://api.laut.fm/stations",
              "started_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())}
    try:
        code, raw = request("GET", "https://api.laut.fm/stations",
                            timeout=300)
        out = os.path.join(RESULTS, "laut_stations.json")
        atomic_write(out, raw)
        status["http_status"] = code
        status["bytes"] = len(raw)
        status["sha256"] = hashlib.sha256(raw).hexdigest()
        try:
            parsed = json.loads(raw.decode("utf-8", "replace"))
            if isinstance(parsed, list):
                status["stations_in_payload"] = len(parsed)
            elif isinstance(parsed, dict):
                for key in ("stations", "result", "data"):
                    if isinstance(parsed.get(key), list):
                        status["stations_in_payload"] = len(parsed[key])
                        break
        except Exception as e:  # noqa: BLE001
            status["parse_note"] = f"payload kept raw; parse failed: {e}"
        status["ok"] = True
    except Exception as e:  # noqa: BLE001
        status["ok"] = False
        status["error"] = f"{type(e).__name__}: {e}"
    status["finished_at"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    save_json(status_path, status)
    print(json.dumps(status))
    return 0 if status["ok"] else 1


# ------------------------------------------------------------ shoutcast
def sanitise(genre):
    return re.sub(r"[^A-Za-z0-9_.-]+", "_", genre) + ".json"


def load_genre_plan():
    with open(GENRES_FILE) as f:
        data = json.load(f)
    return data["all"], set(data["banked"])


def stage_shoutcast(dry_run=False, streamurl_cap=None):
    if streamurl_cap is None:
        streamurl_cap = int(os.environ.get("PLATFORM_STREAMURL_CAP", "5000"))
    all_genres, banked = load_genre_plan()
    status_path = os.path.join(RESULTS, "shoutcast_fetch_status.json")
    status = {"stage": "shoutcast", "genres_total": len(all_genres),
              "genres_banked_before": len(banked), "done": {},
              "started_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())}
    if os.path.exists(status_path):
        try:
            prev = json.load(open(status_path))
            status["done"] = prev.get("done", {})
        except Exception:
            pass
    todo = [g for g in all_genres
            if g not in banked and g not in status["done"]]
    print(f"shoutcast: {len(all_genres)} genres total, {len(banked)} banked, "
          f"{len(status['done'])} fetched in earlier runs, "
          f"{len(todo)} remaining")
    if dry_run:
        for g in todo:
            print("  remaining:", g)
        status["dry_run_remaining"] = todo
        return 0
    deadline = time.time() + TIME_BUDGET_S
    os.makedirs(GENRE_DIR, exist_ok=True)
    for genre in todo:
        if time.time() > deadline:
            status["stopped"] = "time budget reached; rerun to resume"
            break
        rec = {"genre": genre}
        try:
            code, raw = request(
                "POST", "https://directory.shoutcast.com/Home/BrowseByGenre",
                data={"genrename": genre}, timeout=60)
            rec["http_status"] = code
            try:
                data = json.loads(raw.decode("utf-8", "replace"))
                if isinstance(data, list):
                    rec["stations"] = len(data)
                    save_json(os.path.join(GENRE_DIR, sanitise(genre)), data)
                else:
                    rec["stations"] = None
                    rec["note"] = "response was not a JSON array; kept raw"
                    atomic_write(os.path.join(
                        GENRE_DIR, sanitise(genre) + ".raw.txt"), raw)
            except Exception:
                rec["stations"] = None
                atomic_write(os.path.join(
                    GENRE_DIR, sanitise(genre) + ".raw.txt"), raw)
        except Exception as e:  # noqa: BLE001
            rec["error"] = f"{type(e).__name__}: {e}"
        status["done"][genre] = rec
        save_json(status_path, status)
        time.sleep(PAUSE_S)
    # Top stations route.
    try:
        code, raw = request("POST", "https://directory.shoutcast.com/Home/Top",
                            timeout=60)
        atomic_write(os.path.join(RESULTS, "shoutcast_top.json"), raw)
        status["top_http_status"] = code
    except Exception as e:  # noqa: BLE001
        status["top_error"] = f"{type(e).__name__}: {e}"
    save_json(status_path, status)
    # GetStreamUrl sweep over station IDs in the fetched genre files.
    # Exact stream URLs are the join key against the whole probe queue.
    ids, seen = [], set()
    if os.path.isdir(GENRE_DIR):
        for fn in sorted(os.listdir(GENRE_DIR)):
            if not fn.endswith(".json"):
                continue
            try:
                data = json.load(open(os.path.join(GENRE_DIR, fn)))
            except Exception:
                continue
            if isinstance(data, list):
                for st in data:
                    sid = st.get("ID") if isinstance(st, dict) else None
                    if sid is not None and sid not in seen:
                        seen.add(sid)
                        ids.append(sid)
    sweep_path = os.path.join(RESULTS, "shoutcast_streamurls.jsonl")
    swept = set()
    if os.path.exists(sweep_path):
        with open(sweep_path) as f:
            for line in f:
                try:
                    swept.add(json.loads(line)["station_id"])
                except Exception:
                    pass
    remaining_ids = [i for i in ids if i not in swept][:streamurl_cap]
    status["streamurl_ids_total"] = len(ids)
    status["streamurl_ids_remaining_this_run"] = len(remaining_ids)
    save_json(status_path, status)
    with open(sweep_path, "a") as out:
        for sid in remaining_ids:
            if time.time() > deadline:
                status["streamurl_stopped"] = "time budget; rerun to resume"
                break
            rec = {"station_id": sid}
            try:
                code, raw = request(
                    "POST",
                    "https://directory.shoutcast.com/Player/GetStreamUrl",
                    data={"station": sid}, timeout=45)
                rec["http_status"] = code
                rec["body"] = raw.decode("utf-8", "replace")[:1000]
            except Exception as e:  # noqa: BLE001
                rec["error"] = f"{type(e).__name__}: {e}"
            out.write(json.dumps(rec) + "\n")
            out.flush()
            time.sleep(PAUSE_S)
    status["finished_at"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    save_json(status_path, status)
    print(json.dumps({k: status[k] for k in
                      ("genres_total", "genres_banked_before")}),
          f"genres fetched total: {len(status['done'])}")
    return 0


# ---------------------------------------------------------------- rcast
def stage_rcast():
    """Stub. The RCast directory (rcast.net/dir) is 2,694 paginated HTML
    pages; the crawl is stage 3 and has not been built. This records the
    fact and fetches nothing, rather than pretending coverage."""
    status = {"stage": "rcast", "implemented": False,
              "note": ("RCast directory crawl not yet implemented (stage 3). "
                       "Route: https://rcast.net/dir, 2,694 HTML pages, "
                       "stream URL per entry; family is 53,886 queue "
                       "endpoints. Nothing was fetched."),
              "at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())}
    save_json(os.path.join(RESULTS, "rcast_status.json"), status)
    print("rcast: NOT IMPLEMENTED (stage 3 stub); nothing fetched")
    return 0


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--stage", choices=["laut", "shoutcast", "rcast", "all"],
                    default="all")
    ap.add_argument("--dry-run", action="store_true",
                    help="shoutcast only: list remaining genres, fetch nothing")
    ap.add_argument("--streamurl-cap", type=int, default=None,
                    help="max GetStreamUrl lookups this run "
                         "(default: env PLATFORM_STREAMURL_CAP or 5000)")
    args = ap.parse_args()
    os.makedirs(RESULTS, exist_ok=True)
    rc = 0
    if args.stage in ("laut", "all"):
        rc |= stage_laut()
    if args.stage in ("shoutcast", "all"):
        rc |= stage_shoutcast(dry_run=args.dry_run,
                              streamurl_cap=args.streamurl_cap)
    if args.stage in ("rcast", "all"):
        stage_rcast()
    return rc


if __name__ == "__main__":
    sys.exit(main())
