#!/usr/bin/env python3
"""Off-VM radio server prober. Runs on GitHub Actions runners.

One status request per server (first answering path wins). With --shard NNN
it works only data/servers-NNN.jsonl, which is how the workflow fans the
sweep out across a job matrix; without it, all shards in order. Results
append to results/results-NNN.jsonl, one JSON line per server; a server
already in the results file is skipped, so runs resume where the last one
stopped. A per-invocation time budget stops the run cleanly; the workflow
commits whatever exists.

Per server it tries, on the preferred scheme then the fallback:
  /status-json.xsl, /status.xsl, /stats?sid=1, /7.html, /index.html?sid=1
A server that gives no HTTP response at all after MAX_NO_RESPONSE attempts
is abandoned early: unreachable hosts otherwise burn the full timeout on
every candidate path and dominate the runtime.
Evidence recorded: whether a status page answered, the parsed mount list,
and how many of the server's catalogued endpoint paths appear as live
mounts.
"""
import argparse, json, os, re, sys, time
from concurrent.futures import ThreadPoolExecutor
from urllib.parse import urlparse
import urllib.request

ROOT = os.path.dirname(os.path.abspath(__file__))
DATA = os.path.join(ROOT, "data")
RESULTS = os.path.join(ROOT, "results")
UA = {"User-Agent": "Mozilla/5.0 (X11; Linux x86_64) KilambiResearch/1.0"}
TIME_BUDGET_S = int(os.environ.get("PROBE_TIME_BUDGET", "3300"))
WORKERS = int(os.environ.get("PROBE_WORKERS", "96"))
FETCH_TIMEOUT_S = int(os.environ.get("PROBE_FETCH_TIMEOUT", "4"))
MAX_NO_RESPONSE = 6
CANDIDATES = ["/status-json.xsl", "/status.xsl", "/stats?sid=1",
              "/7.html", "/index.html?sid=1"]

def fetch(url, timeout=None):
    req = urllib.request.Request(url, headers=UA)
    with urllib.request.urlopen(req, timeout=timeout or FETCH_TIMEOUT_S) as r:
        return r.status, r.read(2_000_000).decode("utf-8", "replace")

def mounts_from_json(text):
    d = json.loads(text)
    src = d.get("icestats", {}).get("source", [])
    if isinstance(src, dict):
        src = [src]
    out = []
    for s in src:
        lu = s.get("listenurl", "")
        if lu:
            out.append(urlparse(lu).path)
    return out

def mounts_from_text(text):
    m = set(re.findall(r'mount="([^"]+)"', text))
    m |= set(re.findall(r'Mount Point[^<]*</td>\s*<td[^>]*>(/[^<]+)', text))
    m |= set(re.findall(r'href="(/[^"]+\.m3u)"', text))
    return sorted(m)

def check(rec):
    server, schemes = rec["s"], rec.get("sch", ["http"])
    order = ["https", "http"] if "https" in schemes else ["http", "https"]
    no_response = 0
    for scheme in order:
        for path in CANDIDATES:
            url = f"{scheme}://{server}{path}"
            try:
                code, body = fetch(url)
            except Exception:
                no_response += 1
                if no_response >= MAX_NO_RESPONSE:
                    return {"server": server, "status_url": None,
                            "http_code": None, "mounts_found": 0,
                            "mounts_sample": [],
                            "endpoints_on_server": rec["n"],
                            "sample_paths_mount_confirmed": 0,
                            "note": "no response from host"}
                continue
            mounts = []
            if path.endswith("status-json.xsl"):
                try:
                    mounts = mounts_from_json(body)
                except Exception:
                    mounts = []
            if not mounts and "7.html" in path:
                parts = body.strip().split(",")
                if len(parts) >= 7 and parts[0].isdigit():
                    mounts = ["(shoutcast-v1-alive)"]
            if not mounts:
                mounts = mounts_from_text(body)
            confirmed = 0
            if mounts and mounts != ["(shoutcast-v1-alive)"]:
                confirmed = sum(1 for p in rec.get("p", []) if p in mounts)
            return {"server": server, "status_url": url, "http_code": code,
                    "mounts_found": len(mounts), "mounts_sample": mounts[:50],
                    "endpoints_on_server": rec["n"],
                    "sample_paths_mount_confirmed": confirmed}
    return {"server": server, "status_url": None, "http_code": None,
            "mounts_found": 0, "mounts_sample": [],
            "endpoints_on_server": rec["n"],
            "sample_paths_mount_confirmed": 0,
            "note": "no status page answered"}

def run_shard(shard, start):
    num = shard[len("servers-"):-len(".jsonl")]
    res_path = os.path.join(RESULTS, f"results-{num}.jsonl")
    done = set()
    if os.path.exists(res_path):
        with open(res_path) as f:
            for line in f:
                try:
                    done.add(json.loads(line)["server"])
                except Exception:
                    pass
    todo = []
    with open(os.path.join(DATA, shard)) as f:
        for line in f:
            rec = json.loads(line)
            if rec["s"] not in done:
                todo.append(rec)
    if not todo:
        print(f"shard {num}: nothing to do", flush=True)
        return True
    print(f"shard {num}: {len(todo)} servers to probe", flush=True)
    with open(res_path, "a") as out:
        with ThreadPoolExecutor(max_workers=WORKERS) as ex:
            for r in ex.map(check, todo):
                out.write(json.dumps(r) + "\n")
                out.flush()
                if time.time() - start > TIME_BUDGET_S:
                    print("time budget reached; stopping cleanly", flush=True)
                    return False
    print(f"shard {num}: complete", flush=True)
    return True

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--shard", default=None,
                    help="shard number to probe, e.g. 007 (default: all)")
    args = ap.parse_args()
    os.makedirs(RESULTS, exist_ok=True)
    shards = sorted(f for f in os.listdir(DATA) if f.startswith("servers-"))
    if args.shard is not None:
        want = f"servers-{args.shard}.jsonl"
        if want not in shards:
            print(f"no such shard: {want}", flush=True)
            return 1
        run_shard(want, time.time())
        return 0
    start = time.time()
    for shard in shards:
        if not run_shard(shard, start):
            return 0
    print("all shards complete", flush=True)
    return 0

if __name__ == "__main__":
    sys.exit(main())
