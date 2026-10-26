#!/usr/bin/env python3
"""Join a saved laut.fm stations payload against the launch probe queue.

Purely local: no fetching. Runs on the research machine against a saved
JSON, and inside GitHub Actions against the fetch stage's output.

  python3 platform_join_laut.py --api platform-results/laut_stations.json
      [--queue platform_data/laut_queue_urls-00.txt]
      [--out platform-results/laut_confirmed_endpoints.jsonl]
      [--tally platform-results/laut_join_tally.json]

Queue input is one stream URL per line; --queue takes one path or a
glob (the laut-family extract of phase0/state/probe-queue.txt ships
split as platform_data/laut_queue_urls-NN.txt, and the default is that
glob; the full probe-queue.txt works too, non-laut lines are ignored).

Matching, per API station, in order:
  1. exact: the station's stream URL, normalised (scheme ignored, query
     and fragment stripped, trailing slash stripped, host lowercased),
     equals a normalised queue URL. The queue's ?t302= tokens make raw
     string equality useless, so normalisation is the honest form of
     "exact" here.
  2. slug: any slug alias of the station (its API name slugified, its
     display name slugified, the slug in its stream URL's host
     <slug>.stream.laut.fm, or the first path segment of its stream URL
     with underscores folded to hyphens) equals a queue slug derived the
     same way from a queued laut URL.

Outputs: one JSON line per confirmed queue endpoint (queue URL, station
name, match type) and a tally JSON: queue laut endpoints, API stations,
stations with a stream URL, exact matches, slug matches, unmatched
stations, distinct queue endpoints confirmed.
"""
import argparse, glob, json, os, re, sys, unicodedata
from urllib.parse import urlparse

ROOT = os.path.dirname(os.path.abspath(__file__))
STREAM_KEYS = ("stream_url", "streamUrl", "streamURL", "streamurl",
               "stream", "url")
NAME_KEYS = ("name", "display_name", "displayName", "title")


def slugify(text):
    if not text:
        return ""
    text = unicodedata.normalize("NFKD", str(text))
    text = "".join(c for c in text if not unicodedata.combining(c))
    text = text.lower().replace("_", "-")
    return re.sub(r"[^a-z0-9]+", "-", text).strip("-")


def url_slugs(url):
    """Slugs a stream URL answers to: host subdomain before
    .stream.laut.fm, and the first path segment (underscores folded)."""
    slugs = set()
    try:
        p = urlparse(url if "://" in url else "https://" + url)
    except Exception:
        return slugs
    host = (p.hostname or "").lower()
    if host.endswith(".stream.laut.fm"):
        sub = host[: -len(".stream.laut.fm")]
        if sub:
            slugs.add(slugify(sub))
    first = p.path.strip("/").split("/")[0] if p.path else ""
    if first:
        slugs.add(slugify(first))
    return {s for s in slugs if s}


def normalise_url(url):
    if not url:
        return ""
    try:
        p = urlparse(url if "://" in url else "https://" + url)
    except Exception:
        return ""
    host = (p.hostname or "").lower()
    if not host:
        return ""
    port = ""
    try:
        if p.port and p.port not in (80, 443):
            port = f":{p.port}"
    except ValueError:
        pass
    return f"{host}{port}{p.path.rstrip('/')}"


def is_laut(url):
    try:
        host = (urlparse(url if "://" in url else "https://" + url)
                .hostname or "").lower()
    except Exception:
        return False
    return host == "laut.fm" or host.endswith(".laut.fm")


def station_records(payload):
    if isinstance(payload, list):
        return payload
    if isinstance(payload, dict):
        for key in ("stations", "result", "data", "items"):
            if isinstance(payload.get(key), list):
                return payload[key]
    return []


def first_key(rec, keys):
    for k in keys:
        v = rec.get(k)
        if v:
            return v
    return ""


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--api", required=True,
                    help="saved laut.fm stations JSON (list or wrapper)")
    ap.add_argument("--queue", default=os.path.join(
        ROOT, "platform_data", "laut_queue_urls-*.txt"),
        help="queue file path or glob pattern")
    ap.add_argument("--out", default=os.path.join(
        ROOT, "platform-results", "laut_confirmed_endpoints.jsonl"))
    ap.add_argument("--tally", default=os.path.join(
        ROOT, "platform-results", "laut_join_tally.json"))
    args = ap.parse_args()

    with open(args.api) as f:
        stations = station_records(json.load(f))

    queue_by_norm = {}
    queue_by_slug = {}
    queue_total = 0
    queue_files = sorted(glob.glob(args.queue)) or [args.queue]
    for qpath in queue_files:
        with open(qpath) as f:
            for line in f:
                url = line.strip()
                if not url or not is_laut(url):
                    continue
                queue_total += 1
                queue_by_norm.setdefault(normalise_url(url), url)
                for slug in url_slugs(url):
                    queue_by_slug.setdefault(slug, []).append(url)

    confirmed = {}  # queue url -> record
    tally = {"queue_laut_endpoints": queue_total,
             "api_stations": len(stations),
             "api_stations_with_stream_url": 0,
             "stations_matched_exact": 0,
             "stations_matched_slug": 0,
             "stations_unmatched": 0}
    unmatched_names = []
    for rec in stations:
        if not isinstance(rec, dict):
            tally["stations_unmatched"] += 1
            continue
        stream = first_key(rec, STREAM_KEYS)
        name = first_key(rec, NAME_KEYS)
        aliases = set()
        for key in NAME_KEYS:
            s = slugify(rec.get(key))
            if s:
                aliases.add(s)
        if stream:
            tally["api_stations_with_stream_url"] += 1
            aliases |= url_slugs(stream)
            norm = normalise_url(stream)
            hit = queue_by_norm.get(norm)
            if hit:
                tally["stations_matched_exact"] += 1
                confirmed.setdefault(
                    hit, {"queue_url": hit, "station": name,
                          "match": "exact_url"})
                continue
        hit_urls = []
        for slug in aliases:
            hit_urls.extend(queue_by_slug.get(slug, []))
        if hit_urls:
            tally["stations_matched_slug"] += 1
            for u in hit_urls:
                confirmed.setdefault(
                    u, {"queue_url": u, "station": name, "match": "slug"})
        else:
            tally["stations_unmatched"] += 1
            if name:
                unmatched_names.append(name)
    tally["queue_endpoints_confirmed"] = len(confirmed)
    tally["unmatched_station_names_sample"] = unmatched_names[:20]

    os.makedirs(os.path.dirname(args.out), exist_ok=True)
    with open(args.out, "w") as f:
        for url in sorted(confirmed):
            f.write(json.dumps(confirmed[url]) + "\n")
    with open(args.tally, "w") as f:
        json.dump(tally, f, indent=1)
    print(json.dumps(tally, indent=1))
    return 0


if __name__ == "__main__":
    sys.exit(main())
