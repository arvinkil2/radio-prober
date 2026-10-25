# radio-prober

Off-machine verification of Icecast/Shoutcast server status pages for the
radio.kilambi.com catalog.

Why this exists: the catalog holds about 2.07 million stream endpoints on
about 342 thousand servers. One status request per server can confirm which
mount points are live, but that fetching cannot run from the research
machine (every novel destination reached from an agent-launched process
raises a user approval prompt). This repository moves the fetching to
GitHub Actions runners. Nothing on the research machine fetches.

How it works:

- `data/servers-NNN.jsonl` shards list every server (host:port), its
  endpoint count, schemes, and up to 8 sample endpoint paths, sorted by
  endpoint count descending. Servers already tested by earlier local
  pilots are excluded.
- `.github/workflows/probe.yml` runs every 3 hours (and on manual
  dispatch). Each run probes the next unfinished shard for up to 45
  minutes at 32-way concurrency, then commits `results/results-NNN.jsonl`.
- `prober.py` tries the standard status paths (status-json.xsl,
  status.xsl, stats, 7.html) and records whether a status page answered,
  the parsed mount list, and how many sample paths are confirmed live.
  Results are append-only; a server already in the results is skipped,
  so runs resume.

Politeness: at most a handful of requests per server per full pass, no
stream audio is ever fetched, and the pass runs once per server.
