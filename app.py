import argparse
import json
import os
import threading
import time
from urllib.parse import quote

import requests
from flask import Flask, Response, jsonify, request, send_from_directory

BASE_URL = "https://api.opencaselist.com/v1"
OC_USERNAME = os.environ.get("OC_USERNAME")
OC_PASSWORD = os.environ.get("OC_PASSWORD")
DEFAULT_SHARD = "hspolicy25"
KNOWN_SHARDS = ["hspolicy25", "ndtceda25", "hsld25", "hspf25"]
CACHE_DIR = "caches"

session = requests.Session()
app = Flask(__name__)

# Shared crawl state for progress reporting
crawl_state = {
    "status": "idle",  # idle | running | error | done
    "shard": None,
    "progress": 0.0,
    "message": "",
    "rounds": 0,
    "schools_done": 0,
    "total_schools": 0,
    "teams_done": 0,
    "requests_done": 0,
    "requests_total": 0,
    "error": None,
    "updated_at": None,
}
crawl_lock = threading.Lock()


def login(username, password):
    payload = {
        "username": username,
        "password": password,
        "remember": True,
    }
    resp = session.post(f"{BASE_URL}/login", json=payload)
    resp.raise_for_status()


def ensure_logged_in():
    if session.cookies:
        return
    if not OC_USERNAME or not OC_PASSWORD:
        raise RuntimeError("Set OC_USERNAME and OC_PASSWORD environment variables.")
    login(OC_USERNAME, OC_PASSWORD)


def update_crawl_state(**kwargs):
    with crawl_lock:
        crawl_state.update(kwargs)
        crawl_state["updated_at"] = time.time()


def _normalize_shard(shard):
    shard = (shard or "").strip() or DEFAULT_SHARD
    if shard not in KNOWN_SHARDS:
        raise ValueError(f"Unsupported shard '{shard}'. Must be one of: {', '.join(KNOWN_SHARDS)}")
    return shard


def _cache_file_for_shard(shard):
    os.makedirs(CACHE_DIR, exist_ok=True)
    return os.path.join(CACHE_DIR, f"round_cache_{shard}.json")


def fetch_json(url, params=None):
    resp = session.get(url, params=params)
    print(f"[fetch] GET {resp.url} -> {resp.status_code}")
    resp.raise_for_status()
    # Track requests during a crawl for progress visibility.
    with crawl_lock:
        if crawl_state.get("status") == "running":
            crawl_state["requests_done"] = crawl_state.get("requests_done", 0) + 1
            crawl_state["updated_at"] = time.time()
    return resp.json()


def fetch_schools(shard):
    return fetch_json(f"{BASE_URL}/caselists/{shard}/schools")


def fetch_teams(shard, school_name):
    return fetch_json(f"{BASE_URL}/caselists/{shard}/schools/{school_name}/teams")


def fetch_rounds(shard, school_name, team_name):
    return fetch_json(
        f"{BASE_URL}/caselists/{shard}/schools/{school_name}/teams/{team_name}/rounds",
        params={"side": ""},
    )


def build_round_index(shard=DEFAULT_SHARD):
    shard = _normalize_shard(shard)
    ensure_logged_in()
    print("[build_index] starting")
    schools = fetch_schools(shard)
    rounds = []

    update_crawl_state(
        status="running",
        shard=shard,
        progress=0.0,
        message="Fetched schools",
        rounds=0,
        schools_done=0,
        total_schools=len(schools),
        teams_done=0,
        requests_done=crawl_state.get("requests_done", 0),
        requests_total=1 + len(schools),  # schools call + one teams call per school
        error=None,
    )

    for idx, school in enumerate(schools):
        school_name = school["name"]
        school_display = school.get("display_name") or school_name
        print(f"[build_index] school {school_display}")
        teams = fetch_teams(shard, school_name)
        # Account for upcoming round requests for this school's teams.
        update_crawl_state(
            requests_total=crawl_state.get("requests_total", 0) + len(teams)
        )

        for team in teams:
            team_name = team["name"]
            team_display = team.get("display_name") or f"{school_display} {team_name}"
            print(f"  [team] {team_display}")
            team_rounds = fetch_rounds(shard, school_name, team_name)

            for r in team_rounds:
                opensrc = r.get("opensource")
                file_url = f"/files/{opensrc}" if opensrc else None
                rounds.append(
                    {
                        "team": team_display,
                        "school": school_display,
                        "tournament": r.get("tournament"),
                        "round": r.get("round"),
                        "side": r.get("side"),
                        "opponent": r.get("opponent"),
                        "report": r.get("report", ""),
                        "file_url": file_url,
                        "download_path": opensrc,
                    }
                )

            update_crawl_state(
                status="running",
                shard=shard,
                # progress is based on requests done vs. total discovered so far.
                progress=(
                    float(crawl_state.get("requests_done", 0))
                    / max(crawl_state.get("requests_total", 1), 1)
                ),
                message=f"Processed {school_display} (requests: {crawl_state.get('requests_done', 0)}/{crawl_state.get('requests_total', 0)})",
                rounds=len(rounds),
                schools_done=idx + 1,
                total_schools=len(schools),
                teams_done=crawl_state.get("teams_done", 0) + len(teams),
                requests_done=crawl_state.get("requests_done", 0),
                requests_total=crawl_state.get("requests_total", 0),
                error=None,
            )

    cache = {
        "shard": shard,
        "built_at": time.time(),
        "rounds": rounds,
    }
    cache_file = _cache_file_for_shard(shard)
    with open(cache_file, "w") as f:
        json.dump(cache, f)
    print(f"[build_index] cached {len(rounds)} rounds to {cache_file}")
    update_crawl_state(
        status="done",
        shard=shard,
        progress=1.0,
        message=f"Cached {len(rounds)} rounds (requests: {crawl_state.get('requests_done', 0)}/{crawl_state.get('requests_total', 0)})",
        rounds=len(rounds),
        requests_done=crawl_state.get("requests_done", 0),
        requests_total=crawl_state.get("requests_total", 0),
        error=None,
    )
    return cache


def load_cache(shard=DEFAULT_SHARD):
    shard = _normalize_shard(shard)
    cache_file = _cache_file_for_shard(shard)
    if not os.path.exists(cache_file):
        return None
    with open(cache_file, "r") as f:
        cache = json.load(f)
    if cache.get("shard") != shard:
        return None
    return cache


def ensure_cache(shard=DEFAULT_SHARD, force=False):
    shard = _normalize_shard(shard)
    cache = None if force else load_cache(shard)
    if cache:
        return cache
    return build_round_index(shard)


def filter_results(query, items):
    """Keep only items whose report or team name contains the query (case-insensitive)."""
    q = query.lower()
    filtered = []
    for item in items:
        snippet = (item.get("report") or item.get("snippet") or "").lower()
        team = (item.get("team") or item.get("team_display_name") or "").lower()
        if q in snippet or q in team:
            filtered.append(item)
    return filtered


@app.route("/api/search")
def api_search():
    query = request.args.get("q", "").strip()
    shard_raw = request.args.get("shard", DEFAULT_SHARD)

    if not query:
        return jsonify({"error": "Missing query"}), 400
    try:
        shard = _normalize_shard(shard_raw)
    except ValueError as exc:
        return jsonify({"error": str(exc)}), 400

    cache = load_cache(shard)
    if not cache:
        return jsonify({"error": "Cache missing. Trigger rebuild first."}), 400

    items = cache.get("rounds", [])
    print(f"[api_search] incoming q='{query}' shard='{shard}' items_in_cache={len(items)}")

    filtered_items = filter_results(query, items)
    print(f"[api_search] items after filter: {len(filtered_items)}")
    rows = []
    for r in filtered_items:
        rows.append(
            {
                "team": r.get("team"),
                "school": r.get("school"),
                "file_name": r.get("tournament") or "",
                "file_url": r.get("file_url"),
                "snippet": r.get("report") or "",
            }
        )

    print(f"[api_search] rows returned to client: {len(rows)}")
    return jsonify(rows)


def _crawl_thread(shard):
    try:
        update_crawl_state(requests_done=0)
        build_round_index(shard)
    except Exception as exc:  # pragma: no cover - runtime safeguard
        update_crawl_state(status="error", error=str(exc), message=f"Crawl failed: {exc}")


@app.route("/api/rebuild", methods=["POST", "GET"])
def rebuild_cache():
    shard_raw = request.args.get("shard", DEFAULT_SHARD)
    try:
        shard = _normalize_shard(shard_raw)
    except ValueError as exc:
        return jsonify({"error": str(exc)}), 400

    with crawl_lock:
        if crawl_state.get("status") == "running":
            return (
                jsonify(
                    {
                        "status": "running",
                        "message": "Crawl already in progress",
                        "shard": crawl_state.get("shard"),
                    }
                ),
                409,
            )
        update_crawl_state(
            status="running",
            shard=shard,
            progress=0.0,
            message="Starting crawl",
            rounds=0,
            schools_done=0,
            total_schools=0,
            teams_done=0,
            requests_done=0,
            error=None,
        )

    t = threading.Thread(target=_crawl_thread, args=(shard,), daemon=True)
    t.start()

    return jsonify({"status": "started", "shard": shard})


@app.route("/api/status")
def crawl_status():
    with crawl_lock:
        state_copy = dict(crawl_state)
    return jsonify(state_copy)


@app.route("/")
def root():
    return send_from_directory(".", "index.html")


@app.route("/caches/<path:filename>")
def serve_cache_file(filename):
    return send_from_directory(CACHE_DIR, filename)


@app.route("/files/<path:download_path>")
def proxy_file(download_path):
    ensure_logged_in()
    encoded_path = quote(download_path, safe="/")
    upstream_url = f"{BASE_URL}/download"
    resp = session.get(
        upstream_url,
        params={"path": encoded_path},
        stream=True,
        allow_redirects=True,
    )

    if resp.status_code != 200:
        body = None
        try:
            body = resp.text[:500]
        except Exception:
            body = None
        return (
            jsonify(
                {
                    "error": "Failed to fetch file",
                    "status": resp.status_code,
                    "url_tried": f"{upstream_url}?path={encoded_path}",
                    "body": body,
                }
            ),
            resp.status_code,
        )

    headers = {
        "Content-Type": resp.headers.get("Content-Type", "application/octet-stream"),
        "Content-Disposition": resp.headers.get("Content-Disposition", ""),
    }
    return Response(resp.iter_content(chunk_size=8192), headers=headers)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Card scraper backend")
    parser.add_argument("--rebuild", action="store_true", help="Rebuild cache and exit")
    parser.add_argument(
        "--shard",
        action="append",
        dest="shards",
        help="Shard(s) to use; can be repeated or comma-separated",
    )
    parser.add_argument(
        "--all",
        action="store_true",
        dest="all_shards",
        help="Rebuild cache for all known shards",
    )
    parser.add_argument("--host", default="127.0.0.1", help="Host for server mode")
    parser.add_argument("--port", type=int, default=5000, help="Port for server mode")
    parser.add_argument("--debug", action="store_true", help="Enable Flask debug mode")
    args = parser.parse_args()

    def _parse_shard_args(shard_args, rebuild_all=False):
        if rebuild_all:
            return list(KNOWN_SHARDS)
        shards = []
        if shard_args:
            for raw in shard_args:
                parts = [part.strip() for part in raw.split(",") if part.strip()]
                for part in parts:
                    shard = _normalize_shard(part)
                    if shard not in shards:
                        shards.append(shard)
        if not shards:
            shards.append(DEFAULT_SHARD)
        return shards

    if args.rebuild:
        shards_to_rebuild = _parse_shard_args(args.shards, args.all_shards)
        print(f"[cli] rebuilding cache for shard(s): {', '.join(shards_to_rebuild)}")
        try:
            for shard in shards_to_rebuild:
                update_crawl_state(
                    status="running", shard=shard, message="CLI rebuild", requests_done=0
                )
                build_round_index(shard)
            print(f"[cli] rebuild complete for {len(shards_to_rebuild)} shard(s)")
        except Exception as exc:
            update_crawl_state(status="error", error=str(exc), message=f"CLI rebuild failed: {exc}")
            raise
    else:
        ensure_logged_in()
        app.run(host=args.host, port=args.port, debug=args.debug)
