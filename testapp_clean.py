#!/usr/bin/env python3
"""
Smart Farm API Bridge: A Clean, Simple, and Robust Version

This server acts as a fast, cached bridge to a slow and unreliable
upstream API (Oracle APEX).

Architecture:
1.  API Endpoints are FAST: They only ever read from an in-memory cache.
2.  Background Refresher is PATIENT: A single background thread is solely
    responsible for periodically fetching data from the slow upstream API
    and updating the cache.
"""

import os
import threading
import time
import logging
import requests
from flask import Flask, jsonify, request, Response, make_response
from flask_cors import CORS

# ==============================================================================
# === 1. CONFIGURATION
# ==============================================================================

# --- Flask App Setup ---
app = Flask(__name__)
CORS(app, resources={r"/api/*": {"origins": "*"}})
logging.basicConfig(level=logging.INFO, format='[%(levelname)s] %(asctime)s: %(message)s')
logger = logging.getLogger("testapp_clean")

# --- Core Settings ---
# How often the background refresher should run (in seconds)
BACKGROUND_REFRESH_INTERVAL = int(os.getenv('BACKGROUND_REFRESH_INTERVAL', '300'))  # 5 minutes

# How long the background refresher will wait for the slow Oracle server
UPSTREAM_TIMEOUT = int(os.getenv('UPSTREAM_TIMEOUT', '90'))  # 90 seconds

# --- Final, Hardcoded Oracle APEX URL Configuration ---
# This is the definitive list of all endpoints the bridge will use.
UPSTREAM_URLS = {
    "crop_vision_g4": "https://oracleapex.com/ords/g3_data/crop_vision_g4/",
    "crop_vision_g5": "https://oracleapex.com/ords/g3_data/crop-vision/crop_g5/",
    "greenhouse_g1": "https://oracleapex.com/ords/g3_data/greenhouse_group1/",
    "greenhouse_g9": "https://oracleapex.com/ords/g3_data/iot/greenhouse/",
    "irrigation_telemetry": "https://oracleapex.com/ords/g3_data/TELEMETRY_DATA/",
    "soil_g7": "https://oracleapex.com/ords/g3_data/groups/data/7",
    "soil_g8": "https://oracleapex.com/ords/g3_data/groups/data/8",
    "soil_g10": "https://oracleapex.com/ords/g3_data/groups/data/10",
}

# Map friendly group names the frontend expects to the internal cache keys
# This keeps the simple server compatible with existing frontend routes like
# /api/greenhouse/all or /api/soil/all
GROUP_TO_KEYS = {
    "greenhouse": ["greenhouse_g1", "greenhouse_g9"],
    "irrigation": ["irrigation_telemetry"],
    "soil": ["soil_g7", "soil_g8", "soil_g10"],
    "crop_vision": ["crop_vision_g4", "crop_vision_g5"],
}


def list_sources_for(group: str) -> list:
    """Return the configured source keys for a logical group.
    Falls back to the group name if no explicit mapping exists.
    """
    try:
        g = group.lower()
        keys = GROUP_TO_KEYS.get(g)
        if keys:
            return keys
    except Exception:
        pass
    return [group]


def get_upstream_url(group: str, source: str | None = None) -> str:
    """Return the upstream URL for a given source key or group.
    If a source key is provided, return the explicit URL mapping from UPSTREAM_URLS.
    Otherwise return the first configured upstream for the group.
    """
    try:
        if source:
            return UPSTREAM_URLS.get(source, '')
        # no source provided: try to return the first group's URL
        keys = GROUP_TO_KEYS.get(group)
        if keys and len(keys) > 0:
            return UPSTREAM_URLS.get(keys[0], '')
    except Exception:
        pass
    return ''

# ==============================================================================
# === 2. CORE LOGIC (CACHE & UPSTREAM FETCHER)
# ==============================================================================

# Simple, thread-safe in-memory cache
_cache = {}
_cache_lock = threading.Lock()

# Use a single, persistent session for all upstream requests
_session = requests.Session()
_session.headers.update({
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36'
})

def fetch_from_upstream(url: str, name: str = None) -> dict | None:
    """
    A simple, patient function to fetch data from a single URL.
    This is ONLY used by the background refresher.
    """
    try:
        response = _session.get(url, timeout=UPSTREAM_TIMEOUT)
        response.raise_for_status()  # Raise an exception for bad status codes (4xx or 5xx)
        try:
            return response.json()
        except ValueError:
            logger.warning("Upstream returned non-JSON for %s; returning raw text.", url)
            return {"_raw_text": response.text}
    except requests.exceptions.RequestException as e:
        logger.error("Upstream fetch failed for %s: %s", url, e)
        return None

# ==============================================================================
# === 3. THE BACKGROUND REFRESHER (THE "PATIENT BUTLER")
# ==============================================================================

def run_background_refresher():
    """
    An infinite loop that periodically fetches data for all configured URLs
    and updates the shared cache.
    """
    logger.info("Background refresher thread started. Interval: %s seconds.", BACKGROUND_REFRESH_INTERVAL)
    while True:
        logger.info("Starting background refresh cycle...")
        for name, url in UPSTREAM_URLS.items():
            logger.info("Background refreshing: %s", name)
            data = fetch_from_upstream(url)
            if data is not None:
                with _cache_lock:
                    _cache[name] = {
                        "data": data,
                        "timestamp": time.time()
                    }
                logger.info("Cache updated successfully for: %s", name)
            else:
                logger.warning("Background refresh failed for: %s. Cache not updated.", name)
            # Small delay between requests to be gentle on the API
            time.sleep(1)

        logger.info("Background refresh cycle complete. Sleeping for %s seconds.", BACKGROUND_REFRESH_INTERVAL)
        time.sleep(BACKGROUND_REFRESH_INTERVAL)

# ==============================================================================
# === 4. FLASK API ROUTES (THE "INSTANT SERVER")
# ==============================================================================

@app.route("/")
def home():
    return "Smart Farm API Bridge is running."


@app.route("/api/all")
def get_all_cached_data():
    """
    A single, fast endpoint that returns the entire contents of the cache.
    The Flutter app will call this one endpoint to get everything.
    """
    with _cache_lock:
        # Return a copy of the cache to be thread-safe
        return jsonify(dict(_cache))

# Small convenience endpoint to check a single named key
@app.route("/api/get/<string:key>")
def get_cached_key(key: str):
    with _cache_lock:
        v = _cache.get(key)
        if v is None:
            return jsonify({"error": "not found"}), 404
        return jsonify(v)


# Backwards-compatible endpoint(s) the Flutter app expects.
# Example: GET /api/greenhouse/all -> returns combined cached data for greenhouse keys.
@app.route('/api/<string:group>/all')
def get_group_all(group: str):
    keys = GROUP_TO_KEYS.get(group)
    if not keys:
        return jsonify({"error": "unknown group"}), 404

    out = {}
    with _cache_lock:
        # Special-case irrigation: split telemetry by device_id into two logical sources
        if group == 'irrigation':
            # read the raw telemetry cache (single upstream key)
            entry = _cache.get('irrigation_telemetry')
            if not entry:
                # report both groups but mark as not yet cached
                out['irrigation_g2'] = {"error": "not yet cached"}
                out['irrigation_g6'] = {"error": "not yet cached"}
            else:
                data = entry.get('data')
                ts = int(entry.get('timestamp', 0))

                # Extract rows list
                rows = []
                if isinstance(data, dict):
                    if 'data' in data and isinstance(data['data'], dict) and isinstance(data['data'].get('items'), list):
                        rows = data['data']['items']
                    elif isinstance(data.get('items'), list):
                        rows = data.get('items')
                    else:
                        # Not a list-shaped payload; treat as single row
                        rows = [data]
                elif isinstance(data, list):
                    rows = data

                # Partition rows by device_id
                g2 = [r for r in rows if str(r.get('device_id') or r.get('DEVICE_ID') or r.get('Device_Id') or r.get('device')) in ('1', '1.0', '1')]
                g6 = [r for r in rows if str(r.get('device_id') or r.get('DEVICE_ID') or r.get('Device_Id') or r.get('device')) in ('2', '2.0', '2')]

                def mk_payload(rows_list):
                    if rows_list:
                        payload = dict(rows_list[0])
                    else:
                        payload = {"note": "no rows yet"}
                    payload['_source'] = 'live'
                    payload['_fetched_ts'] = ts
                    return payload

                out['irrigation_g2'] = mk_payload(g2)
                out['irrigation_g6'] = mk_payload(g6)
            return jsonify(out)

        # Default behavior for other groups
        for k in keys:
            entry = _cache.get(k)
            if entry is None:
                out[k] = {"error": "not yet cached"}
                continue

            data = entry.get('data')
            ts = int(entry.get('timestamp', 0))

            # Normalize payload: prefer first item in data['items'] when present,
            # otherwise return data as-is. Attach metadata helpers the Flutter
            # client expects: '_source' and '_fetched_ts'.
            payload = None
            if isinstance(data, dict):
                # If this upstream provides a collection with 'items'
                items = None
                if 'data' in data and isinstance(data['data'], dict) and isinstance(data['data'].get('items'), list):
                    items = data['data']['items']
                elif isinstance(data.get('items'), list):
                    items = data.get('items')

                if items and len(items) > 0 and isinstance(items[0], dict):
                    # pick the first item as the representative payload
                    payload = dict(items[0])
                else:
                    # no items array: use the dict as a payload
                    payload = dict(data)
            else:
                # non-dict response (e.g., list or text)
                payload = {"value": data}

            # Attach metadata
            payload['_source'] = 'live'
            payload['_fetched_ts'] = ts
            out[k] = payload

    # Return a flat map of sourceName -> payload so the Flutter client can
    # treat the response as Map<sourceName, payloadMap> and pick a source.
    return jsonify(out)


# Optional: return a single source from a group, e.g. /api/soil/history/soil_g7
@app.route('/api/<string:group>/history/<string:source>')
def get_group_history_source(group: str, source: str):
    # Verify the source belongs to the group. Allow pseudo-sources for irrigation.
    keys = GROUP_TO_KEYS.get(group)
    if not keys:
        return jsonify({"error": "unknown group or source"}), 404
    if not (group == 'irrigation' and source in ('irrigation_g2', 'irrigation_g6')) and source not in keys:
        return jsonify({"error": "unknown group or source"}), 404

    # Support optional ?limit=N
    try:
        limit = int(request.args.get('limit', '10'))
    except Exception:
        limit = 10

    with _cache_lock:
        # For irrigation, the background refresher stores data under the
        # single upstream key 'irrigation_telemetry'. When the client asks for
        # a pseudo-source like 'irrigation_g2' or 'irrigation_g6', read the
        # telemetry cache instead of looking for a non-existent cache key.
        if group == 'irrigation' and source in ('irrigation_g2', 'irrigation_g6'):
            entry = _cache.get('irrigation_telemetry')
        else:
            entry = _cache.get(source)

        if entry is None:
            return jsonify({"error": "not yet cached"}), 404

        data = entry.get('data')

    # Extract rows list from common shapes
    rows = []
    if isinstance(data, dict):
        if 'data' in data and isinstance(data['data'], dict) and isinstance(data['data'].get('items'), list):
            rows = data['data']['items']
        elif isinstance(data.get('items'), list):
            rows = data.get('items')
        else:
            # If it's a map representing a single row, return it as single-element list
            rows = [data]
    elif isinstance(data, list):
        rows = data
    else:
        rows = [data]

    # Return up to 'limit' rows
    # If this is a pseudo irrigation source, filter by device_id
    if group == 'irrigation' and source in ('irrigation_g2', 'irrigation_g6'):
        desired = '1' if source == 'irrigation_g2' else '2'
        filtered = [r for r in rows if str(r.get('device_id') or r.get('DEVICE_ID') or r.get('Device_Id') or r.get('device')) in (desired, desired + '.0')]
        # Fallback: if filtering returned no rows (different key name or values),
        # return the latest rows from the full telemetry list so the UI is not empty.
        if not filtered:
            # Return the most recent 'limit' rows from the original rows list
            return jsonify(list(rows)[-limit:])
        return jsonify(list(filtered)[:limit])

    return jsonify(list(rows)[:limit])


@app.route('/api/soil/history_by_group')
def soil_history_by_group():
    # Consolidate soil groups 7,8,10 and return up to `limit` rows per group.
    # Default per-group limit is 50. Clients may request smaller pages (e.g. ?limit=5)
    # and may pass an `offset` which will be applied per-group (so ?limit=5&offset=10
    # returns rows 10..14 for every group if available).
    try:
        limit = int(request.args.get('limit', '50'))
        offset = int(request.args.get('offset', '0'))
    except Exception:
        limit = 50
        offset = 0
    synthetic = request.args.get('synthetic') in ('1', 'true', 'yes')

    target_groups = ['7', '8', '10']
    collected_by_group: dict[str, list] = {g: [] for g in target_groups}

    # If requested, return synthetic test rows per group for UI testing
    if synthetic:
        now = time.time()
        for g in target_groups:
            for i in range(limit):
                ts = time.strftime('%Y-%m-%dT%H:%M:%S', time.gmtime(now - i * 600)) + 'Z'
                r = {
                    'created_at': ts,
                    'group_id': int(g),
                    'moisture': 30 + i * 2,
                    'temperature': 20.0 + i * 0.5,
                    'ec': 100 + i * 15,
                    'ph': 5.5 + i * 0.1,
                    'nitrogen': 50 + i * 3,
                    'phosphorus': 120 + i * 4,
                    'potassium': str(150 + i * 5),
                    'valid': 'Y'
                }
                collected_by_group[g].append(r)
        # proceed to sort+return below using the synthetic rows

    # Iterate configured soil sources and collect raw rows
    sources = list_sources_for('soil')
    for src in sources:
        upstream = get_upstream_url('soil', src)
        payload = fetch_from_upstream(upstream, name=f"soil_history:{src}")
        if payload is None:
            # try cached slot
            key = f"soil:{src}"
            cached = _cache.get(key, {})
            cached_data = cached.get('data') if isinstance(cached, dict) else None
            if cached_data:
                if isinstance(cached_data, list):
                    rows = [r for r in cached_data if isinstance(r, dict)]
                elif isinstance(cached_data, dict):
                    rows = [cached_data]
                else:
                    rows = []
            else:
                continue
        else:
            rows = []
            if isinstance(payload, dict):
                items = payload.get('items')
                if isinstance(items, list) and items:
                    rows = [r for r in items if isinstance(r, dict)]
                else:
                    rows = [payload]
            elif isinstance(payload, list):
                rows = [r for r in payload if isinstance(r, dict)]
            else:
                continue

        for r in rows:
            gid = r.get('group_id') or r.get('GROUP_ID') or r.get('groupId') or r.get('Group')
            if gid is None:
                continue
            gid_s = str(gid)
            if gid_s in collected_by_group:
                collected_by_group[gid_s].append(r)

    # sort each group's rows newest-first and apply per-group offset/limit
    result_rows: list[dict] = []
    def _created_key(row: dict):
        for k in ('created_at', 'CREATED_AT', 'Timestamp', 'timestamp', 'T'):
            v = row.get(k)
            if v:
                return v
        return ''

    for g in target_groups:
        grp_rows = collected_by_group.get(g, [])
        grp_rows.sort(key=_created_key, reverse=True)
        start = max(0, offset)
        end = start + max(0, limit)
        for r in grp_rows[start:end]:
            result_rows.append(r)

    return jsonify(result_rows)


@app.route('/api/proxy/image')
def proxy_image():
    # Proxy an external image URL so the web client can load cross-origin images.
    url = request.args.get('url')
    if not url:
        return jsonify({"error": "missing url"}), 400
    try:
        r = _session.get(url, timeout=15, stream=True)
        r.raise_for_status()
        content_type = r.headers.get('Content-Type', 'application/octet-stream')
        # Stream the raw bytes through Flask
        resp = make_response(r.content)
        resp.headers['Content-Type'] = content_type
        return resp
    except requests.RequestException as e:
        logger.error('Image proxy failed for %s: %s', url, e)
        return jsonify({"error": "failed to fetch image"}), 502


@app.route('/api/force_refresh/<string:group>/<string:source>', methods=['POST'])
def force_refresh(group: str, source: str):
    # Ensure source belongs to group
    keys = GROUP_TO_KEYS.get(group)
    if not keys or source not in keys:
        return jsonify({"error": "unknown group or source"}), 404

    url = UPSTREAM_URLS.get(source)
    if not url:
        return jsonify({"error": "unknown source url"}), 404

    data = fetch_from_upstream(url)
    if data is None:
        return jsonify({"error": "upstream fetch failed"}), 502

    with _cache_lock:
        _cache[source] = {"data": data, "timestamp": time.time()}

    return jsonify({"ok": True}), 200


@app.route('/api/config/upstreams')
def config_upstreams():
    return jsonify({"upstreams": UPSTREAM_URLS, "groups": GROUP_TO_KEYS})

# ==============================================================================
# === 5. MAIN EXECUTION BLOCK
# ==============================================================================

if __name__ == "__main__":
    print("🚀 Starting a clean, simple, and robust Smart Farm API Server...")

    # Start the background refresher in a separate, daemon thread
    refresher_thread = threading.Thread(target=run_background_refresher, daemon=True)
    refresher_thread.start()

    # Start the Flask web server
    # Use threaded=True so Flask can handle multiple incoming clients while
    # the background refresher is running in a daemon thread.
    app.run(host="0.0.0.0", port=5000, threaded=True)
