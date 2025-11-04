#!/usr/bin/env python3
"""
Local development variant of testapp_clean.py

This file is identical in behavior to `testapp_clean.py` but points all
upstream URLs to a local mock upstream server (mock_upstream.py).

Run sequence for local development:
  1. Start the mock upstream: python mock_upstream.py
  2. Start this local bridge: python testapp_local.py
  3. Point the Flutter app (or curl) at http://127.0.0.1:5000/api/...
"""

import os
import threading
import time
import logging
import requests
from flask import Flask, jsonify, request, Response, make_response
from flask_cors import CORS

# --- Flask App Setup ---
app = Flask(__name__)
CORS(app, resources={r"/api/*": {"origins": "*"}})
logging.basicConfig(level=logging.INFO, format='[%(levelname)s] %(asctime)s: %(message)s')
logger = logging.getLogger("testapp_local")

# Core settings (same defaults as the clean server)
BACKGROUND_REFRESH_INTERVAL = int(os.getenv('BACKGROUND_REFRESH_INTERVAL', '300'))
UPSTREAM_TIMEOUT = int(os.getenv('UPSTREAM_TIMEOUT', '90'))

# Local mock upstream base (mock_upstream.py defaults to 127.0.0.1:5001)
MOCK_BASE = os.getenv('MOCK_BASE', 'http://127.0.0.1:5001')

# Map of upstream source keys to local mock endpoints
UPSTREAM_URLS = {
    "crop_vision_g4": f"{MOCK_BASE}/crop_vision/",
    "crop_vision_g5": f"{MOCK_BASE}/crop_vision/",
    "greenhouse_g1": f"{MOCK_BASE}/greenhouse/",
    "greenhouse_g9": f"{MOCK_BASE}/greenhouse/",
    "irrigation_telemetry": f"{MOCK_BASE}/irrigation/",
    "soil_g7": f"{MOCK_BASE}/soil/",
    "soil_g8": f"{MOCK_BASE}/soil/",
    "soil_g10": f"{MOCK_BASE}/soil/",
}

# Keep the same logical grouping as the production bridge
GROUP_TO_KEYS = {
    "greenhouse": ["greenhouse_g1", "greenhouse_g9"],
    "irrigation": ["irrigation_telemetry"],
    "soil": ["soil_g7", "soil_g8", "soil_g10"],
    "crop_vision": ["crop_vision_g4", "crop_vision_g5"],
}


def list_sources_for(group: str) -> list:
    try:
        g = group.lower()
        keys = GROUP_TO_KEYS.get(g)
        if keys:
            return keys
    except Exception:
        pass
    return [group]


def get_upstream_url(group: str, source: str | None = None) -> str:
    try:
        if source:
            return UPSTREAM_URLS.get(source, '')
        keys = GROUP_TO_KEYS.get(group)
        if keys and len(keys) > 0:
            return UPSTREAM_URLS.get(keys[0], '')
    except Exception:
        pass
    return ''

# In-memory cache and session
_cache = {}
_cache_lock = threading.Lock()
_session = requests.Session()
_session.headers.update({'User-Agent': 'mock-local-client/1.0'})


def fetch_from_upstream(url: str, name: str = None) -> dict | None:
    try:
        response = _session.get(url, timeout=UPSTREAM_TIMEOUT)
        response.raise_for_status()
        try:
            return response.json()
        except ValueError:
            logger.warning("Upstream returned non-JSON for %s; returning raw text.", url)
            return {"_raw_text": response.text}
    except requests.exceptions.RequestException as e:
        logger.error("Upstream fetch failed for %s: %s", url, e)
        return None


def run_background_refresher():
    logger.info("Background refresher thread started (local). Interval: %s seconds.", BACKGROUND_REFRESH_INTERVAL)
    while True:
        logger.info("Starting local background refresh cycle...")
        for name, url in UPSTREAM_URLS.items():
            logger.info("Background refreshing: %s -> %s", name, url)
            data = fetch_from_upstream(url)
            if data is not None:
                with _cache_lock:
                    _cache[name] = {"data": data, "timestamp": time.time()}
                logger.info("Cache updated for: %s", name)
            else:
                logger.warning("Background refresh failed for: %s", name)
            time.sleep(0.25)
        logger.info("Local background refresh complete. Sleeping %s seconds.", BACKGROUND_REFRESH_INTERVAL)
        time.sleep(BACKGROUND_REFRESH_INTERVAL)

# Reuse the same Flask routes as testapp_clean.py; keep behavior consistent
@app.route("/")
def home():
    return "Smart Farm Local API Bridge is running."

@app.route("/api/all")
def get_all_cached_data():
    with _cache_lock:
        return jsonify(dict(_cache))

@app.route("/api/get/<string:key>")
def get_cached_key(key: str):
    with _cache_lock:
        v = _cache.get(key)
        if v is None:
            return jsonify({"error": "not found"}), 404
        return jsonify(v)

@app.route('/api/<string:group>/all')
def get_group_all(group: str):
    keys = GROUP_TO_KEYS.get(group)
    if not keys:
        return jsonify({"error": "unknown group"}), 404
    out = {}
    with _cache_lock:
        if group == 'irrigation':
            entry = _cache.get('irrigation_telemetry')
            if not entry:
                out['irrigation_g2'] = {"error": "not yet cached"}
                out['irrigation_g6'] = {"error": "not yet cached"}
            else:
                data = entry.get('data')
                ts = int(entry.get('timestamp', 0))
                rows = []
                if isinstance(data, dict):
                    if 'data' in data and isinstance(data['data'], dict) and isinstance(data['data'].get('items'), list):
                        rows = data['data']['items']
                    elif isinstance(data.get('items'), list):
                        rows = data.get('items')
                    else:
                        rows = [data]
                elif isinstance(data, list):
                    rows = data
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
        for k in keys:
            entry = _cache.get(k)
            if entry is None:
                out[k] = {"error": "not yet cached"}
                continue
            data = entry.get('data')
            ts = int(entry.get('timestamp', 0))
            payload = None
            if isinstance(data, dict):
                items = None
                if 'data' in data and isinstance(data['data'], dict) and isinstance(data['data'].get('items'), list):
                    items = data['data']['items']
                elif isinstance(data.get('items'), list):
                    items = data.get('items')
                if items and len(items) > 0 and isinstance(items[0], dict):
                    payload = dict(items[0])
                else:
                    payload = dict(data)
            else:
                payload = {"value": data}
            payload['_source'] = 'live'
            payload['_fetched_ts'] = ts
            out[k] = payload
    return jsonify(out)

@app.route('/api/<string:group>/history/<string:source>')
def get_group_history_source(group: str, source: str):
    keys = GROUP_TO_KEYS.get(group)
    if not keys:
        return jsonify({"error": "unknown group or source"}), 404
    if not (group == 'irrigation' and source in ('irrigation_g2', 'irrigation_g6')) and source not in keys:
        return jsonify({"error": "unknown group or source"}), 404
    try:
        limit = int(request.args.get('limit', '10'))
    except Exception:
        limit = 10
    with _cache_lock:
        if group == 'irrigation' and source in ('irrigation_g2', 'irrigation_g6'):
            entry = _cache.get('irrigation_telemetry')
        else:
            entry = _cache.get(source)
        if entry is None:
            return jsonify({"error": "not yet cached"}), 404
        data = entry.get('data')
    rows = []
    if isinstance(data, dict):
        if 'data' in data and isinstance(data['data'], dict) and isinstance(data['data'].get('items'), list):
            rows = data['data']['items']
        elif isinstance(data.get('items'), list):
            rows = data.get('items')
        else:
            rows = [data]
    elif isinstance(data, list):
        rows = data
    else:
        rows = [data]
    if group == 'irrigation' and source in ('irrigation_g2', 'irrigation_g6'):
        desired = '1' if source == 'irrigation_g2' else '2'
        filtered = [r for r in rows if str(r.get('device_id') or r.get('DEVICE_ID') or r.get('Device_Id') or r.get('device')) in (desired, desired + '.0')]
        if not filtered:
            return jsonify(list(rows)[-limit:])
        return jsonify(list(filtered)[:limit])
    return jsonify(list(rows)[:limit])

@app.route('/api/soil/history_by_group')
def soil_history_by_group():
    try:
        limit = int(request.args.get('limit', '50'))
        offset = int(request.args.get('offset', '0'))
    except Exception:
        limit = 50
        offset = 0
    synthetic = request.args.get('synthetic') in ('1', 'true', 'yes')
    target_groups = ['7', '8', '10']
    collected_by_group: dict[str, list] = {g: [] for g in target_groups}
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
    sources = list_sources_for('soil')
    for src in sources:
        upstream = get_upstream_url('soil', src)
        payload = fetch_from_upstream(upstream, name=f"soil_history:{src}")
        if payload is None:
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
    url = request.args.get('url')
    if not url:
        return jsonify({"error": "missing url"}), 400
    try:
        r = _session.get(url, timeout=15, stream=True)
        r.raise_for_status()
        content_type = r.headers.get('Content-Type', 'application/octet-stream')
        resp = make_response(r.content)
        resp.headers['Content-Type'] = content_type
        return resp
    except requests.RequestException as e:
        logger.error('Image proxy failed for %s: %s', url, e)
        return jsonify({"error": "failed to fetch image"}), 502

@app.route('/api/force_refresh/<string:group>/<string:source>', methods=['POST'])
def force_refresh(group: str, source: str):
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

if __name__ == "__main__":
    print("🚀 Starting Smart Farm Local API Server...")
    refresher_thread = threading.Thread(target=run_background_refresher, daemon=True)
    refresher_thread.start()
    app.run(host="0.0.0.0", port=5000, threaded=True)
