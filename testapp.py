#!/usr/bin/env python3
"""
Smart Farm API Bridge: Final Version

This Flask server connects to the default Oracle APEX AutoREST endpoints.
It fetches the entire collection of data for a given table, then uses Python
to sort the data and return only the single most recent record.

This approach bypasses any permissions issues with custom APEX REST handlers
and provides a reliable backend for the Flutter application.
"""

# Import necessary libraries
from flask import Flask, jsonify, request
from flask_cors import CORS
import os
import requests
import warnings
import time
import logging
import threading
from urllib3.exceptions import InsecureRequestWarning
from requests.adapters import HTTPAdapter
from concurrent.futures import ThreadPoolExecutor
import subprocess
import shutil
import json

# Suppress only the single InsecureRequestWarning from urllib3 needed for this script
warnings.simplefilter('ignore', InsecureRequestWarning)


# ==============================================================================
# === 1. CONFIGURATION
# ==============================================================================
app = Flask(__name__)
import io

# Basic logging config (configured early so CORS setup can log)
logging.basicConfig(level=logging.INFO, format='[%(levelname)s] %(asctime)s %(name)s: %(message)s')
logger = logging.getLogger("testapp")

# Allow cross-origin requests for API routes. In production set CORS_ORIGINS
# to a comma-separated list of allowed origins (e.g. https://dashboard.example.com)
CORS_ORIGINS = os.getenv('CORS_ORIGINS')
if CORS_ORIGINS:
    try:
        origins = [o.strip() for o in CORS_ORIGINS.split(',') if o.strip()]
        CORS(app, resources={r"/api/*": {"origins": origins}})
        logger.info('CORS configured for origins: %s', origins)
    except Exception:
        CORS(app, resources={r"/api/*": {"origins": "*"}})
        logger.exception('Failed to parse CORS_ORIGINS; falling back to "*"')
else:
    # default to wildcard for development; set CORS_ORIGINS in production
    CORS(app, resources={r"/api/*": {"origins": "*"}})
    logger.warning('CORS_ORIGINS not set. Defaulting to "*" (development only).')

# Note: logging is configured above so early startup steps can log.

# NOTE: We no longer use APEX_BASE_URL environment overrides. The bridge
# will use absolute Oracle APEX URLs below to avoid accidentally pointing
# at a local mock at 127.0.0.1:5001.

# Authentication and connection options. Configure these using environment variables.
# - APEX_API_KEY : if provided, used for API-key style authentication.
# - APEX_API_KEY_HEADER : header name to send the API key in (default: Authorization)
#   If header name is 'Authorization', the value will be sent as 'Bearer <key>'.
# - APEX_USER / APEX_PASS : basic auth fallback
# - APEX_VERIFY_SSL : 'true' or 'false' (default true)
# - APEX_FETCH_TIMEOUT : seconds to wait for upstream fetch (default 15)
APEX_API_KEY = os.getenv('APEX_API_KEY')
APEX_API_KEY_HEADER = os.getenv('APEX_API_KEY_HEADER', 'Authorization')
APEX_USER = os.getenv('APEX_USER')
APEX_PASS = os.getenv('APEX_PASS')

# Control SSL verification for testing environments behind self-signed certs.
# By default verification is enabled; set APEX_VERIFY_SSL=false to disable.
APEX_VERIFY_SSL = os.getenv('APEX_VERIFY_SSL', 'true').lower() in ('1', 'true', 'yes')

# Fetch timeout (seconds) for upstream requests.
# Default lowered to 30s so background refreshes don't block for long periods
# when an upstream is slow; you can still override via APEX_FETCH_TIMEOUT env.
APEX_FETCH_TIMEOUT = int(os.getenv('APEX_FETCH_TIMEOUT', '30'))

# Allow forcing whether the curl fallback is used when Python requests fails.
# Default to false for production safety; set USE_CURL_FALLBACK=true in dev when
# you explicitly want the system 'curl' binary to be used as a fallback.
USE_CURL_FALLBACK = os.getenv('USE_CURL_FALLBACK', 'false').lower() in ('1', 'true', 'yes')

# Control whether debug/sample payloads and debug endpoints are enabled.
# By default this is disabled in order to avoid returning synthetic data in
# production or live-testing scenarios. Set ALLOW_DEBUG_SAMPLES=true to enable
ALLOW_DEBUG_SAMPLES = os.getenv('ALLOW_DEBUG_SAMPLES', 'false').lower() in ('1', 'true', 'yes')

# A dictionary mapping our clean API names to the main AutoREST table URLs.
# NOTE: We are NOT using the "/latest" suffix here.
# By default we map one logical table name to its APEX collection URL. To support
# multiple sub-groups (multiple APIs per logical group) we allow overriding
# individual upstream URLs with environment variables named using the pattern:
#   API_URL_{GROUP}_{SOURCE}
# where GROUP is the logical group name (e.g. SOIL, GREENHOUSE) and SOURCE is
# the subgroup identifier (e.g. G1, G9, etc). If no explicit env var is found
# we fall back to the generic APEX_BASE_URL/{group}/ collection URL.
# Absolute collection endpoints (do not depend on environment overrides).
ENDPOINTS = {
    "irrigation": "https://oracleapex.com/ords/g3_data/irrigation/",
    "soil": "https://oracleapex.com/ords/g3_data/soil/",
    "crop_vision": "https://oracleapex.com/ords/g3_data/crop_vision/",
    "greenhouse": "https://oracleapex.com/ords/g3_data/greenhouse/"
}

# Optional hard-coded default overrides for specific subgroup endpoints.
# Replaced with the definitive URLs provided by the operator.
DEFAULT_UPSTREAM_OVERRIDES = {
    'crop_vision': {
        'G4': 'https://oracleapex.com/ords/g3_data/crop_vision_g4/',
        'G5': 'https://oracleapex.com/ords/g3_data/crop-vision/crop_g5/'
    },
    'greenhouse': {
        'G1': 'https://oracleapex.com/ords/g3_data/greenhouse_group1/',
        'G9': 'https://oracleapex.com/ords/g3_data/iot/greenhouse/'
    },
    'irrigation': {
        # All irrigation data now comes from the TELEMETRY_DATA table.
        'DEFAULT': 'https://oracleapex.com/ords/g3_data/TELEMETRY_DATA/'
    },
    'soil': {
        # Specific, correct endpoints for each soil group.
        '7': 'https://oracleapex.com/ords/g3_data/groups/data/7',
        '8': 'https://oracleapex.com/ords/g3_data/groups/data/8',
        '10': 'https://oracleapex.com/ords/g3_data/groups/data/10'
    }
}

# Build a list of allowed image URL prefixes to limit the proxy to known upstreams
def _gather_allowed_image_prefixes() -> list[str]:
    # include the absolute collection endpoints we defined above
    prefixes = [v.rstrip('/') for v in ENDPOINTS.values()]
    for grp, mapping in DEFAULT_UPSTREAM_OVERRIDES.items():
        for v in mapping.values():
            try:
                prefixes.append(str(v).rstrip('/'))
            except Exception:
                continue
    # dedupe
    out = []
    for p in prefixes:
        if p not in out:
            out.append(p)
    return out

ALLOWED_IMAGE_PREFIXES = _gather_allowed_image_prefixes()


def get_upstream_url(group: str, source: str | None = None) -> str:
    """Return the upstream URL for a given logical group and optional source.
    If an environment variable API_URL_{GROUP}_{SOURCE} exists it is used.
    Otherwise fall back to the default collection URL for the group.
    """
    if source:
        env_key = f"API_URL_{group.upper()}_{source.upper()}"
        override = os.getenv(env_key)
        if override:
            logger.info("Using override for %s/%s from env %s", group, source, env_key)
            return override

        # Check built-in defaults provided in DEFAULT_UPSTREAM_OVERRIDES
        grp = group.lower()
        if grp in DEFAULT_UPSTREAM_OVERRIDES:
            # match case-insensitively against provided source keys
            for k, v in DEFAULT_UPSTREAM_OVERRIDES[grp].items():
                if k.upper() == source.upper():
                    logger.info("Using default override for %s/%s from DEFAULT_UPSTREAM_OVERRIDES", group, source)
                    return v

    # fall back to base collection URL (use absolute collection endpoints)
    # If caller didn't specify a source but a 'DEFAULT' mapping exists, use it.
    grp = group.lower()
    if grp in DEFAULT_UPSTREAM_OVERRIDES and 'DEFAULT' in DEFAULT_UPSTREAM_OVERRIDES[grp]:
        return DEFAULT_UPSTREAM_OVERRIDES[grp]['DEFAULT']

    # Use the absolute collection URL we configured in ENDPOINTS or
    # fall back to the canonical oracleapex base path for the group.
    return ENDPOINTS.get(group, f"https://oracleapex.com/ords/g3_data/{group}/")


# Mapping between telemetry device_id and logical group id used by the Flutter UI.
# Device 1 -> group 2, Device 2 -> group 6 per operator instruction.
IRRIGATION_DEVICE_TO_GROUP = {
    1: '2',
    2: '6'
}

# Reverse mapping for convenience: group -> device id
IRRIGATION_GROUP_TO_DEVICE = {v: k for k, v in IRRIGATION_DEVICE_TO_GROUP.items()}


def list_sources_for(group: str) -> list[str]:
    """Discover configured sources for a group by scanning environment variables
    named API_URL_{GROUP}_{SOURCE}. If none are found we return ['default'].
    """
    prefix = f"API_URL_{group.upper()}_"
    sources = []
    for k in os.environ.keys():
        if k.upper().startswith(prefix):
            suffix = k[len(prefix):]
            if suffix:
                sources.append(suffix)
    # Allow explicit CSV list via API_SOURCES_{GROUP}
    env_list = os.getenv(f"API_SOURCES_{group.upper()}")
    if env_list:
        for s in [x.strip() for x in env_list.split(',') if x.strip()]:
            if s not in sources:
                sources.append(s)
    # Include default override sources if configured in-code
    grp = group.lower()
    if grp in DEFAULT_UPSTREAM_OVERRIDES:
        for s in DEFAULT_UPSTREAM_OVERRIDES[grp].keys():
            if s not in sources:
                sources.append(s)
    if not sources:
        return ['DEFAULT']
    return sources


@app.route('/api/config/upstreams', methods=['GET'])
def config_upstreams():
    """Return the effective upstream URL mapping the bridge will use.
    This merges environment variable overrides with the built-in defaults so
    callers can quickly inspect which upstreams are available.
    """
    mapping = {}
    groups = set(list(ENDPOINTS.keys()) + list(DEFAULT_UPSTREAM_OVERRIDES.keys()))
    for g in groups:
        g_lower = g.lower()
        sources = list_sources_for(g_lower)
        mapping[g_lower] = {}
        for src in sources:
            # env override takes precedence
            g_upper = g_lower.upper()
            env_key = f"API_URL_{g_upper}_{src.upper()}"
            env_val = os.getenv(env_key)
            if env_val:
                mapping[g_lower][src] = {'url': env_val, 'source': 'env'}
                continue

            # check built-in defaults
            if g_lower in DEFAULT_UPSTREAM_OVERRIDES and src in DEFAULT_UPSTREAM_OVERRIDES[g_lower]:
                mapping[g_lower][src] = {'url': DEFAULT_UPSTREAM_OVERRIDES[g_lower][src], 'source': 'default_override'}
                continue

            # finally the generic collection URL
            mapping[g_lower][src] = {'url': get_upstream_url(g_lower, None), 'source': 'collection_default'}

    return jsonify(mapping)


def normalize_payload(group: str, source: str | None, payload: dict) -> dict:
    """
    Return a clean, normalized, and canonical copy of a data record.
    This function now perfectly maps the known database schemas to the keys
    expected by the Flutter application's models.
    """
    if not isinstance(payload, dict):
        return {}

    # If the payload contains an 'items' list, use the first item as the record.
    # Our optimized APEX queries now only return one item (or one per group for soil).
    items = payload.get('items', [])
    if not items:
        # If 'items' is empty but payload has data, use the payload itself.
        if 'ID' in payload or 'GROUP_ID' in payload or 'CROP_ID' in payload:
            record = payload
        else:
            return {} # No valid data found
    else:
        record = items[0]
        
    if not isinstance(record, dict):
        return {}

    def _as_float(v):
        try:
            return float(v) if v is not None else None
        except (ValueError, TypeError):
            return None

    def _as_str(v):
        return str(v) if v is not None else None

    # --- FINAL NORMALIZATION LOGIC ---
    normalized = {}
    group = group.lower()

    if group == 'soil':
        normalized['timestamp'] = _as_str(record.get('CREATED_AT'))
        normalized['groupId'] = record.get('GROUP_ID')
        normalized['moisture'] = _as_float(record.get('MOISTURE'))
        normalized['temperature'] = _as_float(record.get('TEMPERATURE'))
        normalized['ec'] = _as_float(record.get('EC'))
        normalized['ph'] = _as_float(record.get('PH'))
        normalized['nitrogen'] = _as_float(record.get('NITROGEN'))
        normalized['phosphorus'] = _as_float(record.get('PHOSPHORUS'))
        normalized['potassium'] = _as_float(record.get('POTASSIUM'))

    elif group == 'irrigation':
        # Telemetry payloads may use different names: timestamp, flow_rate, total_flow, pump_status
        normalized['timestamp'] = _as_str(record.get('TIMESTAMP') or record.get('timestamp'))
        # flow rate may be provided as flow_rate (L/min) or flow_rate_Lmin
        normalized['flowRate'] = _as_float(record.get('FLOW_RATE_LMIN') or record.get('flow_rate_Lmin') or record.get('flow_rate'))
        normalized['totalVolume'] = _as_float(record.get('TOTAL_VOLUME') or record.get('total_volume') or record.get('total_flow'))
        normalized['waterUsedCycle'] = _as_float(record.get('WATER_USED_CYCLE') or record.get('water_used_cycle'))
        normalized['pumpState'] = _as_str(record.get('PUMP_STATE') or record.get('pump_state') or record.get('pump_status'))
        # expose the device id when present so callers can map to logical groups
        normalized['device_id'] = record.get('device_id') or record.get('DEVICE_ID')

    elif group == 'greenhouse':
        # Handle different greenhouse schemas based on source
        if source and source.upper() == 'G1':
            normalized['timestamp'] = _as_str(record.get('T'))
            normalized['temperature'] = _as_float(record.get('TEMP'))
            normalized['humidity'] = _as_float(record.get('HUM'))
            normalized['light'] = _as_float(record.get('LIGHT'))
            normalized['airPressure'] = _as_float(record.get('AIR_PRESSURE'))
            normalized['co2'] = _as_float(record.get('CO2'))
        else: # Assumes G9 or IOT schema
            normalized['timestamp'] = _as_str(record.get('TIMESTAMP_READING'))
            normalized['temperature'] = _as_float(record.get('TEMPERATURE_DHT22') or record.get('TEMPERATURE_BMP280'))
            normalized['humidity'] = _as_float(record.get('HUMIDITY'))
            normalized['pressure'] = _as_float(record.get('PRESSURE'))

    elif group == 'crop_vision':
        # Handle different crop vision schemas
        if source and source.upper() == 'G4':
            normalized['cropId'] = _as_str(record.get('CROP_ID') or record.get('crop_id'))
            normalized['disease'] = _as_str(record.get('DISEASE') or record.get('disease') or record.get('key_name') or record.get('keyName'))
            normalized['growthStage'] = _as_str(record.get('GROWTH_STAGE') or record.get('growth_stage') or record.get('growthStage'))
            normalized['recommendation'] = _as_str(record.get('RECOMMENDATIONS') or record.get('RECOMMENDATION') or record.get('recommendation'))
            # Try to expose a usable imageUrl for G4 too. Upstream sources sometimes
            # return only a filename; when that's the case construct a full URL
            # by joining the upstream collection URL with the filename so the
            # Flutter client can render thumbnails consistently with G5.
            img_val = record.get('IMAGE_URL') or record.get('image_url') or record.get('imageUrl') or record.get('IMAGE') or record.get('image')
            if img_val is not None:
                img_s = _as_str(img_val)
                if img_s and not img_s.lower().startswith('http'):
                    try:
                        upstream = get_upstream_url(group, source)
                        if upstream.endswith('/') and not img_s.startswith('/'):
                            full = upstream + img_s
                        else:
                            full = upstream.rstrip('/') + '/' + img_s.lstrip('/')
                        normalized['imageUrl'] = full
                    except Exception:
                        normalized['imageUrl'] = img_s
                else:
                    normalized['imageUrl'] = img_s
        else: # Assumes G5 schema
            # Prefer canonical uppercase DB column names, but fall back to
            # common snake_case or camelCase names seen in raw history rows.
            normalized['cropId'] = _as_str(record.get('CROP_ID') or record.get('crop_id') or record.get('Crop_ID'))
            normalized['disease'] = _as_str(record.get('DISEASE') or record.get('disease') or record.get('key_name') or record.get('keyName'))
            normalized['recommendation'] = _as_str(record.get('RECOMMENDATION') or record.get('recommendation'))
            normalized['imageUrl'] = _as_str(record.get('IMAGE_URL') or record.get('image_url') or record.get('imageUrl'))

    # Add metadata from the bridge for debugging
    if '_source' in payload:
        normalized['_source'] = payload['_source']
    if '_fetched_ts' in payload:
        normalized['_fetched_ts'] = payload['_fetched_ts']

    return normalized


@app.route('/api/<string:group>/all', methods=['GET'])
def get_all_group(group: str):
    """Return latest payloads for all configured sources under a group.
    Non-blocking: return cached entries immediately; if a source is missing or stale
    schedule a background refresh and return a 'refreshing' placeholder so callers
    don't block waiting on upstream fetches.
    """
    group = group.lower()
    sources = list_sources_for(group)
    result = {}
    now = time.time()

    # Special handling for irrigation (consolidated telemetry) kept synchronous for now.
    if group == 'irrigation':
        upstream = get_upstream_url(group, None)
        payload = fetch_from_apex(upstream, name=f"telemetry:{group}")
        if payload is not None:
            items = []
            if isinstance(payload, dict) and isinstance(payload.get('items'), list):
                items = [r for r in payload.get('items') if isinstance(r, dict)]
            elif isinstance(payload, list):
                items = [r for r in payload if isinstance(r, dict)]

            groups_map: dict[str, list] = {}
            for r in items:
                did = None
                try:
                    did = int(r.get('device_id') or r.get('DEVICE_ID'))
                except Exception:
                    try:
                        did = int(str(r.get('device_id') or r.get('DEVICE_ID')))
                    except Exception:
                        did = None
                gid = IRRIGATION_DEVICE_TO_GROUP.get(did) if did is not None else None
                if gid is None:
                    gid = str(did) if did is not None else 'unknown'
                groups_map.setdefault(gid, []).append(r)

            for gid, rows_list in groups_map.items():
                def _ts_key(rr: dict):
                    for k in ('timestamp', 'TIMESTAMP', 'created_at', 'CREATED_AT'):
                        v = rr.get(k)
                        if v:
                            return v
                    return ''
                rows_list.sort(key=_ts_key, reverse=True)
                top = rows_list[0] if rows_list else None
                if top is None:
                    continue
                try:
                    norm = normalize_payload(group, gid, {'items': [top]})
                except Exception:
                    norm = {}
                resp = dict(norm) if isinstance(norm, dict) else {"data": norm}
                resp["_source"] = "live"
                resp["_fetched_ts"] = int(now)
                result[gid] = resp
            return jsonify(result)

    # For other groups: non-blocking behavior
    for src in sources:
        key = _cache_key(group, src)
        # Ensure cache entry exists
        entry = _cache.setdefault(key, {"ts": 0, "data": None})

        # If fresh cached data available -> return it immediately
        try:
            if entry["data"] is not None and (now - entry["ts"] < CACHE_TTL):
                payload = dict(entry["data"]) if isinstance(entry["data"], dict) else {"data": entry["data"]}
                payload["_source"] = "cache"
                payload["_cached_age_s"] = int(now - entry["ts"])
                result[src] = payload
                continue
        except Exception:
            # In case of unexpected cache shape, treat as missing and trigger refresh.
            logger.exception("Cache read error for %s", key)

        # No fresh cache: return a non-blocking placeholder and start background refresh
        result[src] = {"_source": "refreshing"}

        # Fire-and-forget background refresh so we don't block the request
        try:
            # Submit to the single shared executor which limits concurrency and
            # queues additional work. This reduces the simultaneous connections
            # created against upstream APEX and avoids saturating the urllib3
            # connection pool.
            try:
                _executor.submit(_background_refresh, group, src)
            except Exception:
                # fallback to a daemon thread if executor is unavailable
                threading.Thread(target=_background_refresh, args=(group, src), daemon=True).start()
        except Exception:
            logger.exception("Failed to start background refresh for %s", key)

    return jsonify(result)


@app.route('/api/<string:group>/history/<string:source>', methods=['GET'])
def get_group_history(group: str, source: str):
    """Return up to 10 raw upstream rows for a specific group:source.
    This endpoint intentionally returns raw DB column shapes (no normalization)
    so the frontend can render complete tables preserving original column names.
    """
    group = group.lower()
    upstream = get_upstream_url(group, source)
    payload = fetch_from_apex(upstream, name=f"history:{group}:{source}")
    if payload is None:
        # Upstream failed. Attempt to return cached raw rows if available so
        # the dashboard can remain functional when upstream APEX is flaky.
        key = _cache_key(group, source)
        cached = _cache.get(key, {})
        data = cached.get('data') if isinstance(cached, dict) else None
        if data:
            # If the cached data is a normalized dict, wrap it into a list so
            # the response shape matches the usual history endpoint (list of rows).
            if isinstance(data, dict):
                return jsonify([data])
            elif isinstance(data, list):
                return jsonify(data[:10])
        last_err = _last_fetch_errors.get(f"history:{group}:{source}")
        return jsonify({"error": "Upstream fetch failed", "last_error": last_err}), 503

    # Normalize the upstream shape into a list of row dicts (raw)
    rows = []
    if isinstance(payload, dict):
        items = payload.get('items')
        if isinstance(items, list) and items:
            rows = [r for r in items if isinstance(r, dict)]
        else:
            # payload may be a single record dict
            rows = [payload]
    elif isinstance(payload, list):
        rows = [r for r in payload if isinstance(r, dict)]
    else:
        return jsonify({"error": "Unexpected upstream payload shape"}), 502

    # Return the first 10 rows (assumed most recent are first when upstream returns ordered results)
    limited = rows[:10]

    # If this is crop_vision G4, augment raw rows with an imageUrl field so
    # the frontend can render thumbnails even when upstream returns only a
    # filename or uses inconsistent column names. We build a full URL by
    # joining the upstream collection URL with the filename when necessary.
    try:
        if group.lower() == 'crop_vision' and source and source.upper() == 'G4':
            upstream = get_upstream_url(group, source)
            for r in limited:
                img_val = r.get('IMAGE_URL') or r.get('image_url') or r.get('imageUrl') or r.get('IMAGE') or r.get('image') or r.get('file_name')
                if img_val is None:
                    continue
                img_s = str(img_val)
                if img_s and not img_s.lower().startswith('http'):
                    # join with upstream collection URL
                    if upstream.endswith('/') and not img_s.startswith('/'):
                        full = upstream + img_s
                    else:
                        full = upstream.rstrip('/') + '/' + img_s.lstrip('/')
                    r['imageUrl'] = full
                else:
                    r['imageUrl'] = img_s
    except Exception:
        pass

    return jsonify(limited)


@app.route('/api/soil/history_by_group', methods=['GET'])
def soil_history_by_group():
    """Consolidated history endpoint for soil groups 7,8,10.
    Queries configured soil upstream sources, collects raw rows, groups
    them by numeric group_id and returns up to `limit` rows per group.
    This endpoint is intended for the Flutter client to simplify fetching
    history for multiple groups in one call.
    """
    # desired target groups in order
    target_groups = ['7', '8', '10']
    try:
        # Increase default limit so consolidated soil history returns more
        # rows per group by default (was 5, now 50). Clients may still
        # request a different value using ?limit=N.
        limit = int(request.args.get('limit', '50'))
    except Exception:
        limit = 5

    collected_by_group: dict[str, list] = {g: [] for g in target_groups}
    # If requested, return synthetic test rows per group to aid UI testing.
    # Use ?synthetic=1 to enable. This is intentionally local-only and does
    # not affect regular upstream fetch behavior.
    synthetic = str(request.args.get('synthetic', '')).lower() in ('1', 'true', 'yes')
    # Only honor synthetic parameter when explicitly allowed by environment.
    if synthetic and not ALLOW_DEBUG_SAMPLES:
        # If synthetic usage is disabled, ignore and continue to real upstream fetches.
        synthetic = False

    if synthetic:
        now = time.time()
        for g in target_groups:
            for i in range(limit):
                ts = time.strftime('%Y-%m-%dT%H:%M:%S', time.gmtime(now - i * 600)) + 'Z'
                # Simple deterministic synthetic values that vary per index
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
    sources = list_sources_for('soil')
    # Iterate configured soil sources and collect raw rows
    for src in sources:
        upstream = get_upstream_url('soil', src)
        payload = fetch_from_apex(upstream, name=f"soil_history:{src}")
        if payload is None:
            # no data from this source; try to use cached slot so UI can show
            # at least one recent row when upstream is unavailable.
            key = _cache_key('soil', src)
            cached = _cache.get(key, {})
            cached_data = cached.get('data') if isinstance(cached, dict) else None
            if cached_data:
                # If cached_data is a list of rows, extend; if dict, append
                if isinstance(cached_data, list):
                    rows = [r for r in cached_data if isinstance(r, dict)]
                elif isinstance(cached_data, dict):
                    rows = [cached_data]
                else:
                    rows = []
            else:
                # no cached data either; skip
                continue

        rows = []
        if isinstance(payload, dict):
            items = payload.get('items')
            if isinstance(items, list) and items:
                rows = [r for r in items if isinstance(r, dict)]
            else:
                # single-record dict
                rows = [payload]
        elif isinstance(payload, list):
            rows = [r for r in payload if isinstance(r, dict)]
        else:
            continue

        for r in rows:
            # flexible key names for group id
            gid = r.get('group_id') or r.get('GROUP_ID') or r.get('groupId') or r.get('Group')
            if gid is None:
                continue
            gid_s = str(gid)
            if gid_s in collected_by_group:
                collected_by_group[gid_s].append(r)

    # sort each group's rows newest-first by created_at-like fields
    def _created_key(row: dict):
        for k in ('created_at', 'CREATED_AT', 'Timestamp', 'timestamp', 'T'):
            v = row.get(k)
            if v:
                return v
        return ''

    result_rows: list[dict] = []
    for g in target_groups:
        grp_rows = collected_by_group.get(g, [])
        grp_rows.sort(key=_created_key, reverse=True)
        # take up to limit
        for r in grp_rows[:limit]:
            result_rows.append(r)

    return jsonify(result_rows)


@app.route('/api/proxy/image', methods=['GET'])
def proxy_image():
    """Proxy an image URL from permitted upstreams and return raw bytes.
    Query param: url=<encoded absolute url>
    Only URLs starting with one of ALLOWED_IMAGE_PREFIXES are permitted to
    avoid creating an open proxy. Returns image bytes with the original
    Content-Type and a short Cache-Control header.
    """
    url = request.args.get('url')
    if not url:
        return jsonify({'error': 'missing url parameter'}), 400

    # basic validation
    if not (url.lower().startswith('http://') or url.lower().startswith('https://')):
        return jsonify({'error': 'invalid url scheme'}), 400

    # ensure the URL is under an allowed prefix
    allowed = False
    for p in ALLOWED_IMAGE_PREFIXES:
        if url.startswith(p):
            allowed = True
            break
    if not allowed:
        logger.warning('Proxy image blocked for url=%s (not in allowed prefixes)', url)
        return jsonify({'error': 'url not allowed'}), 403

    try:
        resp = _session.get(url, timeout=max(int(APEX_FETCH_TIMEOUT), 8), verify=VERIFY_SSL)
    except requests.RequestException as ex:
        logger.warning('Proxy fetch error for %s: %s', url, ex)
        return jsonify({'error': 'upstream fetch failed', 'detail': str(ex)}), 502

    if resp.status_code != 200:
        logger.warning('Proxy upstream returned status %s for %s', resp.status_code, url)
        return jsonify({'error': 'upstream returned error', 'status': resp.status_code}), 502

    ctype = resp.headers.get('content-type', 'application/octet-stream')
    # Require image content types
    if not ctype.lower().startswith('image'):
        logger.warning('Proxy upstream content-type not image for %s: %s', url, ctype)
        return jsonify({'error': 'upstream did not return an image', 'content-type': ctype}), 415

    # Return raw bytes with proper mimetype
    r = app.response_class(resp.content, mimetype=ctype)
    # allow short caching at edge to reduce repeated loads
    r.headers['Cache-Control'] = 'public, max-age=300'
    return r

# Runtime settings for robust fetches
# Sensible defaults: allow a few retries with a moderate initial backoff.
MAX_RETRIES = int(os.getenv('MAX_RETRIES', '3'))
INITIAL_BACKOFF = float(os.getenv('INITIAL_BACKOFF', '2.0'))  # seconds
# CACHE_TTL default is higher for local development (avoid immediate staleness)
CACHE_TTL = int(os.getenv('CACHE_TTL', '300'))  # seconds: how long to keep successful fetch in memory

# Simple in-memory cache keyed by endpoint_name
_cache = {name: {"ts": 0, "data": None} for name in ENDPOINTS.keys()}
# (Monitoring variables removed: we no longer track per-upstream last-error
# and last-success timestamps to keep the server simpler and easier to
# reason about in development. Errors are still logged.)
# Reference to the background refresher thread (set at startup when launched)
_refresher_thread = None
# Event used to signal background threads to exit cleanly on shutdown.
_shutdown_event = threading.Event()

# Lightweight containers for last-fetch diagnostics. These are intentionally
# simple dicts so older code paths that reference them won't raise NameError.
# We avoid complex monitoring logic in this simplified bridge.
_last_fetch_errors: dict = {}
_last_fetch_success_ts: dict = {}

# Create a Session with sane headers to reduce chance of being blocked
_session = requests.Session()
_session.headers.update({
    'User-Agent': 'Mozilla/5.0 (compatible; SmartFarm/1.0; +https://example.org/)',
    'Accept': 'application/json, text/plain, */*',
    'Accept-Language': 'en-US,en;q=0.9',
})

# Configure HTTPAdapter connection pool sizes and mount on session. Adjust
# pool sizes to match our concurrency limits so urllib3 doesn't log "connection
# pool is full" warnings when many background refreshes are queued.
# Reduce default concurrency to be less aggressive with slow upstreams.
MAX_CONCURRENT_FETCHES = int(os.getenv('MAX_CONCURRENT_FETCHES', '2'))
adapter = HTTPAdapter(pool_connections=max(2, MAX_CONCURRENT_FETCHES), pool_maxsize=max(2, MAX_CONCURRENT_FETCHES))
_session.mount('https://', adapter)
_session.mount('http://', adapter)

# Thread pool used to run background refreshes submitted from web handlers.
# This avoids spawning unbounded threads when many clients trigger refreshes
# simultaneously (the root cause of the connection-pool warnings).
_executor = ThreadPoolExecutor(max_workers=MAX_CONCURRENT_FETCHES)

# Track consecutive failures and last failure time per group:source so the
# scheduled refresher can back off from repeatedly trying an endpoint that
# keeps timing out. Values tuned for development; make configurable later.
_consecutive_failures: dict[str, int] = {}
_last_failure_ts: dict[str, float] = {}
FAIL_SKIP_THRESHOLD = int(os.getenv('FAIL_SKIP_THRESHOLD', '2'))
FAIL_SKIP_WINDOW = int(os.getenv('FAIL_SKIP_WINDOW', '600'))  # seconds to skip after threshold reached

# Attach authentication if provided. Prefer API key header, otherwise basic auth.
if APEX_API_KEY:
    if APEX_API_KEY_HEADER.lower() == 'authorization':
        # use Bearer schema if sending to Authorization header
        _session.headers['Authorization'] = f'Bearer {APEX_API_KEY}'
    else:
        _session.headers[APEX_API_KEY_HEADER] = APEX_API_KEY
    logger.info('APEX: using API key header for upstream requests (header=%s)', APEX_API_KEY_HEADER)
elif APEX_USER and APEX_PASS:
    _session.auth = (APEX_USER, APEX_PASS)
    logger.info('APEX: using basic auth for upstream requests (user provided)')
else:
    logger.info('APEX: no auth configured; performing anonymous requests')

# Global verify flag for requests (useful for self-signed certs in testing).
VERIFY_SSL = bool(APEX_VERIFY_SSL)
logger.info('APEX: default_base=%s verify_ssl=%s fetch_timeout=%s', 'https://oracleapex.com/ords/g3_data', VERIFY_SSL, APEX_FETCH_TIMEOUT)
logger.info('ALLOW_DEBUG_SAMPLES=%s', ALLOW_DEBUG_SAMPLES)

# --- Forwarding configuration: optionally forward normalized records back to an APEX
# endpoint so the canonical APEX dataset stays in sync. Controlled with env vars:
# - ENABLE_APEX_FORWARD=true to enable forwarding
# - APEX_FORWARD_URL a default target URL to POST normalized payloads
# - APEX_FORWARD_{GROUP} an optional per-group override
ENABLE_APEX_FORWARD = os.getenv('ENABLE_APEX_FORWARD', 'false').lower() in ('1', 'true', 'yes')
APEX_FORWARD_URL = os.getenv('APEX_FORWARD_URL')

# Track last forward errors for debugging
_last_forward_errors: dict = {}

# ==============================================================================
# === 2. HELPER FUNCTION
# ==============================================================================
def fetch_from_apex(url: str, name: str = None) -> dict | None:
    """
    Robust fetcher: use session with browser-like headers, retries with exponential backoff,
    and detailed logging. Returns parsed JSON (dict) on success, or None on failure.
    """
    name = name or url
    backoff = INITIAL_BACKOFF
    last_exc = None
    overall_start = time.time()

    for attempt in range(1, MAX_RETRIES + 1):
        attempt_start = time.time()
        try:
            # Respect configured SSL verification and timeout settings.
            # Use APEX_FETCH_TIMEOUT (seconds) and VERIFY_SSL.
            resp = _session.get(url, timeout=APEX_FETCH_TIMEOUT, verify=VERIFY_SSL)
            elapsed = time.time() - attempt_start
            logger.debug("Attempt %d to fetch %s -> %s (elapsed=%.2fs)", attempt, url, resp.status_code, elapsed)
            if resp.status_code == 200:
                try:
                    data = resp.json()
                    # record successful fetch timestamp for this logical name
                    try:
                        _last_fetch_success_ts[name] = int(time.time())
                        # clear any previous error state for this name
                        _last_fetch_errors.pop(name, None)
                    except Exception:
                        pass
                    logger.info("Fetched %s (attempt=%d) in %.2fs", url, attempt, elapsed)
                    return data
                except ValueError:
                    logger.warning("Non-JSON response from %s (len=%d). First 300 chars: %s",
                                   url, len(resp.text), resp.text[:300])
                    return None
            elif resp.status_code == 403:
                logger.warning("403 Forbidden when fetching %s (attempt %d). Response body (truncated): %s",
                               url, attempt, resp.text[:400])
                # Don't retry for 403; likely blocked by upstream rules.
                return None
            else:
                logger.warning("HTTP %d from %s (attempt %d). Body (truncated): %s",
                               resp.status_code, url, attempt, resp.text[:300])
        except requests.RequestException as ex:
            last_exc = ex
            elapsed = time.time() - attempt_start
            logger.warning("Network error fetching %s (attempt %d, elapsed=%.2fs): %s", url, attempt, elapsed, ex)

        # Exponential backoff before retrying
        time.sleep(backoff)
        backoff *= 2

    total_elapsed = time.time() - overall_start
    logger.error("Failed to fetch %s after %d attempts (total_elapsed=%.2fs). Last error: %s", url, MAX_RETRIES, total_elapsed, last_exc)
    # Record the last exception for the given logical name for easier debugging
    try:
        _last_fetch_errors[name or url] = str(last_exc)
    except Exception:
        _last_fetch_errors[name or url] = repr(last_exc)
    # Simplified behavior: do not attempt system curl fallback; return failure.
    return None


# Note: a /api/health route previously provided detailed monitoring info.
# To keep this bridge simple and avoid coupling server-side monitoring with
# UI availability, that route and its detailed state summary have been
# removed. Errors are still logged to the server logs.

# ==============================================================================
# === 3. API ROUTES (exposed to the Flutter app)
# ==============================================================================
def _cache_key(group: str, source: str | None = None) -> str:
    return f"{group}:{source or 'default'}"


def _background_refresh(group: str, source: str | None = None):
    key = _cache_key(group, source)
    try:
        # Ensure the cache slot exists for this group:source key. Without this
        # setdefault a background thread writing to _cache[key] can raise
        # KeyError if the key hasn't been created yet (we initialize only
        # top-level endpoint keys at startup). Calling setdefault makes the
        # background path consistent with the synchronous refresh which uses
        # setdefault before writing.
        _cache.setdefault(key, {"ts": 0, "data": None})

        upstream = get_upstream_url(group, source)
        payload = fetch_from_apex(upstream, name=key)
        if payload is not None:
            # Normalize before storing so consumers receive the canonical shape
            try:
                payload = normalize_payload(group, source, payload)
            except Exception:
                pass
            _cache[key]["data"] = payload
            _cache[key]["ts"] = time.time()
            logger.info("Background refresh succeeded for %s", key)
            # reset failure counters on success
            try:
                _consecutive_failures.pop(key, None)
                _last_failure_ts.pop(key, None)
            except Exception:
                pass
        else:
            logger.warning("Background refresh did not return data for %s", key)
            # record a failure for backoff logic
            try:
                _consecutive_failures[key] = (_consecutive_failures.get(key, 0) + 1)
                _last_failure_ts[key] = time.time()
            except Exception:
                pass
    except Exception as ex:
        logger.exception("Background refresh error for %s: %s", key, ex)
        # record failure for exception path as well
        try:
            _consecutive_failures[key] = (_consecutive_failures.get(key, 0) + 1)
            _last_failure_ts[key] = time.time()
        except Exception:
            pass


def get_debug_sample(endpoint_name: str) -> dict | None:
    """
    Return a synthetic sample payload shaped like the real data for UI testing.
    Returns None if the endpoint_name is unknown.
    """
    ts = time.strftime('%Y-%m-%dT%H:%M:%S')
    if endpoint_name == 'greenhouse':
        return {
            'Timestamp': ts,
            'Temperature': 22.8,
            'Humidity': 48.0,
            'Pressure': 101.2,
            'CO': 0,
            'lpg': 0,
            'Smoke': 0,
            'CO2': 410,
            'Light_Intensity': 980,
            'methane_level': 0,
            'propane_level': 0,
            'butane_level': 0,
            'hydrogen_level': 0,
            'alcohol_level': 0,
            'ammonia_level': 0,
            'benzene_level': 0,
            'toluene_level': 0,
        }
    if endpoint_name == 'irrigation':
        return {
            'timestamp': ts,
            'flow_rate_Lmin': 35.0,
            'total_volume': 87.6,
            'water_used_cycle': 4.2,
            'pump_state': 'OFF',
        }
    if endpoint_name == 'soil':
        return {
            'Timestamp': ts,
            'Moisture': 33.4,
            'Temperature': 19.5,
            'EC': 1.2,
            'pH': 6.5,
            'Nitrogen(N)': 2.1,
            'Phosphorus(P)': 0.8,
            'Potassium(K)': 1.5,
        }
    if endpoint_name == 'crop_vision':
        return {
            'Timestamp': ts,
            'Crop_ID': 'field-1',
            'Disease': 'none',
            'Growth_stage': 'vegetative',
            'Pest_count': 0,
            'Recommendation': 'none',
        }
    return None

@app.route("/", methods=["GET"])
def home():
    """A simple health-check endpoint."""
    return jsonify({
        "status": "running",
        "message": "Smart Farm API Server is active.",
    })


# Note: a /api/health endpoint was previously implemented to provide
# per-upstream monitoring information. That logic has been removed to
# simplify the bridge and avoid coupling server-side monitoring state
# with UI availability logic in the Flutter app.


# Make debug cache-fill endpoints available unconditionally so UI testing can
# seed the in-memory cache without toggling ALLOW_DEBUG_SAMPLES. These are
# intentionally lightweight and only write to the local in-memory cache.
@app.route('/api/fill_cache/<string:endpoint_name>', methods=['POST'])
def fill_cache(endpoint_name: str):
    """Fill the in-memory cache with a debug sample for the given endpoint.
    Useful for local UI testing when upstream is unavailable.
    """
    if endpoint_name not in ENDPOINTS:
        return jsonify({"error": f"Invalid endpoint: {endpoint_name}"}), 404
    sample = get_debug_sample(endpoint_name)
    if sample is None:
        return jsonify({"error": "No debug sample for endpoint"}), 404
    # Backwards-compatible behavior: populate only the top-level slot for callers.
    # Keep behavior minimal and predictable to avoid unexpected side-effects.
    _cache.setdefault(endpoint_name, {"ts": 0, "data": None})
    _cache[endpoint_name]["data"] = sample
    _cache[endpoint_name]["ts"] = time.time()

    logger.info("Cache filled for %s", endpoint_name)
    return jsonify({"status": "ok", "endpoint": endpoint_name, "_source": "debug_cached"})


@app.route('/api/fill_cache/<string:group>/<string:source>', methods=['POST'])
def fill_cache_group_source(group: str, source: str):
    key = _cache_key(group, source)
    sample = get_debug_sample(group)
    if sample is None:
        return jsonify({"error": "No debug sample for group"}), 404
    _cache.setdefault(key, {"ts": 0, "data": None})
    _cache[key]["data"] = sample
    _cache[key]["ts"] = time.time()
    logger.info("Cache filled for %s with debug sample", key)
    return jsonify({"status": "ok", "endpoint": key, "_source": "debug_cached"})


@app.route('/debug/fill_cache', methods=['GET'])
def debug_fill_cache_page():
    """Serve a small HTML page with buttons to POST to the fill-cache endpoints.
    This keeps the mutating API POST-only while providing a browser-friendly
    UI for local demos and QA.
    """
    # Create a simple listing of top-level endpoints and a small form to
    # POST arbitrary group/source pairs. Use Fetch API for POSTs so the
    # page works from the dev server without extra tooling.
    endpoints = list(ENDPOINTS.keys())
    # Build HTML with inline JS
    html = [
        "<!doctype html>",
        "<html><head><meta charset='utf-8'><title>Debug: Fill Cache</title>",
        "<style>body{font-family:Arial,Helvetica,sans-serif;margin:20px}button{margin:6px;padding:8px}</style>",
        "</head><body>",
        "<h2>Debug: Fill Cache</h2>",
        "<p>Click a button to seed the in-memory cache with a synthetic sample.</p>",
        "<div id=buttons>",
    ]
    for e in endpoints:
        html.append(f"<button onclick=postEndpoint('{e}')>Fill '{e}'</button>")

    html += [
        "</div>",
        "<hr>",
        "<h3>Fill specific group/source</h3>",
        "<label>Group: <input id=group value='crop_vision'></label>",
        "<label> Source: <input id=source value='G5'></label>",
        "<button onclick=postGroupSource()>Fill Group/Source</button>",
        "<pre id=out style='background:#f6f8fa;border:1px solid #ddd;padding:8px;margin-top:12px;max-width:800px'></pre>",
        "<script>",
        "async function postEndpoint(e){",
        "  const res = await fetch('/api/fill_cache/'+e, {method:'POST'});",
        "  const txt = await res.text();",
        "  document.getElementById('out').textContent = '['+res.status+'] '+txt;",
        "}",
        "async function postGroupSource(){",
        "  const g=document.getElementById('group').value;const s=document.getElementById('source').value;",
        "  const res = await fetch('/api/fill_cache/'+encodeURIComponent(g)+'/'+encodeURIComponent(s), {method:'POST'});",
        "  const txt = await res.text();document.getElementById('out').textContent='['+res.status+'] '+txt;",
        "}",
        "</script>",
        "</body></html>",
    ]
    return '\n'.join(html), 200, {'Content-Type': 'text/html; charset=utf-8'}


if ALLOW_DEBUG_SAMPLES:
    @app.route('/api/debug/greenhouse', methods=['GET'])
    def debug_greenhouse():
        sample = {
            'Timestamp': time.strftime('%Y-%m-%dT%H:%M:%S'),
            'Temperature': 24.3,
            'Humidity': 52.1,
            'Pressure': 101.3,
            'CO': 0,
            'lpg': 0,
            'Smoke': 0,
            'CO2': 415,
            'Light_Intensity': 1250,
            'methane_level': 0,
            'propane_level': 0,
            'butane_level': 0,
            'hydrogen_level': 0,
            'alcohol_level': 0,
            'ammonia_level': 0,
            'benzene_level': 0,
            'toluene_level': 0,
        }
        sample['_source'] = 'debug'
        sample['_debug_ts'] = int(time.time())
        return jsonify(sample)


    # ------------------------------------------------------------------------------
    # Development-only debug endpoints
    # These return a synthetic record shaped like the models expected by the
    # Flutter app so you can verify the UI and networking without upstream APEX.
    # ------------------------------------------------------------------------------
    @app.route('/api/debug/irrigation', methods=['GET'])
    def debug_irrigation():
        sample = {
            'Timestamp': time.strftime('%Y-%m-%dT%H:%M:%S'),
            'flow_rate_Lmin': 47.2,
            'total_volume': 123.4,
            'water_used_cycle': 5.6,
            'pump_state': 'ON',
        }
        sample['_source'] = 'debug'
        sample['_debug_ts'] = int(time.time())
        return jsonify(sample)


@app.route('/api/force_refresh/<string:endpoint_name>', methods=['POST'])
def force_refresh(endpoint_name: str):
    """Synchronously fetch from upstream and update the cache. Returns the fetched payload or error.
    Useful for development to test live upstream responses on demand.
    """
    # Keep force refresh simple and synchronous: fetch from the configured
    # upstream and update the in-memory cache before returning. This avoids
    # the complexity and potential silent failures introduced by background
    # threads managing fetch lifecycle.
    if endpoint_name not in ENDPOINTS:
        return jsonify({"error": f"Invalid endpoint name: '{endpoint_name}'"}), 404

    logger.info("Force refresh (sync) requested for %s", endpoint_name)

    payload = fetch_from_apex(ENDPOINTS[endpoint_name], name=endpoint_name)
    if payload is None:
        logger.warning("Force refresh: upstream fetch failed for %s", endpoint_name)
        last_err = _last_fetch_errors.get(endpoint_name)
        return jsonify({"error": "Upstream fetch failed", "last_error": last_err}), 503

    try:
        payload = normalize_payload(endpoint_name, None, payload)
    except Exception:
        pass

    _cache.setdefault(endpoint_name, {"ts": 0, "data": None})
    _cache[endpoint_name]["data"] = payload
    _cache[endpoint_name]["ts"] = time.time()
    resp = dict(payload) if isinstance(payload, dict) else {"data": payload}
    resp["_source"] = "live"
    resp["_fetched_ts"] = int(time.time())
    logger.info("Force refresh succeeded for %s (sync)", endpoint_name)
    return jsonify(resp)


@app.route('/api/force_refresh/<string:group>/<string:source>', methods=['POST'])
def force_refresh_group_source(group: str, source: str):
    key = _cache_key(group, source)
    upstream = get_upstream_url(group, source)
    logger.info("Force refresh requested for %s", key)
    # Synchronous fetch for a specific group/source. Keep behavior simple and
    # blocking so callers get deterministic results and background failures
    # don't silently hide problems.
    payload = fetch_from_apex(upstream, name=key)
    if payload is None:
        logger.warning("Force refresh: upstream fetch failed for %s", key)
        last_err = _last_fetch_errors.get(key)
        return jsonify({"error": "Upstream fetch failed", "last_error": last_err}), 503

    _cache.setdefault(key, {"ts": 0, "data": None})
    try:
        payload = normalize_payload(group, source, payload)
    except Exception:
        pass
    _cache[key]["data"] = payload
    _cache[key]["ts"] = time.time()
    resp = dict(payload) if isinstance(payload, dict) else {"data": payload}
    resp["_source"] = "live"
    resp["_fetched_ts"] = int(time.time())
    logger.info("Force refresh succeeded for %s (sync)", key)
    return jsonify(resp)


@app.route('/api/ingest/<string:group>/<string:source>', methods=['POST'])
def ingest_from_device(group: str, source: str):
    """Endpoint for edge devices (Raspberry Pi) to POST telemetry directly to
    the bridge during integration testing or production. Payload must be JSON.
    This stores the payload in the in-memory cache under the subgroup key and
    marks the source as 'ingest'. In a real deployment you would enqueue the
    message for persistence or forward to Oracle APEX as needed.
    """
    try:
        payload = request.get_json(force=True)
    except Exception as ex:
        logger.warning("Ingest: invalid JSON from device for %s/%s: %s", group, source, ex)
        return jsonify({"error": "invalid json"}), 400

    key = _cache_key(group, source)
    _cache.setdefault(key, {"ts": 0, "data": None})
    _cache[key]["data"] = payload
    _cache[key]["ts"] = time.time()
    # annotate payload so clients know it came from an ingesting device
    if isinstance(payload, dict):
        payload["_source"] = "ingest"
    logger.info("Ingested payload for %s (size=%d)", key, len(str(payload)))
    # Ingest stored in local in-memory cache. Forwarding has been disabled
    # in this simplified build to avoid echoing data back to upstream APEX.
    return jsonify({"status": "ok", "endpoint": key, "_source": "ingest"})
    
# -----------------------------------------------------------------------------
# Automatic scheduled refresher
# -----------------------------------------------------------------------------
# Instead of relying on ad-hoc background threads or an external poller,
# run_scheduled_refreshes will periodically and synchronously refresh each
# configured group/source pair and write the result to the in-memory cache.
# This keeps behavior deterministic and surfaces upstream fetch failures
# immediately in the server logs.
BACKGROUND_REFRESH_INTERVAL = int(os.getenv('BACKGROUND_REFRESH_INTERVAL', '300'))  # seconds
# Short initial delay before the first refresh cycle starts. This lets the
# HTTP server finish startup and handle incoming client requests so the UI
# can request cached/placeholder data instead of being blocked by slow
# upstream fetches that may take up to APEX_FETCH_TIMEOUT each.
BACKGROUND_INITIAL_DELAY = int(os.getenv('BACKGROUND_INITIAL_DELAY', '5'))  # seconds


def run_scheduled_refreshes():
    """Run an infinite loop that refreshes all configured group/source pairs.
    This function intentionally performs synchronous fetches so any errors
    are logged and the cache is consistently updated. It is meant to be
    started in a daemon thread from the main process.
    """
    groups = set(list(ENDPOINTS.keys()) + list(DEFAULT_UPSTREAM_OVERRIDES.keys()))
    logger.info("Background refresher starting: groups=%s interval=%s initial_delay=%s", groups, BACKGROUND_REFRESH_INTERVAL, BACKGROUND_INITIAL_DELAY)
    # Allow a short initial delay so the Flask HTTP server can start and
    # respond to client requests (avoids blocking the UI during long upstream
    # fetches). Sleep once before entering the refresh loop.
    try:
        if BACKGROUND_INITIAL_DELAY > 0:
            logger.info("Delaying first refresh by %s seconds to allow HTTP server startup...", BACKGROUND_INITIAL_DELAY)
            # Use wait so we can wake early if shutdown requested.
            _shutdown_event.wait(BACKGROUND_INITIAL_DELAY)
            if _shutdown_event.is_set():
                logger.info("Shutdown requested before first refresh; exiting refresher loop.")
                return
    except Exception:
        pass

    # Single-threaded synchronous loop: call the background refresh function
    # directly for each group/source. This is simpler to debug and ensures the
    # refresher won't create additional threads or rely on an executor.
    while not _shutdown_event.is_set():
        try:
            logger.info("Starting background refresh cycle (synchronous)...")
            for g in groups:
                g_lower = g.lower()
                sources = list_sources_for(g_lower)
                for s in sources:
                    key = _cache_key(g_lower, s)
                    # Skip sources that have repeatedly failed recently.
                    try:
                        fail_count = _consecutive_failures.get(key, 0)
                        last_fail = _last_failure_ts.get(key, 0)
                        if fail_count >= FAIL_SKIP_THRESHOLD and (time.time() - (last_fail or 0) < FAIL_SKIP_WINDOW):
                            logger.info("Skipping %s due to recent failures (count=%s, age=%.1fs)", key, fail_count, time.time() - (last_fail or 0))
                            continue
                    except Exception:
                        pass

                    logger.info("Refreshing %s:%s (sync)", g_lower, s)
                    try:
                        # Direct synchronous call. _background_refresh handles
                        # updating the cache and any necessary failure counters.
                        _background_refresh(g_lower, s)
                    except Exception as ex:
                        logger.exception("Synchronous scheduled refresh error for %s: %s", key, ex)

            logger.info("Background refresh cycle complete; sleeping %s seconds", BACKGROUND_REFRESH_INTERVAL)
        except Exception as ex:
            logger.exception("Unhandled error in scheduled refresher: %s", ex)
        # Wait with timeout so a shutdown event can interrupt the sleep.
        if _shutdown_event.wait(BACKGROUND_REFRESH_INTERVAL):
            logger.info("Shutdown requested; exiting refresher loop.")
            break

# ==============================================================================
# === 4. MAIN EXECUTION BLOCK
# ==============================================================================
if __name__ == "__main__":
    # Determine a short server code version to display on startup. Prefer
    # a git short commit if available, otherwise fall back to a timestamp.
    version = None
    try:
        proc = subprocess.run(['git', 'rev-parse', '--short', 'HEAD'], capture_output=True, text=True, timeout=2)
        if proc.returncode == 0:
            version = proc.stdout.strip()
    except Exception:
        version = None

    if not version:
        version = time.strftime('%Y%m%d%H%M%S')

    banner = f"🚀 Starting Smart Farm API Server (Final Version) — version={version}"
    print(banner)
    logger.info(banner)
    # Start the scheduled background refresher in a daemon thread so the
    # main thread remains dedicated to serving HTTP requests. The refresher
    # performs synchronous fetches for each configured group/source on a
    # periodic interval to keep the cache warm for clients.
    # Start the scheduled background refresher in a daemon thread so the
    # main thread remains dedicated to serving HTTP requests. The refresher
    # performs synchronous fetches for each configured group/source on a
    # periodic interval to keep the cache warm for clients.
    _refresher_thread = threading.Thread(target=run_scheduled_refreshes, daemon=True)
    _refresher_thread.start()

    try:
        app.run(host="0.0.0.0", port=5000, debug=False, threaded=True)
    except KeyboardInterrupt:
        # Signal background threads to stop and wait briefly for them to exit.
        logger.info('Keyboard interrupt received: shutting down background refresher...')
        try:
            _shutdown_event.set()
            if _refresher_thread is not None:
                _refresher_thread.join(timeout=5)
        except Exception:
            pass
        raise
    # For local testing: enable threaded mode and disable the interactive
    # Werkzeug debugger to avoid the console lock that stops automation.