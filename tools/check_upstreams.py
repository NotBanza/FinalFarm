#!/usr/bin/env python3
r"""
check_upstreams.py

Run this from the same virtualenv as the Flask app. It imports the
`testapp` module to reuse the same requests Session, SSL and timeout
settings, then attempts a GET against every configured group/source and
prints a JSON report with status or error.

Usage (Windows cmd.exe):
    (venv) C:\...\Testing the Flask app> python tools\check_upstreams.py

This is a development helper to reproduce the same failures you're seeing
from the bridge and capture the upstream response text or exception.
"""
import sys
from pathlib import Path

# Ensure the repository root (parent of this tools/ dir) is on sys.path so
# `import testapp` works when running `python tools\check_upstreams.py`.
ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
        sys.path.insert(0, str(ROOT))
import json
import time
import sys

try:
    import testapp
except Exception as ex:
    print("Error: could not import testapp module. Make sure you're running this script from the repository root and that testapp.py exists.", file=sys.stderr)
    raise

from testapp import list_sources_for, get_upstream_url, ENDPOINTS, DEFAULT_UPSTREAM_OVERRIDES, _session, VERIFY_SSL, APEX_FETCH_TIMEOUT


def probe():
    groups = set(list(ENDPOINTS.keys()) + list(DEFAULT_UPSTREAM_OVERRIDES.keys()))
    report = {}
    for g in sorted(groups):
        report[g] = {}
        sources = list_sources_for(g)
        for s in sources:
            url = get_upstream_url(g, s)
            entry = {"url": url}
            try:
                r = _session.get(url, timeout=APEX_FETCH_TIMEOUT, verify=VERIFY_SSL)
                entry["status_code"] = r.status_code
                # try to include JSON body when possible, but truncate long texts
                try:
                    entry["body"] = r.json()
                except Exception:
                    entry["body"] = r.text[:2000]
            except Exception as ex:
                entry["error"] = str(ex)
            report[g][s] = entry
    return report


def main():
    print("Probing configured upstreams using testapp's session settings...")
    t0 = time.time()
    r = probe()
    print(json.dumps(r, indent=2, default=str))
    print(f"Done in {time.time()-t0:.2f}s")


if __name__ == '__main__':
    main()
