#!/usr/bin/env python3
"""
verbose_probe.py

Usage:
  python tools\verbose_probe.py https://oracleapex.com/ords/g3_data/irrigation_g6/

This helper attempts a GET with a long timeout and prints either the
response (status, headers, truncated body) or a full exception traceback.
It is useful to capture SSL/TLS errors and connection resets when the
lighter `check_upstreams.py` only reported a timeout.
"""
import sys
import traceback
import requests

def probe(url: str):
    print(f"Probing: {url}\n")
    try:
        # Try with a longer timeout and default SSL verification first
        r = requests.get(url, timeout=30)
        print(f"Status: {r.status_code}")
        print("Response headers:")
        for k, v in r.headers.items():
            print(f"  {k}: {v}")
        print('\nBody (first 2000 chars):')
        print(r.text[:2000])
        return
    except Exception as e:
        print("First attempt failed (with SSL verification). Exception:\n")
        traceback.print_exc()

    print('\nRetrying with SSL verification disabled (verify=False) for diagnosis...')
    try:
        r = requests.get(url, timeout=30, verify=False)
        print(f"Status (insecure): {r.status_code}")
        print("Response headers:")
        for k, v in r.headers.items():
            print(f"  {k}: {v}")
        print('\nBody (first 2000 chars):')
        print(r.text[:2000])
        return
    except Exception:
        print("Second attempt failed (verify=False). Exception:\n")
        traceback.print_exc()


if __name__ == '__main__':
    if len(sys.argv) < 2:
        print("Usage: python tools\\verbose_probe.py <url>")
        sys.exit(1)
    probe(sys.argv[1])
