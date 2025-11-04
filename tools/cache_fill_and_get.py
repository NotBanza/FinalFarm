import requests
import json
import sys

URL_POST = 'http://127.0.0.1:5000/api/fill_cache/greenhouse'
URL_GET = 'http://127.0.0.1:5000/api/greenhouse/latest'

try:
    r = requests.post(URL_POST, timeout=10)
    print('POST', URL_POST)
    print('Status:', r.status_code)
    try:
        print('Body:', json.dumps(r.json(), indent=2))
    except Exception:
        print('Body (raw):', r.text)

    r2 = requests.get(URL_GET, timeout=10)
    print('\nGET', URL_GET)
    print('Status:', r2.status_code)
    print('Headers:', dict(r2.headers))
    try:
        j = r2.json()
        print('Body (json):', json.dumps(j, indent=2))
        print('\n_parsed _source:', j.get('_source'))
    except Exception:
        print('Body (raw):', r2.text)

except Exception as e:
    print('Error during requests:', e)
    sys.exit(1)
