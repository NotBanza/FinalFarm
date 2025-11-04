import requests
import traceback
url = 'https://oracleapex.com/ords/g3_data/irrigation_g6/'
print('Testing URL:', url)
try:
    r = requests.get(url, timeout=30, verify=False)
    print('STATUS', r.status_code)
    print('HEADERS', dict(r.headers))
    print('BODY (first 1000 chars)')
    print(r.text[:1000])
except Exception:
    traceback.print_exc()
