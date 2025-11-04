import requests
import traceback

URL = "https://oracleapex.com/ords/g3_data/greenhouse_group1/"

print('Testing URL:', URL)
try:
    r = requests.get(URL, timeout=30, verify=False)
    print('STATUS:', r.status_code)
    print('HEADERS:', r.headers)
    print('BODY_SNIPPET:', r.text[:1000])
except Exception:
    traceback.print_exc()
