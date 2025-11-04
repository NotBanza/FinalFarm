# === poll_status.py ===
import requests
import time
import sys

if len(sys.argv) < 2:
    print("Usage: python poll_status.py <endpoint_key>")
    print("Example: python poll_status.py soil:DEFAULT")
    sys.exit(1)

endpoint = sys.argv[1]
url = f"http://127.0.0.1:5000/api/refresh_status/{endpoint}"
start_time = time.time()
initial_status = None

try:
    initial_status = requests.get(url).json()
except Exception as e:
    print(f"Error fetching initial status from {url}: {e}")
    sys.exit(1)

print(f"Polling status for '{endpoint}'...")
print(f"Initial cache timestamp: {initial_status.get('last_cached_ts')}")

while True:
    try:
        time.sleep(5)
        status = requests.get(url).json()
        current_ts = status.get('last_cached_ts')
        elapsed = time.time() - start_time
        
        print(f"[{int(elapsed)}s] Current cache timestamp: {current_ts}")

        if current_ts is not None and current_ts != initial_status.get('last_cached_ts'):
            print("\n✅ Success! Cache was updated.")
            print(f"Total time for background refresh: {int(elapsed)} seconds.")
            break
            
        if elapsed > 180: # 3-minute timeout
            print("\n❌ Timed out waiting for cache update.")
            break
    except KeyboardInterrupt:
        print("\nPolling stopped.")
        break
    except Exception as e:
        print(f"Error during polling: {e}")
        time.sleep(5)
