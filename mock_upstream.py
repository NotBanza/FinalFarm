from flask import Flask, jsonify, request
import time
from typing import List, Dict

app = Flask(__name__)

def now_ts(offset_seconds: int = 0) -> str:
    return time.strftime('%Y-%m-%dT%H:%M:%S', time.gmtime(time.time() - offset_seconds))

def make_rows(count: int, prefix: str, group_id: int | None = None) -> List[Dict]:
    rows = []
    for i in range(count):
        ts = now_ts(i * 60)
        row = {
            'timestamp': ts,
            'created_at': ts,
            'device_id': str((i % 2) + 1),
        }
        if prefix == 'soil':
            row.update({
                'moisture': 30.0 + i * 0.5,
                'ph': 6.0 + (i % 5) * 0.1,
                'group_id': group_id if group_id is not None else 7,
            })
        elif prefix == 'greenhouse':
            row.update({'temperature': 24.0 + i * 0.2, 'humidity': 45.0 + (i % 10)})
        elif prefix == 'irrigation':
            row.update({'flow': 5.0 + (i % 7) * 0.5, 'total_volume': 100 + i * 2})
        elif prefix == 'crop':
            row.update({'label': 'healthy' if i % 6 != 0 else 'disease', 'confidence': 0.9 - (i % 5) * 0.05,
                'image': f'https://picsum.photos/seed/crop{i}/400/300'})
        rows.append(row)
    return rows


@app.route('/ords/g3_data/greenhouse/', methods=['GET'])
@app.route('/greenhouse/', methods=['GET'])
def greenhouse_list():
    # Return a collection-style payload with 'items' for the bridge to interpret as rows
    items = make_rows(8, 'greenhouse')
    return jsonify({'data': {'items': items}})


@app.route('/ords/g3_data/irrigation/', methods=['GET'])
@app.route('/irrigation/', methods=['GET'])
def irrigation_list():
    items = make_rows(12, 'irrigation')
    return jsonify({'data': {'items': items}})


@app.route('/ords/g3_data/soil/', methods=['GET'])
@app.route('/soil/', methods=['GET'])
def soil_list():
    # Return multiple groups inside the same endpoint (simulate upstream grouping)
    all_items = []
    # group 7
    all_items.extend(make_rows(6, 'soil', group_id=7))
    # group 8
    all_items.extend(make_rows(6, 'soil', group_id=8))
    # group 10
    all_items.extend(make_rows(6, 'soil', group_id=10))
    return jsonify({'data': {'items': all_items}})


@app.route('/ords/g3_data/crop_vision/', methods=['GET'])
@app.route('/crop_vision/', methods=['GET'])
def crop_vision_list():
    items = make_rows(5, 'crop')
    return jsonify({'data': {'items': items}})


if __name__ == '__main__':
    print('Starting mock upstream on http://127.0.0.1:5001')
    app.run(host='127.0.0.1', port=5001)
