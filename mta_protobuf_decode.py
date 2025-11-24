import os
from google.transit import gtfs_realtime_pb2
from flask import Flask, jsonify, request
from flask_cors import CORS
import requests
import time
import pandas as pd
import  re

app = Flask(__name__)
CORS(app)

# This is spefifically for IRT lines
FEED_URL = "https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/nyct%2Fgtfs"

# Load stop names
df = pd.read_csv('stations.csv')
STOP_NAMES = df.set_index('GTFS Stop ID')['Stop Name'].to_dict()

STOP = '127N'

# Regex pattern for valid MTA stop IDs
STOP_ID_PATTERN = re.compile(r'^[A-Z]?\d{2,4}[NS]?$')

def is_valid_stop_id(stop_id):
    """
    Validates MTA stop ID format
    Examples of valid formats:
    - 137N, 247S (most common)
    - 137, 247 (no direction)
    - A42N, R11S (letter prefix)
    - 1234N (4 digits)
    """
    return bool(STOP_ID_PATTERN.match(stop_id))

# API will be given a designated stop - methods to returns that stop's trains, as well as set the designated stop ID itself.
@app.route('/train/getStop')
def get_designated_stop():
    global STOP
    return get_train_times(STOP)

@app.route('/train/setStop', methods=['POST'])
def post_designated_stop():
    global STOP    
        
    data = request.get_json()
    stop_id = data.get('stop_id')
    
    if not is_valid_stop_id(stop_id):
        return {"error": "invalid stop_id provided"}
    
    STOP = stop_id
    return {"success": f"default stop_id updated to {stop_id}"}
    

# Get the trains corresponding to the stop ID provided
@app.route('/train/<stop_id>')
def get_train_times(stop_id):
    try:
        response = requests.get(FEED_URL, timeout=10)
        response.raise_for_status()

        feed = gtfs_realtime_pb2.FeedMessage()
        feed.ParseFromString(response.content)

        trains = []
        for entity in feed.entity:
            if entity.HasField('trip_update'):
                trip = entity.trip_update

                last_stop = None
                arrival_time = None

                for stop_time_update in trip.stop_time_update:
                    if stop_time_update.stop_id == stop_id:
                        if stop_time_update.HasField('arrival'):
                            arrival_time = stop_time_update.arrival.time

                    if arrival_time is not None:
                        last_stop = stop_time_update.stop_id

                if arrival_time and last_stop and ((arrival_time - int(time.time())) // 60) > -2:
                    trains.append({
                        'route': trip.trip.route_id,
                        'arrival': arrival_time,
                        'destination': get_stop_name(last_stop[:-1]),
                        'minutes_away': (arrival_time - int(time.time())) // 60
                    })

        trains.sort(key=lambda x: x['arrival'])
        return jsonify(trains)

    except requests.RequestException as e:
        return jsonify({"error": f"Failed to fetch MTA data: {str(e)}"}), 502
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    
@app.route('/health')
def health():
    return jsonify({"status": "ok"})

def get_stop_name(stop_id):
    return STOP_NAMES.get(stop_id, stop_id)

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8080)