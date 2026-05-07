"""
NYC Taxi — Zone Map Stats API
Serves live zone data (lat/lon + trip counts) from Cassandra as JSON.
Grafana's Infinity datasource polls this endpoint every 30 s.

Run:  python scripts/zone_api.py
URL:  http://localhost:5001/zones
"""
import gevent.monkey; gevent.monkey.patch_all()

import os
import json
from http.server import HTTPServer, BaseHTTPRequestHandler
from dotenv import load_dotenv

load_dotenv()

CASSANDRA_HOST     = os.getenv("CASSANDRA_HOST",     "localhost")
CASSANDRA_PORT     = int(os.getenv("CASSANDRA_PORT", "9042"))
CASSANDRA_KEYSPACE = os.getenv("CASSANDRA_KEYSPACE", "taxi_streaming")
PORT               = int(os.getenv("ZONE_API_PORT",  "5001"))


def fetch_zones() -> list:
    from cassandra.cluster import Cluster
    cluster = Cluster([CASSANDRA_HOST], port=CASSANDRA_PORT)
    session = cluster.connect(CASSANDRA_KEYSPACE)
    rows = session.execute(
        "SELECT zone_id, lat, lon, zone_name, borough, trip_count, predicted_demand "
        "FROM zone_map_stats WHERE snapshot='current'"
    )
    data = [
        {
            "zone_id":          r.zone_id,
            "lat":              r.lat,
            "lon":              r.lon,
            "zone_name":        r.zone_name,
            "borough":          r.borough,
            "trip_count":       r.trip_count  or 0,
            "predicted_demand": r.predicted_demand or 0,
        }
        for r in rows
    ]
    cluster.shutdown()
    return data


class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path.startswith("/zones"):
            try:
                data = fetch_zones()
                body = json.dumps(data).encode()
                self.send_response(200)
                self.send_header("Content-Type",                 "application/json")
                self.send_header("Content-Length",               str(len(body)))
                self.send_header("Access-Control-Allow-Origin",  "*")
                self.end_headers()
                self.wfile.write(body)
                print(f"  GET /zones → {len(data)} zones served")
            except Exception as exc:
                body = json.dumps({"error": str(exc)}).encode()
                self.send_response(500)
                self.send_header("Content-Type", "application/json")
                self.end_headers()
                self.wfile.write(body)
                print(f"  GET /zones ERROR: {exc}")
        else:
            self.send_response(404)
            self.end_headers()

    def log_message(self, *_):
        pass


if __name__ == "__main__":
    print("=" * 50)
    print(f"  NYC Taxi — Zone API")
    print(f"  Cassandra : {CASSANDRA_HOST}:{CASSANDRA_PORT}/{CASSANDRA_KEYSPACE}")
    print(f"  Endpoint  : http://localhost:{PORT}/zones")
    print("=" * 50)
    HTTPServer(("0.0.0.0", PORT), Handler).serve_forever()
