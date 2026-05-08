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
RESET_FLAG         = "/tmp/zone_reset.flag"


def _query_zone_map_stats(snapshot: str) -> list:
    from cassandra.cluster import Cluster
    cluster = Cluster([CASSANDRA_HOST], port=CASSANDRA_PORT)
    session = cluster.connect(CASSANDRA_KEYSPACE)
    rows = session.execute(
        "SELECT zone_id, lat, lon, zone_name, borough, trip_count, predicted_demand "
        "FROM zone_map_stats WHERE snapshot=%s",
        (snapshot,),
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


def fetch_zones() -> list:
    return _query_zone_map_stats("current")


def fetch_zones_batch(year: int) -> list:
    return _query_zone_map_stats(f"batch-{year}")


def reset_current_snapshot() -> int:
    """Delete all zone_map_stats rows for snapshot='current' and drop the reset flag."""
    from cassandra.cluster import Cluster
    cluster = Cluster([CASSANDRA_HOST], port=CASSANDRA_PORT)
    session = cluster.connect(CASSANDRA_KEYSPACE)
    rows = session.execute(
        "SELECT zone_id FROM zone_map_stats WHERE snapshot='current'"
    )
    zone_ids = [r.zone_id for r in rows]
    for zid in zone_ids:
        session.execute(
            "DELETE FROM zone_map_stats WHERE snapshot='current' AND zone_id=%s",
            (zid,),
        )
    cluster.shutdown()
    # Signal the streaming job to zero its in-memory totals
    open(RESET_FLAG, "w").close()
    return len(zone_ids)


def _parse_qs(path: str) -> dict:
    """Extract query string params from a path like /zones/batch?year=2022."""
    if "?" not in path:
        return {}
    qs = path.split("?", 1)[1]
    params = {}
    for part in qs.split("&"):
        if "=" in part:
            k, v = part.split("=", 1)
            params[k] = v
    return params


class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        path = self.path.split("?")[0]
        params = _parse_qs(self.path)

        if path == "/zones/batch":
            try:
                year = int(params.get("year", 2022))
                data = fetch_zones_batch(year)
                self._send_json(data)
                print(f"  GET /zones/batch?year={year} → {len(data)} zones served")
            except Exception as exc:
                self._send_error(exc)
                print(f"  GET /zones/batch ERROR: {exc}")
        elif path == "/zones":
            try:
                data = fetch_zones()
                self._send_json(data)
                print(f"  GET /zones → {len(data)} zones served")
            except Exception as exc:
                self._send_error(exc)
                print(f"  GET /zones ERROR: {exc}")
        else:
            self.send_response(404)
            self.end_headers()

    def do_POST(self):
        path = self.path.split("?")[0]
        if path == "/zones/reset":
            self._handle_reset()
        else:
            self.send_response(404)
            self.end_headers()

    def do_DELETE(self):
        path = self.path.split("?")[0]
        if path == "/zones/current":
            self._handle_reset()
        else:
            self.send_response(404)
            self.end_headers()

    def _handle_reset(self):
        try:
            n = reset_current_snapshot()
            self._send_json({"reset": True, "zones_cleared": n})
            print(f"  RESET /zones/current → {n} zones cleared")
        except Exception as exc:
            self._send_error(exc)
            print(f"  RESET ERROR: {exc}")

    def _send_json(self, data):
        body = json.dumps(data).encode()
        self.send_response(200)
        self.send_header("Content-Type",                "application/json")
        self.send_header("Content-Length",              str(len(body)))
        self.send_header("Access-Control-Allow-Origin", "*")
        self.end_headers()
        self.wfile.write(body)

    def _send_error(self, exc):
        body = json.dumps({"error": str(exc)}).encode()
        self.send_response(500)
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *_):
        pass


if __name__ == "__main__":
    print("=" * 50)
    print(f"  NYC Taxi — Zone API")
    print(f"  Cassandra : {CASSANDRA_HOST}:{CASSANDRA_PORT}/{CASSANDRA_KEYSPACE}")
    print(f"  Live      : http://localhost:{PORT}/zones")
    print(f"  Batch     : http://localhost:{PORT}/zones/batch?year=2022")
    print(f"  Reset     : POST http://localhost:{PORT}/zones/reset")
    print("=" * 50)
    HTTPServer(("0.0.0.0", PORT), Handler).serve_forever()
