"""
NYC Taxi Pipeline — Zone Centroid Loader
Downloads the official TLC taxi zone shapefile (EPSG:2263 NY State Plane),
converts polygon centroids to WGS84, and inserts into zone_map_stats so
Grafana's Geomap panel can display the live heatmap.

Run once before starting the streaming job:
    python scripts/load_zone_centroids.py
"""

import gevent.monkey; gevent.monkey.patch_all()  # must be before all other imports for Python 3.12

import os
import io
import zipfile
import requests
from datetime import datetime, timezone
from dotenv import load_dotenv

load_dotenv()

CASSANDRA_HOST     = os.getenv("CASSANDRA_HOST",     "localhost")
CASSANDRA_PORT     = int(os.getenv("CASSANDRA_PORT", "9042"))
CASSANDRA_KEYSPACE = os.getenv("CASSANDRA_KEYSPACE", "taxi_streaming")

TLC_ZONES_URL = "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zones.zip"


def fetch_zones() -> list:
    import shapefile
    from pyproj import Transformer

    print("  Downloading TLC taxi zone shapefile ...", end="", flush=True)
    resp = requests.get(TLC_ZONES_URL, timeout=60)
    resp.raise_for_status()

    z   = zipfile.ZipFile(io.BytesIO(resp.content))
    shp = io.BytesIO(z.read(next(n for n in z.namelist() if n.endswith(".shp"))))
    dbf = io.BytesIO(z.read(next(n for n in z.namelist() if n.endswith(".dbf"))))
    shx = io.BytesIO(z.read(next(n for n in z.namelist() if n.endswith(".shx"))))
    print(f" OK ({len(z.namelist())} files in zip)")

    sf = shapefile.Reader(shp=shp, dbf=dbf, shx=shx)

    # NY State Plane Long Island, US feet (EPSG:2263) → WGS84 (EPSG:4326)
    to_wgs84 = Transformer.from_crs("EPSG:2263", "EPSG:4326", always_xy=True)

    field_names = [f[0] for f in sf.fields[1:]]

    zones = []
    for rec in sf.shapeRecords():
        attrs     = dict(zip(field_names, rec.record))
        zone_id   = int(attrs.get("LocationID") or attrs.get("location_id") or 0)
        zone_name = str(attrs.get("zone")       or attrs.get("Zone")        or f"Zone {zone_id}")
        borough   = str(attrs.get("borough")    or attrs.get("Borough")     or "Unknown")

        if zone_id == 0:
            continue

        pts = rec.shape.points
        if not pts:
            continue

        # Average of polygon vertices — accurate enough for map markers
        avg_x = sum(p[0] for p in pts) / len(pts)
        avg_y = sum(p[1] for p in pts) / len(pts)
        lon, lat = to_wgs84.transform(avg_x, avg_y)

        zones.append({
            "zone_id":   zone_id,
            "lat":       lat,
            "lon":       lon,
            "zone_name": zone_name,
            "borough":   borough,
        })

    zones.sort(key=lambda z: z["zone_id"])
    print(f"  Computed centroids for {len(zones)} zones")
    return zones


def insert_to_cassandra(zones: list):
    from cassandra.cluster import Cluster

    print(f"  Connecting to Cassandra {CASSANDRA_HOST}:{CASSANDRA_PORT} ...", end="", flush=True)
    cluster = Cluster([CASSANDRA_HOST], port=CASSANDRA_PORT)
    session = cluster.connect(CASSANDRA_KEYSPACE)
    print(" OK")

    stmt = session.prepare("""
        INSERT INTO zone_map_stats
            (snapshot, zone_id, lat, lon, zone_name, borough,
             trip_count, predicted_demand, weather_label, updated_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """)

    now = datetime.now(timezone.utc)
    for z in zones:
        session.execute(stmt, (
            "current",
            z["zone_id"],
            z["lat"],
            z["lon"],
            z["zone_name"],
            z["borough"],
            0,
            0,
            "clear_mild",
            now,
        ))

    cluster.shutdown()
    print(f"  Inserted {len(zones)} rows into zone_map_stats (snapshot='current')")


def main():
    print("=" * 60)
    print("  NYC Taxi Zone Centroids — Cassandra Loader")
    print(f"  Source   : TLC official shapefile (EPSG:2263 → WGS84)")
    print(f"  Keyspace : {CASSANDRA_KEYSPACE}")
    print("=" * 60 + "\n")

    try:
        zones = fetch_zones()
    except Exception as exc:
        print(f"\nERROR fetching shapefile: {exc}")
        sys.exit(1)

    insert_to_cassandra(zones)

    # Also save a local JSON file so streaming_job.py can load centroids
    # without needing a cassandra-driver connection at startup
    json_path = os.path.join(os.path.dirname(__file__), "..", "zone_centroids.json")
    import json
    with open(json_path, "w") as f:
        json.dump({str(z["zone_id"]): {k: v for k, v in z.items() if k != "zone_id"} for z in zones}, f)
    print(f"  Saved {len(zones)} centroids → zone_centroids.json")

    print("\n  Done. Zone centroids ready in zone_map_stats + zone_centroids.json.")
    print("  The streaming job will update trip counts every 30 s.")
    print("=" * 60)


if __name__ == "__main__":
    main()
