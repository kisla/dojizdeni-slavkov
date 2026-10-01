#!/usr/bin/env python3
"""
Tracks real-time delays for bus line 106 and train line S6 (direction: towards Brno)
using KORDIS JMK's official open GTFS-RT feed (CC BY 4.0), and appends results to a CSV log.

Data sources (official, CC BY 4.0 licensed open data from KORDIS JMK):
  - Static schedule: https://kordis-jmk.cz/gtfs/gtfs.zip
  - Realtime vehicle positions: https://kordis-jmk.cz/gtfs/gtfsReal.dat

Note: this feed does NOT include a delay field directly - only vehicle positions.
Delay is estimated by comparing each vehicle's current timestamp against the
scheduled time for its current/next stop in the static timetable.
Car driving time is additionally fetched from the Mapy.com Routing REST API
(https://api.mapy.com/v1/routing/route, routeType=car_fast_traffic), which returns
a live-traffic-aware duration. The API key is read from the MAPY_API_KEY
environment variable. (Claude's credential-injection proxy was tried first, but
its "Body parameter" injection only supports JSON/form request bodies - this
endpoint is a bodyless GET with the key in the query string, which the proxy
can't handle, so a plain env var is used instead. The key is a free-tier,
non-billable Mapy.com key scoped to this project only.)
"""
import csv
import datetime
from zoneinfo import ZoneInfo
import json
import os
import zipfile
import io
import urllib.request
import urllib.parse
import urllib.error

from google.transit import gtfs_realtime_pb2

GTFS_STATIC_URL = "https://kordis-jmk.cz/gtfs/gtfs.zip"
GTFS_RT_URL = "https://kordis-jmk.cz/gtfs/gtfsReal.dat"
ROUTES_OF_INTEREST = {"106": "bus", "S6": "train"}
CACHE_DIR = os.path.join(os.path.dirname(__file__), "gtfs_cache")
STATIC_ZIP_PATH = os.path.join(CACHE_DIR, "gtfs.zip")
LOG_PATH = os.path.join(os.path.dirname(__file__), "dojizdeni_log_rijen2026.csv")
STATIC_MAX_AGE_HOURS = 20

# Polni 332, Slavkov u Brna -> Vlnena/Digiteq Automotive, Prizova 7, Brno-stred
ROUTE_START_LONLAT = (16.8779297, 49.1509648)
ROUTE_END_LONLAT = (16.6168654, 49.1892194)
MAPY_ROUTING_URL = "https://api.mapy.com/v1/routing/route"


def get_car_drive_minutes():
    """Live-traffic driving time in minutes via Mapy.com, or (None, error note) on failure."""
    api_key = os.environ.get("MAPY_API_KEY")
    if not api_key:
        return None, "MAPY_API_KEY environment variable not set"
    params = {
        "start": f"{ROUTE_START_LONLAT[0]},{ROUTE_START_LONLAT[1]}",
        "end": f"{ROUTE_END_LONLAT[0]},{ROUTE_END_LONLAT[1]}",
        "routeType": "car_fast_traffic",
        "apikey": api_key,
    }
    url = f"{MAPY_ROUTING_URL}?{urllib.parse.urlencode(params)}"
    try:
        data = fetch(url)
        payload = json.loads(data)
        duration_s = payload["duration"]
        return round(duration_s / 60, 1), ""
    except urllib.error.HTTPError as exc:
        body = exc.read().decode("utf-8", errors="replace")[:500]
        return None, f"mapy.com routing call failed: HTTP {exc.code} {exc.reason} | body={body!r}"
    except Exception as exc:
        return None, f"mapy.com routing call failed: {exc}"


def fetch(url, dest_path=None):
    req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
    with urllib.request.urlopen(req, timeout=60) as resp:
        data = resp.read()
    if dest_path:
        with open(dest_path, "wb") as f:
            f.write(data)
    return data


def ensure_static_gtfs():
    os.makedirs(CACHE_DIR, exist_ok=True)
    needs_fetch = True
    if os.path.exists(STATIC_ZIP_PATH):
        age_h = (datetime.datetime.now().timestamp() - os.path.getmtime(STATIC_ZIP_PATH)) / 3600
        if age_h < STATIC_MAX_AGE_HOURS:
            needs_fetch = False
    if needs_fetch:
        fetch(GTFS_STATIC_URL, STATIC_ZIP_PATH)
    return STATIC_ZIP_PATH


def load_route_ids(zf):
    """Returns dict route_id -> route_short_name (e.g. "106", "S6")."""
    route_ids = {}
    with zf.open("routes.txt") as f:
        reader = csv.DictReader(io.TextIOWrapper(f, encoding="utf-8-sig"))
        for row in reader:
            name = row["route_short_name"]
            if name in ROUTES_OF_INTEREST:
                route_ids[row["route_id"]] = name
    return route_ids


def load_relevant_trips(zf, route_ids):
    """Returns dict trip_id -> (route_short_name, headsign), restricted to direction_id==1 (towards Brno)."""
    trips = {}
    with zf.open("trips.txt") as f:
        reader = csv.DictReader(io.TextIOWrapper(f, encoding="utf-8-sig"))
        for row in reader:
            if row["route_id"] in route_ids and row.get("direction_id") == "1":
                trips[row["trip_id"]] = (route_ids[row["route_id"]], row.get("trip_headsign", ""))
    return trips


def load_stop_times_for_trips(zf, trip_ids):
    """Returns dict (trip_id, stop_id) -> (arrival_time_str, departure_time_str)."""
    result = {}
    with zf.open("stop_times.txt") as f:
        reader = csv.DictReader(io.TextIOWrapper(f, encoding="utf-8-sig"))
        for row in reader:
            if row["trip_id"] in trip_ids:
                result[(row["trip_id"], row["stop_id"])] = (row["arrival_time"], row["departure_time"])
    return result


PRAGUE_TZ = ZoneInfo("Europe/Prague")


def gtfs_time_to_epoch(time_str, service_date):
    """GTFS times can exceed 24:00:00 for trips past midnight.
    GTFS schedule times are always local (Europe/Prague) wall-clock time, regardless
    of the host machine's system timezone (the cloud runner uses UTC), so the base
    datetime must be explicitly tz-aware to compute the correct UTC epoch."""
    h, m, s = (int(x) for x in time_str.split(":"))
    base = datetime.datetime.combine(service_date, datetime.time(0, 0, 0), tzinfo=PRAGUE_TZ)
    return (base + datetime.timedelta(hours=h, minutes=m, seconds=s)).timestamp()


def main():
    now = datetime.datetime.now(PRAGUE_TZ)
    today = now.date()

    static_path = ensure_static_gtfs()
    with zipfile.ZipFile(static_path) as zf:
        route_ids = load_route_ids(zf)
        trips = load_relevant_trips(zf, route_ids)
        stop_times = load_stop_times_for_trips(zf, set(trips.keys()))

    rt_data = fetch(GTFS_RT_URL)
    feed = gtfs_realtime_pb2.FeedMessage()
    feed.ParseFromString(rt_data)

    rows_by_route = {name: [] for name in ROUTES_OF_INTEREST.keys()}
    seen_vehicle_ids = set()

    for entity in feed.entity:
        if not entity.HasField("vehicle"):
            continue
        v = entity.vehicle
        trip_id = v.trip.trip_id
        if trip_id not in trips:
            continue
        route_name, headsign = trips[trip_id]
        stop_id = v.stop_id
        dedup_key = (route_name, stop_id)
        if dedup_key in seen_vehicle_ids:
            continue  # same physical train already logged this run (coupled multi-unit trains / calendar
            # variants can report multiple trip_ids for what is the same real service at the same stop)
        key = (trip_id, stop_id)
        if key not in stop_times:
            continue
        arrival_str, departure_str = stop_times[key]
        sched_str = departure_str if v.current_status == 1 else arrival_str  # 1=STOPPED_AT -> use departure
        try:
            sched_epoch = gtfs_time_to_epoch(sched_str, today)
        except Exception:
            continue
        actual_epoch = v.timestamp
        delay_min = round((actual_epoch - sched_epoch) / 60, 1)
        seen_vehicle_ids.add(dedup_key)
        rows_by_route[route_name].append({
            "trip_id": trip_id,
            "headsign": headsign,
            "stop_id": stop_id,
            "delay_min": delay_min,
            "vehicle_label": v.vehicle.label,
        })

    car_min, car_note = get_car_drive_minutes()

    file_exists = os.path.exists(LOG_PATH)
    with open(LOG_PATH, "a", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        if not file_exists:
            writer.writerow(["timestamp", "route", "trip_id", "headsign", "stop_id", "delay_min",
                              "vehicle_label", "car_drive_min", "note"])
        ts = now.strftime("%Y-%m-%d %H:%M:%S")
        for route_name in ROUTES_OF_INTEREST.keys():
            entries = rows_by_route[route_name]
            if not entries:
                writer.writerow([ts, route_name, "", "NO_ACTIVE_VEHICLE", "", "", "", "", ""])
                continue
            # pick the vehicle closest to Brno-bound progress; just log all found (usually 1)
            for e in entries:
                writer.writerow([ts, route_name, e["trip_id"], e["headsign"], e["stop_id"], e["delay_min"],
                                  e["vehicle_label"], "", ""])
        writer.writerow([ts, "CAR", "", "", "", "", "", car_min if car_min is not None else "", car_note])
        print(f"Logged {ts}: " + ", ".join(f"{r}={len(rows_by_route[r])} vehicle(s)" for r in ROUTES_OF_INTEREST.keys())
              + f", car={car_min} min" + (f" ({car_note})" if car_note else ""))


if __name__ == "__main__":
    main()
