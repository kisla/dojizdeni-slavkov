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
import re
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
STATUS_PATH = os.path.join(os.path.dirname(__file__), "latest_status.json")
STATIC_MAX_AGE_HOURS = 20
STALE_VEHICLE_MAX_AGE_MINUTES = 20
# KORDIS's AVL system occasionally leaves a vehicle tagged with a trip_id it already
# finished (or hasn't started), while its GPS position/timestamp is genuinely live - this
# produces implausible delays that are a feed misassignment artifact, not a real delay.
# A month of logged readings shows a clean gap between genuine delays (-13.4 to +18.2 min)
# and this artifact (observed as low as -58.7 min) - nothing legitimate falls in between,
# so 20 min is a safe cutoff that catches the artifact without risking real disruptions
# (previously 60, which let milder cases of the same artifact slip through and pollute
# the historical-average stats, e.g. the S6 07:43 average was skewed from -0.2 to -7.2 min
# by a single -41.9 reading).
MAX_PLAUSIBLE_DELAY_MINUTES = 20

# Platform stop_ids at the Slavkov u Brna origin stops (grouped by parent_station in stops.txt):
# bus station (parent U16328N107) and train station (parent U16333N246).
ORIGIN_STOP_IDS = {
    "106": {"U16328Z2", "U16328Z3", "U16328Z8", "U16328Z10", "U16328Z7", "U16328Z1",
            "U16328Z5", "U16328Z57", "U16328Z9", "U16328Z69", "U16328Z59", "U16328Z6", "U16328N107"},
    "S6": {"U16333Z1", "U16333Z2", "U16333Z11", "U16333Z10", "U16333N246"},
}

# Polni 332, Slavkov u Brna -> Vlnena/Digiteq Automotive, Prizova 7, Brno-stred
ROUTE_START_LONLAT = (16.8779297, 49.1509648)
ROUTE_END_LONLAT = (16.6168654, 49.1892194)
MAPY_ROUTING_URL = "https://api.mapy.com/v1/routing/route"

# Terminus stop for each route's Brno-bound trips (last stop_id in the trip), used to
# estimate "arrival at destination" time. Looked up once from stop_times.txt/stops.txt:
# bus 106 -> UAN Zvonarka, train S6 -> Hlavni nadrazi.
DEST_STOP_IDS = {
    "106": "U1696Z6",   # UAN Zvonarka
    "S6": "U1146Z99",   # Hlavni nadrazi
}
# Walking time from each terminus to Vlnena (Prizova 7), estimated from straight-line
# distance with a 1.3x street-detour factor at ~80 m/min walking pace, rounded up for
# street crossings. Not live data - these distances/routes don't meaningfully change.
WALK_MINUTES_TO_OFFICE = {
    "106": 8,   # UAN Zvonarka -> Vlnena (~560 m)
    "S6": 7,    # Hlavni nadrazi -> Vlnena (~460 m)
}

# Realistic morning-commute departures from Slavkov u Brna worth tracking every weekday
# (picked from the actual timetable so office arrival lands roughly 07:25-08:30):
# bus 106 is ~25 min to UAN Zvonarka + 8 min walk, S6 is ~37 min to Hlavni nadrazi + 7 min walk.
MONITORED_DEPARTURES = {
    "106": ["06:52", "07:22", "07:52"],
    "S6": ["06:43", "07:06", "07:43"],
}

# Public, unauthenticated endpoint behind idsjmk.cz's own "Aktualni informace" banner
# (homepage -> MIMORADNE UDALOSTI). Found via the site's own JS bundle - no automation
# restriction like mapa.idsjmk.cz's API has. Returns short operational notices (traffic
# jams, blocked stops, etc.) tagged with the affected line short names.
TRAFFIC_TWEETS_URL = "https://www.idsjmk.cz/api/traffic-state/tweets"
INCIDENT_MAX_AGE_MINUTES = 90


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
    """Returns dict trip_id -> (route_short_name, headsign, service_id), restricted to direction_id==1 (towards Brno)."""
    trips = {}
    with zf.open("trips.txt") as f:
        reader = csv.DictReader(io.TextIOWrapper(f, encoding="utf-8-sig"))
        for row in reader:
            if row["route_id"] in route_ids and row.get("direction_id") == "1":
                trips[row["trip_id"]] = (route_ids[row["route_id"]], row.get("trip_headsign", ""), row["service_id"])
    return trips


def load_valid_service_ids(zf, service_date):
    """Returns set of service_id valid on service_date, per calendar.txt + calendar_dates.txt exceptions."""
    date_str = service_date.strftime("%Y%m%d")
    weekday_cols = ["monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday"]
    weekday_col = weekday_cols[service_date.weekday()]
    valid = set()
    with zf.open("calendar.txt") as f:
        reader = csv.DictReader(io.TextIOWrapper(f, encoding="utf-8-sig"))
        for row in reader:
            if row["start_date"] <= date_str <= row["end_date"] and row[weekday_col] == "1":
                valid.add(row["service_id"])
    with zf.open("calendar_dates.txt") as f:
        reader = csv.DictReader(io.TextIOWrapper(f, encoding="utf-8-sig"))
        for row in reader:
            if row["date"] != date_str:
                continue
            if row["exception_type"] == "1":
                valid.add(row["service_id"])
            elif row["exception_type"] == "2":
                valid.discard(row["service_id"])
    return valid


def load_origin_departures(zf, trips, valid_service_ids):
    """Returns dict route_name -> list of (trip_id, departure_time_str, origin_stop_id), for
    trips valid today, departing from the Slavkov u Brna origin stop for that route."""
    result = {name: [] for name in ROUTES_OF_INTEREST}
    with zf.open("stop_times.txt") as f:
        reader = csv.DictReader(io.TextIOWrapper(f, encoding="utf-8-sig"))
        for row in reader:
            trip_id = row["trip_id"]
            if trip_id not in trips:
                continue
            route_name, _headsign, service_id = trips[trip_id]
            if service_id not in valid_service_ids:
                continue
            if row["stop_id"] in ORIGIN_STOP_IDS[route_name]:
                result[route_name].append((trip_id, row["departure_time"], row["stop_id"]))
    return result


def load_platform_codes(zf, stop_ids):
    """Returns dict stop_id -> platform_code ("" if not set in the feed)."""
    result = {}
    with zf.open("stops.txt") as f:
        reader = csv.DictReader(io.TextIOWrapper(f, encoding="utf-8-sig"))
        for row in reader:
            if row["stop_id"] in stop_ids:
                result[row["stop_id"]] = row.get("platform_code", "")
    return result


def next_departure(origin_departures_for_route, now):
    """Earliest (trip_id, departure_time_str, origin_stop_id) at/after now, or None if none remain today."""
    now_seconds = now.hour * 3600 + now.minute * 60 + now.second
    best = None
    best_seconds = None
    for trip_id, dep_str, stop_id in origin_departures_for_route:
        h, m, s = (int(x) for x in dep_str.split(":"))
        dep_seconds = h * 3600 + m * 60 + s
        if dep_seconds >= now_seconds and (best is None or dep_seconds < best_seconds):
            best = (trip_id, dep_str, stop_id)
            best_seconds = dep_seconds
    return best


def historical_average_delay(route_name, departure_time):
    """Mean delay_min for route_name at this specific scheduled departure_time ("HH:MM")
    across all prior logged rows, or (None, 0) if no data yet. Keyed by departure time
    (not just route) since different runs of the same line have very different typical
    delays - e.g. the 7:06 train isn't representative of the 8:30 one."""
    if not os.path.exists(LOG_PATH) or not departure_time:
        return None, 0
    total, count = 0.0, 0
    with open(LOG_PATH, newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            if row.get("route") != route_name or row.get("departure_time") != departure_time:
                continue
            val = row.get("delay_min", "")
            if val == "":
                continue
            try:
                total += float(val)
                count += 1
            except ValueError:
                continue
    if count == 0:
        return None, 0
    return round(total / count, 1), count


def historical_average_car(departure_slot):
    """Mean car_drive_min for CAR rows logged at this rounded departure_slot ("HH:MM"),
    or (None, 0) if no data yet."""
    if not os.path.exists(LOG_PATH):
        return None, 0
    total, count = 0.0, 0
    with open(LOG_PATH, newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            if row.get("route") != "CAR" or row.get("departure_time") != departure_slot:
                continue
            val = row.get("car_drive_min", "")
            if val == "":
                continue
            try:
                total += float(val)
                count += 1
            except ValueError:
                continue
    if count == 0:
        return None, 0
    return round(total / count, 1), count


def round_to_slot(now, slot_minutes=15):
    """Rounds a time to the nearest slot_minutes boundary, as "HH:MM"."""
    total = now.hour * 60 + now.minute
    rounded = round(total / slot_minutes) * slot_minutes
    rounded %= 24 * 60
    return f"{rounded // 60:02d}:{rounded % 60:02d}"


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


def shift_hhmm(time_str, delay_min):
    """Scheduled GTFS time + a delay in minutes, wrapped to 24h, as "HH:MM"."""
    h, m, s = (int(x) for x in time_str.split(":"))
    total = round(h * 60 + m + s / 60 + delay_min) % (24 * 60)
    return f"{total // 60:02d}:{total % 60:02d}"


def fmt_hhmm(time_str):
    """GTFS times aren't reliably zero-padded ("6:52:00", not "06:52:00"), so a naive
    [:5] slice silently mangles every single-digit hour (00-09) - exactly the morning
    commute window this whole project cares about. Parse properly instead."""
    h, m, _s = time_str.split(":")
    return f"{int(h):02d}:{int(m):02d}"


def gtfs_time_to_epoch(time_str, service_date):
    """GTFS times can exceed 24:00:00 for trips past midnight.
    GTFS schedule times are always local (Europe/Prague) wall-clock time, regardless
    of the host machine's system timezone (the cloud runner uses UTC), so the base
    datetime must be explicitly tz-aware to compute the correct UTC epoch."""
    h, m, s = (int(x) for x in time_str.split(":"))
    base = datetime.datetime.combine(service_date, datetime.time(0, 0, 0), tzinfo=PRAGUE_TZ)
    return (base + datetime.timedelta(hours=h, minutes=m, seconds=s)).timestamp()


def estimate_office_arrival(route_name, trip_id, headsign, stop_times, live_delay_min, historical_avg_delay_min):
    """Estimated arrival time at Vlnena ("HH:MM"), the scheduled (no-delay) destination
    arrival ("HH:MM"), and a note (for short-turn workings that don't reach the usual
    terminus), for the given trip. Delay estimate prefers live data, falls back to the
    historical average, falls back to 0 (on-time) if neither is available yet."""
    dest_stop_id = DEST_STOP_IDS[route_name]
    key = (trip_id, dest_stop_id)
    if key not in stop_times:
        # Not every working of a line runs all the way to the usual terminus (e.g. some S6
        # trains end at Zidenice, nadrazi instead of continuing to Hlavni nadrazi) - there's
        # no honest ETA to compute, so say why instead of silently showing nothing.
        note = f"nejede do cíle, končí v {headsign}" if headsign else "nejede do obvyklého cíle"
        return None, None, note
    dest_arrival_str, _ = stop_times[key]
    h, m, s = (int(x) for x in dest_arrival_str.split(":"))
    sched_minutes = h * 60 + m + s / 60

    delay_min = live_delay_min if live_delay_min is not None else (
        historical_avg_delay_min if historical_avg_delay_min is not None else 0)
    walk_min = WALK_MINUTES_TO_OFFICE[route_name]
    total_minutes = round(sched_minutes + delay_min + walk_min)
    total_minutes %= 24 * 60
    eta_str = f"{total_minutes // 60:02d}:{total_minutes % 60:02d}"
    dest_sched_str = fmt_hhmm(dest_arrival_str)
    return eta_str, dest_sched_str, None


def normalize_stop_id(stop_id):
    """Strips zero-padding GTFS-RT adds to single-digit platform codes (e.g. "Z02" -> "Z2")
    so it matches the static schedule's stop_ids. Leaves multi-digit codes (e.g. "Z10")
    untouched, since there's no digit directly before the padding zero(s) in that case."""
    return re.sub(r"(\D)0+(\d)$", r"\1\2", stop_id)


def strip_html(text):
    text = re.sub(r"<[^>]+>", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def get_incident_notes(now):
    """Returns dict route_name -> {"text", "time"} for the most recent idsjmk.cz traffic
    notice mentioning that route, among notices no older than INCIDENT_MAX_AGE_MINUTES.
    Returns {} entries as None if the feed is unreachable or has nothing relevant."""
    notes = {name: None for name in ROUTES_OF_INTEREST}
    try:
        data = json.loads(fetch(TRAFFIC_TWEETS_URL))
    except Exception:
        return notes
    for route_name in ROUTES_OF_INTEREST:
        best = None
        for item in data:
            if route_name not in item.get("lines", []):
                continue
            try:
                item_time = datetime.datetime.fromisoformat(item["time"])
            except Exception:
                continue
            age_min = (now - item_time).total_seconds() / 60
            if age_min > INCIDENT_MAX_AGE_MINUTES or age_min < -5:
                continue
            if best is None or item_time > best[0]:
                best = (item_time, item)
        if best is not None:
            notes[route_name] = {"text": strip_html(best[1]["body"]), "time": best[0].isoformat()}
    return notes


def lookup_todays_trip_history(route_name, trip_id, today_date_str):
    """Scans today's already-logged rows for this specific trip_id. Returns the latest
    (most recently logged) delay/slavkov_time/brno_time seen today for it, plus - if
    precise_morning_watch.py recorded a "precise-brno" checkpoint for it - that
    checkpoint's own delay/time, which is a real observation made right at the Brno
    destination stop, more trustworthy than a live mid-route snapshot (whose slavkov/brno
    times are only the current delay propagated across the whole trip, not an actual
    measurement at either end). Used so that a trip which has already finished and
    dropped out of the live GTFS-RT feed by the time of a later poll still shows its last
    known figures instead of falling back to the generic historical average."""
    result = {
        "latest_delay": None, "latest_slavkov": None, "latest_brno": None,
        "precise_brno_delay": None, "precise_brno_time": None,
    }
    if not os.path.exists(LOG_PATH):
        return result
    with open(LOG_PATH, newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            if (row.get("route") != route_name or row.get("trip_id") != trip_id
                    or not row.get("timestamp", "").startswith(today_date_str)):
                continue
            delay_str = row.get("delay_min", "")
            if delay_str == "":
                continue
            result["latest_delay"] = float(delay_str)
            result["latest_slavkov"] = row.get("slavkov_time") or result["latest_slavkov"]
            result["latest_brno"] = row.get("brno_time") or result["latest_brno"]
            if row.get("headsign") == "precise-brno":
                result["precise_brno_delay"] = result["latest_delay"]
                result["precise_brno_time"] = row.get("brno_time") or None
    return result


def build_departure_info(route_name, trip_id, dep_time, stop_id, rows_by_route, stop_times, platform_codes, trips):
    """Assembles the full status block (live/historical delay, platform, ETA) for one
    specific scheduled departure (trip_id may be None if that departure doesn't run today)."""
    if trip_id is None:
        avg_delay, sample_size = historical_average_delay(route_name, dep_time)
        return {
            "departure": dep_time,
            "platform": None,
            "live_delay_min": None,
            "historical_avg_delay_min": avg_delay,
            "historical_sample_size": sample_size,
            "dest_arrival_scheduled": None,
            "eta": None,
            "eta_note": None,
            "slavkov_time": None,
            "brno_time": None,
        }
    live_delay = None
    slavkov_time = None
    brno_time = None
    for e in rows_by_route[route_name]:
        if e["trip_id"] == trip_id:
            live_delay = e["delay_min"]
            slavkov_time = e["slavkov_time"]
            brno_time = e["brno_time"]
            break
    today_str = datetime.datetime.now(PRAGUE_TZ).strftime("%Y-%m-%d")
    hist = lookup_todays_trip_history(route_name, trip_id, today_str)
    if hist["precise_brno_delay"] is not None:
        # A precise_morning_watch.py checkpoint beats any live/mid-route snapshot - it's
        # measured right at the destination, not propagated from wherever the vehicle was.
        live_delay = hist["precise_brno_delay"]
        slavkov_time = slavkov_time or hist["latest_slavkov"]
        brno_time = hist["precise_brno_time"] or brno_time
    elif live_delay is None:
        # Not on the live feed right now (likely already finished its run and dropped off)
        # - fall back to the last reading logged for it earlier today rather than going blank.
        live_delay = hist["latest_delay"]
        slavkov_time = slavkov_time or hist["latest_slavkov"]
        brno_time = brno_time or hist["latest_brno"]
    avg_delay, sample_size = historical_average_delay(route_name, dep_time)
    headsign = trips.get(trip_id, (None, None, None))[1]
    eta, dest_sched, eta_note = estimate_office_arrival(route_name, trip_id, headsign, stop_times, live_delay, avg_delay)
    return {
        "departure": dep_time,
        "platform": platform_codes.get(stop_id, "") or None,
        "live_delay_min": live_delay,
        "historical_avg_delay_min": avg_delay,
        "historical_sample_size": sample_size,
        "dest_arrival_scheduled": dest_sched,
        "eta": eta,
        "eta_note": eta_note,
        "slavkov_time": slavkov_time,
        "brno_time": brno_time,
    }


def main():
    now = datetime.datetime.now(PRAGUE_TZ)
    today = now.date()

    static_path = ensure_static_gtfs()
    with zipfile.ZipFile(static_path) as zf:
        route_ids = load_route_ids(zf)
        trips = load_relevant_trips(zf, route_ids)
        stop_times = load_stop_times_for_trips(zf, set(trips.keys()))
        valid_service_ids = load_valid_service_ids(zf, today)
        origin_departures = load_origin_departures(zf, trips, valid_service_ids)
        all_origin_stop_ids = set().union(*ORIGIN_STOP_IDS.values())
        platform_codes = load_platform_codes(zf, all_origin_stop_ids)

    # trip_id -> its scheduled Slavkov departure time, both raw (for shift_hhmm's seconds
    # parsing) and formatted ("HH:MM", for tagging every logged delay reading with the
    # specific departure it belongs to, regardless of which stop the vehicle was tracked at).
    trip_departure_raw = {
        trip_id: dep_str
        for route_name in ROUTES_OF_INTEREST
        for trip_id, dep_str, _stop_id in origin_departures[route_name]
    }
    trip_departure_time = {trip_id: fmt_hhmm(dep_str) for trip_id, dep_str in trip_departure_raw.items()}

    # trip_id -> its scheduled Brno-destination arrival time (raw), for the same reason.
    trip_dest_arrival_raw = {}
    for trip_id, (route_name, _headsign, _service_id) in trips.items():
        key = (trip_id, DEST_STOP_IDS[route_name])
        if key in stop_times:
            trip_dest_arrival_raw[trip_id] = stop_times[key][0]

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
        route_name, headsign, _service_id = trips[trip_id]
        # GTFS-RT zero-pads single-digit platform codes (e.g. "U16332Z02") while the static
        # schedule doesn't ("U16332Z2"), so a literal match silently drops single-digit
        # platforms entirely - strip that padding before looking the stop up.
        stop_id = normalize_stop_id(v.stop_id)
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
        # GTFS-RT feeds sometimes leave a vehicle's last-known position in the feed after
        # it's actually finished its run (lost GPS signal, end of trip, etc.) instead of
        # removing the entity. A position report that's too old to be "live" would produce
        # a huge bogus delay (observed: 206.7 min on a trip scheduled ~3.5h earlier) - skip it.
        report_age_min = (now.timestamp() - actual_epoch) / 60
        if report_age_min > STALE_VEHICLE_MAX_AGE_MINUTES:
            continue
        delay_min = round((actual_epoch - sched_epoch) / 60, 1)
        if abs(delay_min) > MAX_PLAUSIBLE_DELAY_MINUTES:
            continue
        seen_vehicle_ids.add(dedup_key)
        # The observed delay is assumed to hold roughly steady for the rest of the trip, so
        # it's used to estimate the actual (not just scheduled) time at both checkpoints the
        # user actually cares about - Slavkov and the Brno terminus - regardless of which
        # stop the GPS feed happened to report this vehicle at.
        slavkov_time = shift_hhmm(trip_departure_raw[trip_id], delay_min) if trip_id in trip_departure_raw else None
        brno_time = shift_hhmm(trip_dest_arrival_raw[trip_id], delay_min) if trip_id in trip_dest_arrival_raw else None
        rows_by_route[route_name].append({
            "trip_id": trip_id,
            "headsign": headsign,
            "stop_id": stop_id,
            "delay_min": delay_min,
            "vehicle_label": v.vehicle.label,
            "slavkov_time": slavkov_time,
            "brno_time": brno_time,
        })

    car_min, car_note = get_car_drive_minutes()
    car_slot = round_to_slot(now)
    incident_notes = get_incident_notes(now)

    file_exists = os.path.exists(LOG_PATH)
    with open(LOG_PATH, "a", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        if not file_exists:
            writer.writerow(["timestamp", "route", "trip_id", "headsign", "stop_id", "delay_min",
                              "vehicle_label", "departure_time", "car_drive_min", "note",
                              "slavkov_time", "brno_time"])
        ts = now.strftime("%Y-%m-%d %H:%M:%S")
        for route_name in ROUTES_OF_INTEREST.keys():
            entries = rows_by_route[route_name]
            nd_for_log = next_departure(origin_departures[route_name], now)
            if not entries:
                writer.writerow([ts, route_name, "", "NO_ACTIVE_VEHICLE", "", "", "",
                                  fmt_hhmm(nd_for_log[1]) if nd_for_log else "", "", "", "", ""])
                continue
            # pick the vehicle closest to Brno-bound progress; just log all found (usually 1)
            for e in entries:
                writer.writerow([ts, route_name, e["trip_id"], e["headsign"], e["stop_id"], e["delay_min"],
                                  e["vehicle_label"], trip_departure_time.get(e["trip_id"], ""), "", "",
                                  e["slavkov_time"] or "", e["brno_time"] or ""])
        writer.writerow([ts, "CAR", "", "", "", "", "", car_slot, car_min if car_min is not None else "", car_note, "", ""])
        print(f"Logged {ts}: " + ", ".join(f"{r}={len(rows_by_route[r])} vehicle(s)" for r in ROUTES_OF_INTEREST.keys())
              + f", car={car_min} min" + (f" ({car_note})" if car_note else ""))

    car_eta = None
    if car_min is not None:
        car_eta_minutes = round(now.hour * 60 + now.minute + now.second / 60 + car_min) % (24 * 60)
        car_eta = f"{car_eta_minutes // 60:02d}:{car_eta_minutes % 60:02d}"
    car_hist_avg, car_hist_count = historical_average_car(car_slot)

    status = {
        "generated_at": now.isoformat(),
        "car": {
            "drive_min": car_min, "note": car_note, "eta": car_eta,
            "departure_time": car_slot,
            "historical_avg_min": car_hist_avg,
            "historical_sample_size": car_hist_count,
        },
    }
    for route_name in ROUTES_OF_INTEREST.keys():
        nd = next_departure(origin_departures[route_name], now)
        nd_info = build_departure_info(
            route_name, nd[0] if nd else None, fmt_hhmm(nd[1]) if nd else None, nd[2] if nd else None,
            rows_by_route, stop_times, platform_codes, trips)
        status[route_name] = {
            "next_departure": nd_info["departure"],  # "HH:MM"
            "platform": nd_info["platform"],
            "live_delay_min": nd_info["live_delay_min"],
            "historical_avg_delay_min": nd_info["historical_avg_delay_min"],
            "historical_sample_size": nd_info["historical_sample_size"],
            "dest_arrival_scheduled": nd_info["dest_arrival_scheduled"],
            "walk_min": WALK_MINUTES_TO_OFFICE[route_name],
            "eta": nd_info["eta"],
            "eta_note": nd_info["eta_note"],
            "incident": incident_notes[route_name],
            "slavkov_time": nd_info["slavkov_time"],
            "brno_time": nd_info["brno_time"],
        }

        # Realistic morning departures, tracked every weekday regardless of which one is
        # "next" right now, so each builds up its own delay history over the month.
        by_time = {fmt_hhmm(dep_str): (trip_id, stop_id) for trip_id, dep_str, stop_id in origin_departures[route_name]}
        watch_list = []
        for dep_time in MONITORED_DEPARTURES[route_name]:
            match = by_time.get(dep_time)
            trip_id, stop_id = match if match else (None, None)
            watch_list.append(build_departure_info(
                route_name, trip_id, dep_time, stop_id, rows_by_route, stop_times, platform_codes, trips))
        status.setdefault("morning_watch", {})[route_name] = watch_list

    with open(STATUS_PATH, "w", encoding="utf-8") as f:
        json.dump(status, f, ensure_ascii=False, indent=2)
    print(f"Wrote {STATUS_PATH}")


if __name__ == "__main__":
    main()
