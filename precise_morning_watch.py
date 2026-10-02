#!/usr/bin/env python3
"""
Precisely tracks the curated MONITORED_DEPARTURES (bus 106 / train S6) by polling the
live GTFS-RT feed roughly every minute throughout the morning commute window, instead of
relying on a single snapshot per run - which, at a 15-60 min cadence, usually catches a
vehicle somewhere mid-route rather than exactly at Slavkov (departure) or Brno (arrival),
the two checkpoints that actually matter for commute planning.

For each monitored departure, watches for two events: the vehicle being at/near the
Slavkov origin stop, and later at/near the Brno destination stop. Each is logged the
moment it's observed, with the real delay at that exact checkpoint (not estimated/
propagated from elsewhere). Replaces the old fixed-schedule snapshot approach for the
morning window (see track_delays.py, which remains in use for the "Zmerit ted" on-demand
button and other times of day).
"""
import csv
import datetime
import os
import time
import zipfile

from google.transit import gtfs_realtime_pb2

import track_delays as td

POLL_INTERVAL_SECONDS = 60
PRE_WINDOW_MINUTES = 15
POST_WINDOW_BUFFER_MINUTES = 30
HARD_CAP_LOCAL_TIME = "09:30"  # never run past this regardless of schedule, to bound job duration


def resolve_targets(today):
    static_path = td.ensure_static_gtfs()
    with zipfile.ZipFile(static_path) as zf:
        route_ids = td.load_route_ids(zf)
        trips = td.load_relevant_trips(zf, route_ids)
        stop_times = td.load_stop_times_for_trips(zf, set(trips.keys()))
        valid_service_ids = td.load_valid_service_ids(zf, today)
        origin_departures = td.load_origin_departures(zf, trips, valid_service_ids)

    targets = []
    for route_name, dep_times in td.MONITORED_DEPARTURES.items():
        by_time = {td.fmt_hhmm(dep): (trip_id, stop_id) for trip_id, dep, stop_id in origin_departures[route_name]}
        for dep_time in dep_times:
            match = by_time.get(dep_time)
            if not match:
                continue  # doesn't run today (holiday, etc.)
            trip_id, slavkov_stop_id = match
            targets.append({
                "route": route_name, "departure": dep_time, "trip_id": trip_id,
                "slavkov_stop_id": slavkov_stop_id, "brno_stop_id": td.DEST_STOP_IDS[route_name],
                "slavkov_done": False, "brno_done": False,
                "slavkov_time": "", "brno_time": "",
            })
    return targets, stop_times


def compute_window_end_minutes(targets, stop_times):
    latest_min = 0
    for t in targets:
        key = (t["trip_id"], t["brno_stop_id"])
        if key in stop_times:
            h, m, s = (int(x) for x in stop_times[key][0].split(":"))
            latest_min = max(latest_min, h * 60 + m)
    cap_h, cap_m = (int(x) for x in HARD_CAP_LOCAL_TIME.split(":"))
    return min(latest_min + POST_WINDOW_BUFFER_MINUTES, cap_h * 60 + cap_m)


def append_checkpoint_row(t, checkpoint, delay_min, vehicle_label):
    file_exists = os.path.exists(td.LOG_PATH)
    with open(td.LOG_PATH, "a", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        if not file_exists:
            writer.writerow(["timestamp", "route", "trip_id", "headsign", "stop_id", "delay_min",
                              "vehicle_label", "departure_time", "car_drive_min", "note",
                              "slavkov_time", "brno_time"])
        now = datetime.datetime.now(td.PRAGUE_TZ)
        ts = now.strftime("%Y-%m-%d %H:%M:%S")
        stop_id = t["slavkov_stop_id"] if checkpoint == "slavkov" else t["brno_stop_id"]
        writer.writerow([ts, t["route"], t["trip_id"], f"precise-{checkpoint}", stop_id,
                          delay_min, vehicle_label, t["departure"], "", "",
                          t["slavkov_time"], t["brno_time"]])


def poll_once(targets, stop_times, now):
    try:
        rt_data = td.fetch(td.GTFS_RT_URL)
    except Exception as exc:
        print(f"feed fetch failed: {exc}")
        return
    feed = gtfs_realtime_pb2.FeedMessage()
    feed.ParseFromString(rt_data)

    by_trip = {t["trip_id"]: t for t in targets}
    for entity in feed.entity:
        if not entity.HasField("vehicle"):
            continue
        v = entity.vehicle
        t = by_trip.get(v.trip.trip_id)
        if t is None:
            continue
        stop_id = td.normalize_stop_id(v.stop_id)
        for checkpoint, stop_key in (("slavkov", "slavkov_stop_id"), ("brno", "brno_stop_id")):
            if t[f"{checkpoint}_done"] or stop_id != t[stop_key]:
                continue
            key = (v.trip.trip_id, stop_id)
            if key not in stop_times:
                continue
            arrival_str, departure_str = stop_times[key]
            sched_str = departure_str if v.current_status == 1 else arrival_str
            try:
                sched_epoch = td.gtfs_time_to_epoch(sched_str, now.date())
            except Exception:
                continue
            actual_epoch = v.timestamp
            if (now.timestamp() - actual_epoch) / 60 > td.STALE_VEHICLE_MAX_AGE_MINUTES:
                continue
            delay_min = round((actual_epoch - sched_epoch) / 60, 1)
            if abs(delay_min) > td.MAX_PLAUSIBLE_DELAY_MINUTES:
                continue
            actual_time = datetime.datetime.fromtimestamp(actual_epoch, td.PRAGUE_TZ).strftime("%H:%M")
            t[f"{checkpoint}_done"] = True
            t[f"{checkpoint}_time"] = actual_time
            print(f"{t['route']} {t['departure']}: measured {checkpoint} at {actual_time} (delay {delay_min} min)")
            append_checkpoint_row(t, checkpoint, delay_min, v.vehicle.label)


def main():
    now = datetime.datetime.now(td.PRAGUE_TZ)
    targets, stop_times = resolve_targets(now.date())
    if not targets:
        print("No monitored departures run today (holiday/weekend?) - exiting.")
        return

    window_end_min = compute_window_end_minutes(targets, stop_times)
    print(f"Watching {len(targets)} departures until "
          f"{window_end_min // 60:02d}:{window_end_min % 60:02d} local")

    while True:
        now = datetime.datetime.now(td.PRAGUE_TZ)
        now_min = now.hour * 60 + now.minute
        if now_min >= window_end_min or all(t["slavkov_done"] and t["brno_done"] for t in targets):
            break
        poll_once(targets, stop_times, now)
        time.sleep(POLL_INTERVAL_SECONDS)

    unresolved = [t for t in targets if not (t["slavkov_done"] and t["brno_done"])]
    for t in unresolved:
        missing = [c for c in ("slavkov", "brno") if not t[f"{c}_done"]]
        print(f"Window ended, still missing for {t['route']} {t['departure']}: {missing}")
    print(f"Done watching. {len(targets) - len(unresolved)}/{len(targets)} fully resolved.")

    # Regenerate latest_status.json from a fresh live snapshot + the now-enriched history.
    td.main()


if __name__ == "__main__":
    main()
