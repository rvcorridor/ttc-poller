from pandas import DataFrame

import gtfs.gtfs_realtime_pb2 as gtfs
import pandas as pd
import numpy as np
import state

POSITION_COLUMNS = ["HeaderTime", "Timestamp", "TripID", "RouteID", "Latitude", "Longitude", "Bearing", "Speed", "TargetStop",
                "Occupancy"]

DETOUR_COLUMNS = ["DetourID", "ShapeID", "StartStop", "EndStop", ]
TRIP_DETOUR_COLUMNS = ["TripID", "DetourID"]
DETOUR_DATE_COLUMNS = ["AffectedDate", "DetourID"]
DETOUR_SHAPE_COLUMNS = ["ShapeID", "EncodedPolyline"]

# print(gtfs.FeedMessage().ParseFromString());


def extract_positions(fname: str = "samples/vehicle-positions.pb") -> pd.DataFrame | None:
    # I don't want to see the gtfs stuff in main.py

    feed = gtfs.FeedMessage()

    buf = pd.DataFrame(columns=POSITION_COLUMNS)

    try:
        with open(fname, mode="rb") as r:
            feed.ParseFromString(r.read())
            # print(feed)
    except:
        return

    for ent in feed.entity:
        # We only care about scheduled,

        vehicle = ent.vehicle
        # if vehicle.trip.schedule_relationship != gtfs.TripDescriptor.SCHEDULED:
        # print(vehicle)

        if vehicle.HasField("trip"):  # TTC gives position of vehicles still in the garage lol
            lst = [feed.header.timestamp, vehicle.timestamp, vehicle.trip.trip_id, vehicle.trip.route_id, vehicle.position.latitude,
                   vehicle.position.longitude, vehicle.position.bearing, vehicle.position.speed,
                   vehicle.stop_id, vehicle.occupancy_status]
            buf.loc[len(buf)] = lst

    # print(df)

    return buf


def extract_schedule(fname: str = "samples/schedule.pb") -> pd.DataFrame | None:
    return ...

def extract_updates(fname: str = "samples/schedule.pb") -> pd.DataFrame | None:
    return ...

def extract_alerts(fname: str = "samples/alerts.pb") -> pd.DataFrame | None:
    feed = gtfs.FeedMessage()

    try:
        with open(fname, mode="rb") as r:
            feed.ParseFromString(r.read())
    except:
        return

    for alert in feed.entity:
        print(alert)

    return

def extract_detours(fname: str = "samples/bus-detours.pb") \
        -> tuple[DataFrame|None, DataFrame|None, DataFrame|None, DataFrame|None]:
    feed = gtfs.FeedMessage()
    detours = pd.DataFrame(columns=DETOUR_COLUMNS)
    detour_shapes = pd.DataFrame(columns=DETOUR_SHAPE_COLUMNS)
    affected_trips = None
    affected_dates = None

    try:
        with open(fname, mode="rb") as r:
            feed.ParseFromString(r.read())
    except:
        return None,None,None,None

    for detour in feed.entity:
        if detour.HasField("trip_modifications"): # Log to trip_modifications
            # print("trip_modifications")
            shape, start, end = None, None, None
            # print(detour.id)
            # print(detour.trip_modifications)
            # modifications

            for selected in detour.trip_modifications.selected_trips:
                shape = selected.shape_id
                # print(selected.trip_ids)

                temp = pd.DataFrame()
                temp["TripID"] = np.asarray(selected.trip_ids)
                temp["DetourID"] = np.full(len(temp), detour.id);
                # print(temp.head())
                if affected_trips is None:
                    affected_trips = temp
                else:
                    affected_trips = pd.concat([affected_trips, temp], ignore_index=True)

            for modifications in detour.trip_modifications.modifications:
                start = modifications.start_stop_selector.stop_sequence
                end = modifications.end_stop_selector.stop_sequence

            # Extract detour dates
            temp = pd.DataFrame()
            temp["AffectedDate"] = np.asarray(detour.trip_modifications.service_dates)
            temp["DetourID"] = np.full(len(temp), detour.id);
            if affected_dates is None:
                affected_dates = temp
            else:
                affected_dates = pd.concat([affected_dates, temp], ignore_index=True)

            row = [detour.id, shape, start, end]
            detours.loc[len(detours)] = row

        elif detour.HasField("shape"): # Log to detour_shapes
            # print("shape")
            # print(detour.id)
            # print(detour.shape.shape_id)
            # print(detour.shape)

            row = [detour.shape.shape_id, detour.shape.encoded_polyline]
            detour_shapes.loc[len(detour_shapes)] = row

    return detours, affected_dates, affected_trips, detour_shapes

def combine_bus_and_streetcar_data(bus_fname: str="samples/bus-detours.pb", streetcar_fname: str="samples/streetcar-detours.pb") \
        -> tuple[DataFrame, DataFrame, DataFrame, DataFrame]:
    bus_buff = extract_detours(bus_fname)
    streetcar_buff = extract_detours(streetcar_fname)

    tup = []

    for a, b in zip(bus_buff, streetcar_buff):
        if a is None:
            tup.append(b)
        elif b is None:
            tup.append(a)
        else:
            tup.append(pd.concat([a,b], ignore_index=True))

    return tuple(tup)

def process_detours(detours, affected_dates, affected_trips) -> pd.DataFrame:
    ab = pd.merge(detours, affected_dates, on="DetourID")
    abc = pd.merge(ab, affected_trips, on="DetourID")
    # print(abc)

    return abc

def extract_combined_detours(bus_fname: str="samples/bus-detours.pb", streetcar_fname: str="samples/streetcar-detours.pb"):
    a, b, c, d = combine_bus_and_streetcar_data(bus_fname, streetcar_fname)

    abc = process_detours(a, b, c)

    return abc, d



if __name__ == "__main__":
    pd.set_option('display.max_columns', None)
    """

    print()
    a, b, c, d = extract_detours()
    print(a)
    print(b)
    print(c)
    print(d)

    processDetours(a, b, c, d)"""

    print(combine_bus_and_streetcar_data())



