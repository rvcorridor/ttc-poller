import gtfs.gtfs_realtime_pb2 as gtfs
import pandas as pd

DATA_COLUMNS = ["HeaderTime", "Timestamp", "TripID", "RouteID", "Latitude", "Longitude", "Bearing", "Speed", "TargetStop",
                "Occupancy"]

# print(gtfs.FeedMessage().ParseFromString());


def extract_positions(fname: str = "vehicle-positions.pb") -> pd.DataFrame | None:
    # I don't want to see the gtfs stuff in main.py

    feed = gtfs.FeedMessage()

    buf = pd.DataFrame(columns=DATA_COLUMNS)

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


def extract_schedule() -> pd.DataFrame | None:
    return ...


if __name__ == "__main__":


    print(extract_positions())
