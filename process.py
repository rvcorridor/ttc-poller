import gtfs.gtfs_realtime_pb2 as gtfs
import pandas as pd


# print(gtfs.FeedMessage().ParseFromString());

def extract_info(fname: str = "vehicle-positions.pb") -> str | None:
    feed = gtfs.FeedMessage()
    columns = ["Timestamp", "TripID", "RouteID", "Latitude", "Longitude", "Bearing", "Speed", "TargetStop",
               "Occupancy"]

    df = pd.DataFrame(columns=columns)

    pd.set_option('display.max_columns', None)

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
            lst = [vehicle.timestamp, vehicle.trip.trip_id, vehicle.trip.route_id, vehicle.position.latitude,
                   vehicle.position.longitude, vehicle.position.bearing, vehicle.position.speed,
                   vehicle.stop_id, vehicle.occupancy_status]
            df.loc[len(df)] = lst

    print(df)

    out = f'raw/positions-{feed.header.timestamp}.gzip'

    df.to_parquet(out, compression="gzip")

    return out


if __name__ == "__main__":
    extract_info()
