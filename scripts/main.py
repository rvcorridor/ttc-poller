from datetime import datetime
from zoneinfo import ZoneInfo
import requests, time, os
import process
from process import extract_positions
import pandas as pd

DATA_DIR = os.getenv("DATA_DIR", "..")

pd.set_option('display.max_columns', None)
TORONTO_TZ = ZoneInfo("America/Toronto")

position_buffer: None | pd.DataFrame = None

START_DATETIME = datetime.now(TORONTO_TZ)
DATE_OF_START = START_DATETIME.strftime("%Y%m%d")

HEADERS = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) '
                         'Chrome/91.0.4472.124 Safari/537.36'}

ITEMS = {
    "POSITIONS": {
        "name" : "positions",
        "URL" : "https://gtfsrt.ttc.ca/vehicles/position?format=binary"
    },
    "BUS_DETOURS": {
        "name" : "bus-detours",
        "URL" : "https://gtfsrt.ttc.ca/trips/detour?type=bus&format=binary"
    },
    "STREETCAR_DETOURS": {
        "name": "streetcar-detours",
        "URL" : "https://gtfsrt.ttc.ca/trips/detour?type=streetcar&format=binary"
    },
    "TRIP_UPDATES": {
        "name" : "updates",
        "URL" : "https://gtfsrt.ttc.ca/trips/update?format=binary"
    },
    "SERVICE_ALERTS": {
        "name" : "alerts",
        "URL" : "https://gtfsrt.ttc.ca/alerts/all?format=binary"
    }
}

def retrieve_protobuf(info: dict) -> (str | None, int):
    """
    Gets TTC vehicle positions as a protocol buffer, unprocessed from the API.

    :return: (filename of the protocol buffer, None if unsuccessful else unix timestamp)
    """

    req = requests.get(info["URL"], headers=HEADERS)

    print(f"{int(time.time())} : Request returned code {req.status_code}: {req.reason}")

    if req.ok:
        now_datetime = datetime.now(TORONTO_TZ)
        tor = int(now_datetime.timestamp())
        date_of_now = now_datetime.strftime("%Y%m%d")

        os.makedirs(f"{DATA_DIR}/bronze/{info['name']}/{date_of_now}", exist_ok=True)

        fname = f"{DATA_DIR}/bronze/{info['name']}/{date_of_now}/{info['name']}-{tor}.pb"

        with open(fname, "wb") as out:
            out.write(req.content)

        return fname, tor

    return None, 0

def poll_positions():
    """
    Appends the processed contents of the API request to the current buffer.
    """
    global position_buffer

    fname, retrieval = retrieve_protobuf(ITEMS["POSITIONS"])

    buf = extract_positions(fname=fname)

    if buf is None:
        print(f"ERROR : {retrieval}")
        return

    if position_buffer is None:
        position_buffer = buf
    else:
        position_buffer = pd.concat([position_buffer, buf], ignore_index=True)


def flush_positions():
    """
    Normalizes the content in buffer, and places the content into
    silver/<today's date>/positions-<timestamp>.parquet.gzip.
    """

    global position_buffer

    if position_buffer is None or position_buffer.empty:
        return

    position_buffer = position_buffer.drop_duplicates(subset=['Timestamp', 'TripID', 'Latitude', 'Longitude'])

    position_buffer = position_buffer.drop(columns=['HeaderTime'])

    position_buffer = position_buffer.sort_values(by=['Timestamp', 'TripID'])

    os.makedirs(f"{DATA_DIR}/silver/positions/{DATE_OF_START}", exist_ok=True)

    position_buffer.to_parquet(path=f"{DATA_DIR}/silver/positions/{DATE_OF_START}/positions-{int(START_DATETIME.timestamp())}.parquet.sz", compression="snappy")

    position_buffer = None

def flush_detours(bus_detour_name: str, streetcar_detour_name : str):
    detours, shapes = process.extract_combined_detours(bus_detour_name, streetcar_detour_name)

    os.makedirs(f"{DATA_DIR}/silver/detours/{DATE_OF_START}", exist_ok=True)
    os.makedirs(f"{DATA_DIR}/silver/shapes/{DATE_OF_START}", exist_ok=True)

    detours.to_parquet(f"{DATA_DIR}/silver/detours/{DATE_OF_START}/detours-{int(START_DATETIME.timestamp())}.parquet.sz", compression="snappy")
    shapes.to_parquet(f"{DATA_DIR}/silver/shapes/{DATE_OF_START}/shapes-{int(START_DATETIME.timestamp())}.parquet.sz", compression="snappy")

if __name__ == "__main__":
    bus_detour_fname, tor_bus = retrieve_protobuf(ITEMS["BUS_DETOURS"])
    streetcar_detour_fname, tor_scar =retrieve_protobuf(ITEMS["STREETCAR_DETOURS"])
    retrieve_protobuf(ITEMS["TRIP_UPDATES"])
    retrieve_protobuf(ITEMS["SERVICE_ALERTS"])

    os.makedirs(f"{DATA_DIR}/silver/positions/{DATE_OF_START}", exist_ok=True)

    # Extract 20 times, spaced by 15 seconds = 5 min * 4 times / min

    poll_positions()

    for _ in range(9):
        time.sleep(30)
        poll_positions()

    flush_positions()  # finished querying everything needed!
    flush_detours(bus_detour_fname, streetcar_detour_fname)  # Here so it doesn't waste time and mess up the cycle






