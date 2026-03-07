import datetime

import requests, time, os
from process import extract_positions
import pandas as pd

pd.set_option('display.max_columns', None)

position_buffer: None | pd.DataFrame = None
now = time.time()
date_of_start = datetime.datetime.fromtimestamp(now).date()

base_url = "https://gtfsrt.ttc.ca/"
vehicle_ext = "vehicles/position"
headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) '
                         'Chrome/91.0.4472.124 Safari/537.36'}


def retrieve_positions() -> (str | None, int):
    """
    Gets TTC vehicle positions as a protocol buffer, unprocessed from the API.

    :return: (filename of the protocol buffer, None if unsuccessful else unix timestamp)
    """

    req = requests.get("https://gtfsrt.ttc.ca/vehicles/position?format=binary", headers=headers)

    print(f"{int(time.time())} : Request returned code {req.status_code}: {req.reason}")

    if req.ok:
        tor = int(time.time())
        date_of_now = datetime.datetime.fromtimestamp(tor).date()

        os.makedirs(f"bronze/{date_of_now}", exist_ok=True)

        fname = f"bronze/{date_of_now}/positions-{tor}.pb"

        with open(fname, "wb") as out:
            out.write(req.content)

        return fname, tor

    return None, 0


def poll_positions():
    """
    Appends the processed contents of the API request to the current buffer.
    """
    global position_buffer

    fname, retrieval = retrieve_positions()

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

    os.makedirs(f"silver/{date_of_start}", exist_ok=True)

    position_buffer.to_parquet(path=f"silver/{date_of_start}/positions-{int(now)}.parquet.sz", compression="snappy")

    position_buffer = None

def retrieve_bus_trip_detours() -> (str | None, int): # This does it all basically
    req = requests.get("https://gtfsrt.ttc.ca/trips/detour?type=bus&format=binary", headers=headers)

    print(f"{int(time.time())} : Bus trip detours returned code {req.status_code}")

    if req.ok:

        tor = int(time.time())
        date_of_now = datetime.datetime.fromtimestamp(tor).date()

        os.makedirs(f"bronze/{date_of_now}", exist_ok=True)

        fname = f"bronze/{date_of_now}/bus-trip-detours-{tor}.pb"

        with open(fname, "wb") as out:
            out.write(req.content)

        return fname, tor

    return None, 0

def retrieve_streetcar_trip_detours() -> (str | None, int): # This does it all basically
    req = requests.get("https://gtfsrt.ttc.ca/trips/detour?type=streetcar&format=binary", headers=headers)

    print(f"{int(time.time())} : Streetcar trip detours returned code {req.status_code}")

    if req.ok:

        tor = int(time.time())
        date_of_now = datetime.datetime.fromtimestamp(tor).date()

        os.makedirs(f"bronze/{date_of_now}", exist_ok=True)

        fname = f"bronze/{date_of_now}/streetcar-trip-detours-{tor}.pb"

        with open(fname, "wb") as out:
            out.write(req.content)

        return fname, tor

    return None, 0

def retrieve_trip_updates() -> (str | None, int):
    req = requests.get("https://gtfsrt.ttc.ca/trips/update?format=binary", headers=headers)

    print(f"{int(time.time())} : Trip updates returned code {req.status_code}")

    if req.ok:
        tor = int(time.time())
        date_of_now = datetime.datetime.fromtimestamp(tor).date()

        os.makedirs(f"bronze/{date_of_now}", exist_ok=True)

        fname = f"bronze/{date_of_now}/updates-{tor}.pb"

        with open(fname, "wb") as out:
            out.write(req.content)

        return fname, tor

    return None, 0

def retrieve_service_alerts() -> (str | None, int):
    req = requests.get("https://gtfsrt.ttc.ca/alerts/all?format=binary", headers=headers)

    print(f"{int(time.time())} : Service alerts returned code {req.status_code}")

    if req.ok:
        tor = int(time.time())
        date_of_now = datetime.datetime.fromtimestamp(tor).date()

        os.makedirs(f"bronze/{date_of_now}", exist_ok=True)

        fname = f"bronze/{date_of_now}/alerts-{tor}.pb"

        with open(fname, "wb") as out:
            out.write(req.content)

        return fname, tor

    return None, 0




if __name__ == "__main__":

    retrieve_bus_trip_detours()
    retrieve_streetcar_trip_detours()
    retrieve_service_alerts()
    retrieve_trip_updates()

    os.makedirs(f"silver/{date_of_start}", exist_ok=True)

    # Extract 20 times, spaced by 15 seconds = 5 min * 4 times / min

    poll_positions()

    for _ in range(19):
        time.sleep(15)
        poll_positions()

    flush_positions()  # finished querying everything needed!






