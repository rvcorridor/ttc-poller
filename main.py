import datetime

import requests, time, os
from process import extract_positions
import pandas as pd

pd.set_option('display.max_columns', None)

buffer: None | pd.DataFrame = None
now = time.time()
today = datetime.datetime.fromtimestamp(now).date()

base_url = "https://gtfsrt.ttc.ca/"
vehicle_ext = "vehicles/position"
headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) '
                         'Chrome/91.0.4472.124 Safari/537.36'}


def retrieve() -> (str | None, int):
    """
    Gets TTC vehicle positions as a protocol buffer, unprocessed from the API.

    :return: (filename of the protocol buffer, None if unsuccessful else unix timestamp)
    """

    req = requests.get("https://gtfsrt.ttc.ca/vehicles/position?format=binary", headers=headers)

    print(f"{int(time.time())} : Request returned code {req.status_code}: {req.reason}")

    if req.ok:
        tor = int(time.time())
        fname = f"bronze/positions-{tor}.pb"

        with open(fname, "wb") as out:
            out.write(req.content)

        return fname, tor

    return None, 0


def activity():
    """
    Appends the processed contents of the API request to the current buffer.
    """
    global buffer

    fname, retrieval = retrieve()

    buf = extract_positions(fname=fname)

    if buf is None:
        print(f"ERROR : {retrieval}")
        return

    if buffer is None:
        buffer = buf
    else:
        buffer = pd.concat([buffer, buf], ignore_index=True)


def flush():
    """
    Normalizes the content in buffer, and places the content into
    silver/<today's date>/positions-<timestamp>.parquet.gzip.
    """

    global buffer

    if buffer is None or buffer.empty:
        return

    buffer = buffer.drop_duplicates(subset=['Timestamp', 'TripID', 'Latitude', 'Longitude'])

    buffer = buffer.drop(columns=['HeaderTime'])

    buffer = buffer.sort_values(by=['Timestamp', 'TripID'])

    os.makedirs(f"silver/{today}", exist_ok=True)

    buffer.to_parquet(path=f"silver/{today}/positions-{int(now)}.parquet.sz", compression="snappy")

    buffer = None


if __name__ == "__main__":

    os.makedirs(f"silver/{today}", exist_ok=True)

    # Extract 20 times, spaced by 15 seconds = 5 min * 4 times / min

    activity()

    for _ in range(19):
        time.sleep(15)
        activity()

    flush()  # finished querying everything needed!






