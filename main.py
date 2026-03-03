#!/usr/bin/python
import requests, time, os
from process import extract_info

base_url = "https://gtfsrt.ttc.ca/"
vehicle_ext = "vehicles/position"
headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) '
                         'Chrome/91.0.4472.124 Safari/537.36' }


def retrieve() -> str | None:
    req = requests.get("https://gtfsrt.ttc.ca/vehicles/position?format=binary", headers=headers)

    print(f"{int(time.time())} : Request returned code {req.status_code}: {req.reason}")

    if req.ok:
        fname = f"pbufs/{int(time.time())}.pb"

        with open(fname, "wb") as out:
            out.write(req.content)

        return fname




fname = retrieve()

if fname is not None:
    res = extract_info(fname=fname)

    if res is not None:
        os.remove(fname)
    else:
        print(f"Something is wrong with {fname}; Please investigate")




