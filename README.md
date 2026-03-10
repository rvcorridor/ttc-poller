# TTC poller
In progress - currently, this project collects and processes the GTFS-RT feed from the Toronto Transit Commission for further analysis. 

## Usage
Clone the repository and install `requirements.txt`. If you'd like, you may change the environment variable `DATA_DIR` exported in line 3 of `run.sh` 
to whichever folder the data should to be stored in.

## What it does in detail

Currently, each run of the script polls alerts, detours, vehicle positions, and trip updates; then polls the live vehicle positions every thirty seconds for the next 5 minutes (however, it is very adjustable in `scripts/main.py`). Every poll of the live vehicle positions is instantly processed from an unstructured protocol buffer format into a dataframe and appended to an in-memory array.

At the end of the 5 minutes, the script deduplicates within the in-memory array of vehicle positions and stores it as a parquet file, and then processes the streetcar and bus detour files into a combined streetcar-detours file. To have 24/7 uptime on the script, I used cron to schedule a run of the script every 5 minutes on my cloud Linux server. 

By default, it outputs the raw GTFS-RT protocol buffer files to `$DATA_DIR/bronze/<type>/<type>-{timestamp}.pb` and flushes the files of the batch into `$DATA_DIR/silver/<type>/<date>/<type>-<timestamp>.parquet.sz`. Within each batch, it deduplicates by trip_id, latitude, longitude, and timestamp, which shrinks the average vehicle position poll size from 70 kb to 20 kb.

## Next stpes
I want to turn the raw positional data that I'm currently collecting into historical arrival times to analyze travel times and potentially a delay prediction model, and after finishing that I want to figure out how continuously integrate present data into the analytics.


