# TTC poller
This project collects and processes the GTFS-RT feed from the Toronto Transit Commission for further analysis. 

By default, it outputs the raw GTFS-RT protocol buffer files to `$DATA_DIR/bronze/<type>/<type>-{timestamp}.pb`, 
and flushes each 5-minute processed batch into `$DATA_DIR/silver/<type>/<date>/<type>-<timestamp>.parquet.sz`. Within each batch,
it deduplicates by trip_id and timestamp.

In progress - building delay prediction model and analytics of historical arrival times

## Usage
Clone the repository and install `requirements.txt`. If you'd like, you may change the environment variable `DATA_DIR` exported in line 3 of `run.sh` 
to whichever folder the data should to be stored in.


