import sqlite3, os

__initialized = False

## ON PAUSE FOR NOW...


def get_detour_db():
    global __initialized

    os.makedirs("./data", exist_ok=True)
    conn = sqlite3.connect('data/state.db')

    if not __initialized:
        conn.execute('''
            CREATE TABLE IF NOT EXISTS seen_detours (
                detour_id TEXT,
                trip_id TEXT,
                service_date TEXT
            )''')

        __initialized = True

    return conn

def is_seen(conn, detour_id):
    print(detour_id)
    cur = conn.execute(
        "SELECT 1 FROM seen_detours WHERE detour_id = ?",
        (detour_id,)
    )
    return cur.fetchone() is not None

def mark_seen(conn, trip_id, detour_id, service_date):
    conn.execute(
        "INSERT OR IGNORE INTO seen_detours(trip_id, detour_id, service_date) VALUES (?, ?, ?)",
        (trip_id, detour_id, service_date)
    )

def query_seen(conn, trip_id, detour_id, service_date):
    if not is_seen(conn, trip_id):
        mark_seen(conn, trip_id, detour_id, service_date)
        return False
    return True




