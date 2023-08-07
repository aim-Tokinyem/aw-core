import json
import logging
import os
from datetime import datetime, timedelta, timezone
from typing import Iterable, List, Optional

from aw_core.dirs import get_data_dir
from aw_core.models import Event

from .abstract import AbstractStorage

from .config import load_config

logger = logging.getLogger(__name__)

LATEST_VERSION = 1

# The max integer value in PostgreSQL is 2^63 - 1
MAX_TIMESTAMP = 2 ** 63 - 1

CREATE_BUCKETS_TABLE = """
    CREATE TABLE IF NOT EXISTS buckets (
        id TEXT PRIMARY KEY,
        name TEXT,
        type TEXT NOT NULL,
        client TEXT NOT NULL,
        hostname TEXT NOT NULL,
        created TEXT NOT NULL,
        datastr JSONB NOT NULL
    )
"""

CREATE_EVENTS_TABLE = """
    CREATE TABLE IF NOT EXISTS events (
        id SERIAL PRIMARY KEY,
        bucketrow INTEGER NOT NULL,
        starttime BIGINT NOT NULL,
        endtime BIGINT NOT NULL,
        datastr JSONB NOT NULL,
        FOREIGN KEY (bucketrow) REFERENCES buckets(rowid)
    )
"""

INDEX_BUCKETS_TABLE_ID = """
    CREATE INDEX IF NOT EXISTS event_index_id ON events(id);
"""

INDEX_EVENTS_TABLE_STARTTIME = """
    CREATE INDEX IF NOT EXISTS event_index_starttime ON events(bucketrow, starttime);
"""

INDEX_EVENTS_TABLE_ENDTIME = """
    CREATE INDEX IF NOT EXISTS event_index_endtime ON events(bucketrow, endtime);
"""


def _rows_to_events(rows: Iterable) -> List[Event]:
    events = []
    for row in rows:
        eid = row[0]
        starttime = datetime.fromtimestamp(row[1] / 1000000, timezone.utc)
        endtime = datetime.fromtimestamp(row[2] / 1000000, timezone.utc)
        duration = endtime - starttime
        data = json.loads(row[3])
        events.append(Event(id=eid, timestamp=starttime, duration=duration, data=data))
    return events

def get_app_title_strings(event_data):
    if 'app' in event_data:
        appstr = event_data.get("app") or "unknown"
        titlestr = event_data.get("title") or "unknown"
    else:
        appstr = "unknown"
        titlestr = "unknown"
    return appstr, titlestr

class PostgreyStorage(AbstractStorage):
    sid = "postgrey"
    #load_config()
    def __init__(
        self,
        testing,
        connection_string: Optional[str] = None,
        enable_lazy_commit=True,
    ) -> None:
        self.testing = testing
        self.enable_lazy_commit = enable_lazy_commit

        ds_name = self.sid + ("-testing" if testing else "")
        
        config = load_config()
        if not connection_string:
            # Update the following variables according to your PostgreSQL setup
            drivers = config["postgreysql"]["drivers"]
            host = config["postgreysql"]["host"]
            port = config["postgreysql"]["port"]
            database = config["postgreysql"]["database"]
            username = config["postgreysql"]["username"]
            password = config["postgreysql"]["password"]

            # Create the connection string
            connection_string = f"DRIVER={{{drivers}}};SERVER={host};PORT={port};DATABASE={database};UID={username};PWD={password}"

        self.conn = pyodbc.connect(connection_string)
        logger.info("Connected to PostgreSQL database")

        # Create tables
        self.conn.execute(CREATE_BUCKETS_TABLE)
        self.conn.execute(CREATE_EVENTS_TABLE)
        self.conn.execute(INDEX_BUCKETS_TABLE_ID)
        self.conn.execute(INDEX_EVENTS_TABLE_STARTTIME)
        self.conn.execute(INDEX_EVENTS_TABLE_ENDTIME)
        self.conn.execute("SET SESSION synchronous_commit = off;")
        self.commit()

        self.last_commit = datetime.now()
        self.num_uncommitted_statements = 0

    def commit(self):
        self.conn.commit()
        self.last_commit = datetime.now()
        self.num_uncommitted_statements = 0

    def conditional_commit(self, num_statements):
        if self.enable_lazy_commit:
            self.num_uncommitted_statements += num_statements
            if self.num_uncommitted_statements > 50:
                self.commit()
            if (datetime.now() - self.last_commit) > timedelta(seconds=10):
                self.commit()
        else:
            self.commit()

    def buckets(self):
        buckets = {}
        c = self.conn.cursor()
        for row in c.execute(
            "SELECT id, name, type, client, hostname, created, datastr FROM buckets"
        ):
            buckets[row[0]] = {
                "id": row[0],
                "name": row[1],
                "type": row[2],
                "client": row[3],
                "hostname": row[4],
                "created": row[5],
                "data": json.loads(row[6] or "{}"),
            }
        return buckets

    def create_bucket(
        self,
        bucket_id: str,
        type_id: str,
        client: str,
        hostname: str,
        created: str,
        name: Optional[str] = None,
        data: Optional[dict] = None,
    ):
        self.conn.execute(
            "INSERT INTO buckets(id, name, type, client, hostname, created, datastr) "
            + "VALUES (?, ?, ?, ?, ?, ?, ?)",
            [
                bucket_id,
                name,
                type_id,
                client,
                hostname,
                created,
                json.dumps(data or {}),
            ],
        )
        self.commit()
        return self.get_metadata(bucket_id)

    def update_bucket(
        self,
        bucket_id: str,
        type_id: Optional[str] = None,
        client: Optional[str] = None,
        hostname: Optional[str] = None,
        name: Optional[str] = None,
        data: Optional[dict] = None,
    ):
        update_values = [
            ("type", type_id),
            ("client", client),
            ("hostname", hostname),
            ("name", name),
            ("datastr", json.dumps(data) if data is not None else None),
        ]
        updates, values = zip(*[(k, v) for k, v in update_values if v is not None])
        if not updates:
            raise ValueError("At least one field must be updated.")

        sql = (
            "UPDATE buckets SET "
            + ", ".join(f"{u} = ?" for u in updates)
            + " WHERE id = ?"
        )
        self.conn.execute(sql, (*values, bucket_id))
        self.commit()
        return self.get_metadata(bucket_id)

    def delete_bucket(self, bucket_id: str):
        self.conn.execute(
            "DELETE FROM events WHERE bucketrow IN (SELECT rowid FROM buckets WHERE id = ?)",
            [bucket_id],
        )
        cursor = self.conn.execute("DELETE FROM buckets WHERE id = ?", [bucket_id])
        self.commit()
        if cursor.rowcount != 1:
            raise Exception("Bucket did not exist, could not delete")

    def get_metadata(self, bucket_id: str):
        c = self.conn.cursor()
        res = c.execute(
            "SELECT id, name, type, client, hostname, created, datastr FROM buckets WHERE id = ?",
            [bucket_id],
        )
        row = res.fetchone()
        if row is not None:
            return {
                "id": row[0],
                "name": row[1],
                "type": row[2],
                "client": row[3],
                "hostname": row[4],
                "created": row[5],
                "data": json.loads(row[6] or "{}"),
            }
        else:
            raise Exception("Bucket did not exist, could not get metadata")

    def insert_one(self, bucket_id: str, event: Event) -> Event:
        c = self.conn.cursor()
        starttime = event.timestamp.timestamp() * 1000000
        endtime = starttime + (event.duration.total_seconds() * 1000000)
        datastr = json.dumps(event.data)
        duration = event.duration.total_seconds()
        timestamp = event.timestamp
        appstr, titlestr = get_app_title_strings(event.data)
        c.execute(
            "INSERT INTO events(bucketrow, starttime, endtime, timestamps, duration, datastr, appstr, titlestr) "
            "VALUES ((SELECT rowid FROM buckets WHERE id = ?), ?, ?, ?, ?, ?, ?, ?) "
            "RETURNING id",
            [bucket_id, starttime, endtime, timestamp, duration, datastr, appstr, titlestr],
        )
        event.id = c.fetchone()[0]
        self.conditional_commit(1)
        return event
    
    def insert_many(self, bucket_id: str, events: List[Event]) -> None:
        events_insert = [e for e in events if e.id is None]
        event_rows = []
        for event in events_insert:
            starttime = event.timestamp.timestamp() * 1000000
            endtime = starttime + (event.duration.total_seconds() * 1000000)
            datastr = json.dumps(event.data)
            appstr, titlestr = get_app_title_strings(event.data)
            duration = event.duration.total_seconds()
            timestamp = event.timestamp
            event_rows.append((bucket_id, starttime, endtime, timestamp, duration, datastr, appstr, titlestr))

        query = (
            "INSERT INTO events(bucketrow, starttime, endtime, timestamps, duration, datastr, appstr, titlestr) "
            + "VALUES ((SELECT rowid FROM buckets WHERE id = ?), ?, ?, ?, ?, ?, ?, ?)"
        )
        self.conn.executemany(query, event_rows)
        self.conditional_commit(len(event_rows))

    def replace_last(self, bucket_id: str, event: Event) -> bool:
        starttime = event.timestamp.timestamp() * 1000000
        endtime = starttime + (event.duration.total_seconds() * 1000000)
        datastr = json.dumps(event.data)
        appstr, titlestr = get_app_title_strings(event.data)
        duration = event.duration.total_seconds()
        timestamp = event.timestamp
        query = """UPDATE events
                SET starttime = ?, endtime = ?, datastr = ?, timestamps = ?, duration = ?, appstr = ?, titlestr = ?
                WHERE id = (
                        SELECT id FROM events WHERE endtime =
                            (SELECT max(endtime) FROM events WHERE bucketrow =
                                (SELECT rowid FROM buckets WHERE id = ?) LIMIT 1))"""

        self.conn.execute(query, [starttime, endtime, datastr, timestamp, duration, appstr, titlestr, bucket_id])
        self.conditional_commit(1)
        return True


    def delete(self, bucket_id, event_id):
        query = (
            "DELETE FROM events "
            + "WHERE id = ? AND bucketrow = (SELECT b.rowid FROM buckets b WHERE b.id = ?)"
        )
        cursor = self.conn.execute(query, [event_id, bucket_id])
        return cursor.rowcount == 1

    def replace(self, bucket_id: str, event_id: int, event) -> bool:
        starttime = event.timestamp.timestamp() * 1000000
        endtime = starttime + (event.duration.total_seconds() * 1000000)
        datastr = json.dumps(event.data)
        duration = event.duration.total_seconds()
        timestamp = event.timestamp.timestamp()
        
        appstr, titlestr = get_app_title_strings(event.data)
        
        query = """UPDATE events
                    SET bucketrow = (SELECT rowid FROM buckets WHERE id = ?),
                        starttime = ?,
                        endtime = ?,
                        datastr = ?,
                        duration = ?,
                        timestamps = ?,
                        appstr = ?,
                        titlestr = ?
                    WHERE id = ?"""
        self.conn.execute(query, [bucket_id, starttime, endtime, datastr, duration, timestamp, appstr, titlestr, event_id])
        self.conditional_commit(1)
        return True

    def get_event(
        self,
        bucket_id: str,
        event_id: int,
    ) -> Optional[Event]:
        self.commit()
        c = self.conn.cursor()
        query = """
            SELECT id, starttime, endtime, datastr
            FROM events
            WHERE bucketrow = (SELECT rowid FROM buckets WHERE id = ?) AND id = ?
            LIMIT 1
        """
        rows = c.execute(query, [bucket_id, event_id])
        events = _rows_to_events(rows)
        if events:
            return events[0]
        else:
            return None

    def get_events(
        self,
        bucket_id: str,
        limit: int,
        starttime: Optional[datetime] = None,
        endtime: Optional[datetime] = None,
    ):
        if limit == 0:
            return []
        elif limit < 0:
            limit = None
        self.commit()
        c = self.conn.cursor()
        starttime_i = starttime.timestamp() * 1000000 if starttime else 0
        endtime_i = endtime.timestamp() * 1000000 if endtime else MAX_TIMESTAMP
        query = """
            SELECT id, starttime, endtime, datastr
            FROM events
            WHERE bucketrow = (SELECT rowid FROM buckets WHERE id = ?)
            AND endtime >= ? AND starttime <= ?
            ORDER BY endtime DESC LIMIT ?
        """
        rows = c.execute(query, [bucket_id, starttime_i, endtime_i, limit])
        events = _rows_to_events(rows)
        return events

    def get_eventcount(
        self,
        bucket_id: str,
        starttime: Optional[datetime] = None,
        endtime: Optional[datetime] = None,
    ):
        self.commit()
        c = self.conn.cursor()
        starttime_i = starttime.timestamp() * 1000000 if starttime else 0
        endtime_i = endtime.timestamp() * 1000000 if endtime else MAX_TIMESTAMP
        query = (
            "SELECT count(*) "
            + "FROM events "
            + "WHERE bucketrow = (SELECT rowid FROM buckets WHERE id = ?) "
            + "AND endtime >= ? AND starttime <= ?"
        )
        rows = c.execute(query, [bucket_id, starttime_i, endtime_i])
        row = rows.fetchone()
        eventcount = row[0]
        return eventcount
    
    

