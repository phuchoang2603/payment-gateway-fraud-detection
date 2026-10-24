import json
import sqlite3


class FragmentIndex:
    def __init__(self, path):
        self.connection = sqlite3.connect(path)
        self.connection.executescript("""
            PRAGMA journal_mode=OFF;
            PRAGMA cache_size=-2048;
            PRAGMA temp_store=MEMORY;
            CREATE TABLE files (key TEXT PRIMARY KEY, table_name TEXT, item TEXT);
            CREATE TABLE partitions (key TEXT, day TEXT, rows INTEGER);
            CREATE INDEX partition_day ON partitions(day, key);
        """)

    def add(self, files):
        with self.connection:
            for item in files:
                self.connection.execute(
                    "INSERT INTO files VALUES (?, ?, ?)",
                    (item["key"], item["table"], json.dumps(item, sort_keys=True)),
                )
                self.connection.executemany(
                    "INSERT INTO partitions VALUES (?, ?, ?)",
                    ((item["key"], day, rows) for day, rows in item["partitions"].items()),
                )

    def days(self, table):
        return [
            row[0]
            for row in self.connection.execute(
                "SELECT DISTINCT day FROM partitions JOIN files USING(key) WHERE table_name=? ORDER BY day", (table,)
            )
        ]

    def sources(self, table, day):
        for encoded, rows in self.connection.execute(
            "SELECT item, rows FROM partitions JOIN files USING(key) WHERE table_name=? AND day=? ORDER BY key",
            (table, day),
        ):
            yield json.loads(encoded), rows

    def total_bytes(self):
        return sum(json.loads(row[0])["bytes"] for row in self.connection.execute("SELECT item FROM files"))

    def close(self):
        self.connection.close()
