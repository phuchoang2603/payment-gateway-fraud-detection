from contextlib import contextmanager
from importlib import import_module
from threading import Lock

import ibis

_CONNECTION_LOCK = Lock()


@contextmanager
def controlled_native_connection(connection):
    # Data Contract CLI 1.2.7 ignores duckdb_connection for DuckDB server types.
    module = import_module("datacontract.engines.ibis.connections.connect")
    with _CONNECTION_LOCK:
        original = module._connect_duckdb_database
        module._connect_duckdb_database = lambda *args, **kwargs: ibis.duckdb.from_connection(connection)
        try:
            yield
        finally:
            module._connect_duckdb_database = original
