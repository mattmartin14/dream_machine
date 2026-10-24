"""Hardcharger ADBC connection factory."""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager

from adbc_driver_hardcharger import dbapi

from hardcharger_demo.config import Settings


@contextmanager
def connect(settings: Settings) -> Iterator[object]:
    settings.require_etl()
    uri, username, password = settings.connection_credentials()
    connection = dbapi.connect(
        uri=uri,
        username=username,
        password=password,
        autocommit=False,
        settings={"query_timeout": settings.query_timeout_seconds},
        session_tag={"app": "hardcharger-aws-demo", "component": "etl"},
    )
    try:
        yield connection
    finally:
        connection.close()
