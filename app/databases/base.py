from typing import Any

from sqlalchemy import Connection, Engine
from sqlmodel import SQLModel, inspect


class BaseDatabase:
    def __init__(self) -> None:
        self._engine: Engine | None = None
        self._session: Connection | None = None

    def initialize(self):
        if self._engine:
            SQLModel.metadata.create_all(self._engine)

    def close(self):
        if self._session:
            self._session.close()

    def tables(self):
        if not self._session:
            raise Exception("Can't connect to database.")

        inspector: Any = inspect(self._engine)
        try:
            return inspector.get_table_names()
        except AttributeError:
            return []

    def version(self):
        if not self._session:
            return 0.0

        version_fallback = (
            0,
            0,
        )
        version_tuple = self._session.dialect.server_version_info or version_fallback
        version_major_minor = version_tuple[0:2]
        version_string = ".".join([str(version) for version in version_major_minor])
        return float(version_string)
