from typing import Literal

from sqlmodel import Field, SQLModel


class SourceConnectionBase(SQLModel):
    """Base data model of source connection."""

    schema_name: str | None = None  # for postgresql only
    table_name: str | None = None

    # connection requirements
    user: str
    password: str
    host: str
    port: int
    db: str


class SourceConnectionCreate(SourceConnectionBase):
    """
    Data model for creating source connection.
    Only accepts 'mysql' or 'postgresql' as type.
    """

    type: Literal["mysql", "postgresql"]


class SourceConnection(SourceConnectionBase, table=True):
    """Table model of source connection."""

    id: int | None = Field(default=None, primary_key=True)
    type: str  # mysql or postgresql

    @property
    def url(self):
        if self.type == "mysql":
            return f"mysql+pymysql://{self.user}:{self.password}@{self.host}:{self.port}/{self.db}"
        else:
            return f"postgresql://{self.user}:{self.password}@{self.host}:{self.port}/{self.db}"


class SourceConnectionUpdate(SQLModel):
    """
    Data model for updating source connection.
    Does not allow updating of source connection type.
    """

    schema_name: str | None = None  # for postgresql only
    table_name: str | None = None

    # connection requirements
    user: str | None = None
    password: str | None = None
    host: str | None = None
    port: int | None = None
    db: str | None = None


class SourceConnectionPublic(SQLModel):
    """
    Public data model of source connection.
    Should not display password in public.
    """

    id: int
    type: str
    schema_name: str | None = None  # for postgresql only
    table_name: str

    user: str
    host: str
    port: int
    db: str
