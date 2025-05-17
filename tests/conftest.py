import pytest
from sqlmodel import Session, SQLModel, StaticPool, create_engine

sqlite_url = "sqlite:///:memory:"
connect_args = {"check_same_thread": False}
engine = create_engine(sqlite_url, connect_args=connect_args, poolclass=StaticPool)


def get_session_replacement():
    with Session(engine) as session:
        yield session


@pytest.fixture(scope="session", autouse=True)
def setup_and_teardown():
    SQLModel.metadata.create_all(engine)
    yield
    SQLModel.metadata.drop_all(engine)
