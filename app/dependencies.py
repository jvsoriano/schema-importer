# Create database session dependency
from typing import Annotated

from fastapi import Depends
from sqlmodel import Session

from app.databases.sqlite import engine


def get_session():
    with Session(engine) as session:
        yield session


SessionDep = Annotated[Session, Depends(get_session)]
