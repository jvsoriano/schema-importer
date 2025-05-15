from contextlib import asynccontextmanager

from fastapi import FastAPI

from app.databases.sqlite import create_db_and_tables
from app.routers import source_connections


# Create database tables on startup
@asynccontextmanager
async def lifespan(app: FastAPI):
    create_db_and_tables()
    yield


app = FastAPI(title="Schema Importer", lifespan=lifespan)
app.include_router(source_connections.router)
