from os import getenv

from faker import Faker
from passlib.context import CryptContext
from sqlalchemy_utils import create_database, database_exists
from sqlmodel import Field, Session, SQLModel, create_engine

pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")


def verify_password(plain_password, hashed_password):
    return pwd_context.verify(plain_password, hashed_password)


def hash_password(password):
    return pwd_context.hash(password)


class User(SQLModel, table=True):
    id: int | None = Field(default=None, primary_key=True)
    username: str
    password: str
    email: str


class Employee(SQLModel, table=True):
    id: int | None = Field(default=None, primary_key=True)
    name: str
    address: str


class JobTitle(SQLModel, table=True):
    id: int | None = Field(default=None, primary_key=True)
    name: str


user = getenv("MYSQL_USER")
password = getenv("MYSQL_PASSWORD")
host = getenv("MYSQL_HOST")
port = getenv("MYSQL_PORT")
db = getenv("MYSQL_DATABASE")
url = f"mysql+pymysql://{user}:{password}@{host}:{port}/{db}"
engine = create_engine(url)

if not database_exists(engine.url):
    create_database(engine.url)

SQLModel.metadata.create_all(engine)


fake = Faker()

with Session(engine) as session:
    for loop in range(150):
        print(".", end="", flush=True)
        user = User(
            username=fake.user_name(),
            password=hash_password(fake.password()),
            email=fake.email(),
        )
        employee = Employee(name=fake.name(), address=fake.address())
        job_title = JobTitle(name=fake.job())

        session.add(user)
        session.commit()

        session.add(employee)
        session.commit()

        session.add(job_title)
        session.commit()
