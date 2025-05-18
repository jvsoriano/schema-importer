from os import getenv
from random import choice

from faker import Faker
from sqlalchemy_utils import create_database, database_exists
from sqlmodel import Field, Session, SQLModel, create_engine, text


class Student(SQLModel, table=True):
    id: int | None = Field(default=None, primary_key=True)
    name: str
    age: int


class Teacher(SQLModel, table=True):
    id: int | None = Field(default=None, primary_key=True)
    name: str
    age: int


user = getenv("POSTGRES_USER")
password = getenv("POSTGRES_PASSWORD")
host = getenv("POSTGRES_HOST")
port = getenv("POSTGRES_PORT")
db = getenv("POSTGRES_DB")
url = f"postgresql://{user}:{password}@{host}:{port}/{db}"
engine = create_engine(url)

if not database_exists(engine.url):
    create_database(engine.url)

SQLModel.metadata.create_all(engine)


def create_user(user, password, session):
    session.execute(text(f"CREATE USER {user} WITH ENCRYPTED PASSWORD '{password}';"))
    session.commit()


def grant_schema_privilege(user, schema, session):
    session.execute(text(f"GRANT ALL ON SCHEMA {schema} TO {user};"))
    session.commit()

    session.execute(text(f"GRANT ALL ON ALL TABLES IN SCHEMA {schema} TO {user};"))
    session.commit()


def grant_database_privilege(user, database, session):
    session.execute(text(f"GRANT ALL ON DATABASE {database} TO {user};"))
    session.commit()


with engine.connect() as session:
    # create user without schema and database privileges
    create_user(user="johndoe", password="johndoepass", session=session)

    # create user with schema privilege
    create_user(user="janedoe", password="janedoepass", session=session)
    grant_schema_privilege(user="janedoe", schema="public", session=session)

    # create user with schema and database privilege
    create_user(user="jaydoe", password="jaydoepass", session=session)
    grant_schema_privilege(user="jaydoe", schema="public", session=session)
    grant_database_privilege(user="jaydoe", database=db, session=session)

fake = Faker()

with Session(engine) as session:
    for loop in range(150):
        print(".", end="", flush=True)
        student = Student(name=fake.name(), age=choice([18, 19, 20]))
        teacher = Teacher(name=fake.name(), age=choice([33, 40, 45]))

        session.add(student)
        session.commit()

        session.add(teacher)
        session.commit()
