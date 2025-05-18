from os import getenv


class SourceConnectionFactory:
    __source_connections = {
        "mysql": {
            "type": "mysql",
            "table_name": "employee",
            "user": getenv("MYSQL_USER"),
            "password": getenv("MYSQL_PASSWORD"),
            "host": getenv("MYSQL_HOST"),
            "port": getenv("MYSQL_PORT"),
            "db": getenv("MYSQL_DATABASE"),
        },
        "postgresql": {
            "type": "postgresql",
            "schema_name": "public",
            "table_name": "student",
            "user": getenv("POSTGRES_USER"),
            "password": getenv("POSTGRES_PASSWORD"),
            "host": getenv("POSTGRES_HOST"),
            "port": getenv("POSTGRES_PORT"),
            "db": getenv("POSTGRES_DB"),
        },
    }

    def get_source_connection(self, type: str) -> dict[str, str | int]:
        return self.__source_connections[type]
