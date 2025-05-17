class SourceConnectionFactory:
    __source_connections = {
        "mysql": {
            "type": "mysql",
            "table_name": "employees",
            "user": "root",
            "password": "example",
            "host": "localhost",
            "port": 3306,
            "db": "integrate",
        },
        "postgresql": {
            "type": "postgresql",
            "schema_name": "public",
            "table_name": "employees",
            "user": "postgres",
            "password": "example",
            "host": "localhost",
            "port": 5432,
            "db": "integrate",
        },
    }

    def get_source_connection(self, type: str) -> dict[str, str | int]:
        source_connection = self.__source_connections.get(type, {})
        return source_connection
