# Schema Importer

## Description
    
This API creates and tests source connections, retrieves a table schema, list of available database tables, and table data for a select number of databases (MySQL and PostgreSQL).

## Installation

1. Clone the repository:

    ```shell
    git clone https://github.com/jvsoriano/schema-importer.git
    ```

2. Setup `.env` file:

    ```
    MYSQL_USER=<mysql-user>
    MYSQL_PASSWORD=<mysql-password>
    MYSQL_ROOT_PASSWORD=<mysql-root-password>
    MYSQL_DATABASE=<mysql-database>
    MYSQL_HOST=<mysql-host>
    MYSQL_PORT=<mysql-port>

    POSTGRES_USER=<postgres-user>
    POSTGRES_PASSWORD=<postgres-password>
    POSTGRES_DB=<postgres-db>
    POSTGRES_HOST=<postgres-host>
    POSTGRES_PORT=<postgres-port>
    ```

3. Start docker services:

    ```sh
    docker compose up -d --build
    ```

4. Populate test data:

    ```sh
    docker exec -it schema_importer_api python scripts/populate_mysql_test_data.py

    docker exec -it schema_importer_api python scripts/populate_postgresql_test_data.py
    ```

## MySQL Test Data (Please refer in `.env` file)

- Superuser: `MYSQL_USER`/`MYSQL_PASSWORD`

- Database: `MYSQL_DB`

## PostgreSQL Test Data (Please refer in `.env` file)

- Superuser with database and schema privileges: `POSTGRES_USER`/`POSTGRES_PASSWORD`

- User without database and schema privileges: `johndoe`/`johndoepass`

- User with schema privilege only: `janedoe`/`janedoepass`

- User with database and schema privileges: `jaydoe`/`jaydoepass`    

- Database: `POSTGRES_DB`

- Schema: `public`

## Usage

Access the API Documentation in the browser (`https://localhost:8000/docs`).
