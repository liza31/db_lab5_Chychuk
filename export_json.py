from collections.abc import Iterable
from typing import IO

import psycopg2
import psycopg2.extras
import psycopg2.sql as sql
import psycopg2.extensions

import json


# PostgreSQL connection config

DB_HOST = 'localhost'   # PostgreSQL Database server host name
DB_PORT = '5432'        # PostgreSQL Database server port number

DB_NAME = 'db_lab'    # PostgreSQL Database name

DB_USER = 'postgres'          # PostgreSQL Database user name
DB_PASS = '1111'          # PostgreSQL Database user password


# General export config

# Tables to be exported
EXPORT_TABLES = [
    'attacks',
    'attack_groups',
    'potential_targets', 'group_targets',
    'launch_places', 'group_launch_places',
    'missiles', 'group_missiles'
]


# JSON export config

EXPORT_JSON_PATH = "export.json"        # Export JSON file path
EXPORT_JSON_ENCODING = 'UTF-8'          # Export JSON file encoding

EXPORT_JSON_OPTS = dict(indent=4)       # Export JSON file JSON options


# Database export to JSON function and corresponding SQL queries

SQL_TABLE_SELECT = sql.SQL("SELECT * FROM {table}")


def export_to_json(db_conn: psycopg2.extensions.connection,
                   out_file: IO[str],
                   tables: Iterable[str],
                   **out_json_opts):
    """
    Performs export of database tables, specified by `tables` :class:`Iterable` of corresponding tables names,
    obtained through the `db_conn` `psycopg2` connection to the `out_file` file in the JSON format
    using other keyword arguments as JSON formatting options.

    :param db_conn: `psycopg2` database connection
    :param out_file: :class:`IO[str]` object to write exported data
    :param tables: :class:`Iterable` of :class:`str` names of tables to export
    :param out_csv_opts: JSON options
    """

    out_data_tables = {}

    # Collec all requested tables data
    for table in tables:

        with db_conn:

            cur: psycopg2.extensions.cursor

            with db_conn.cursor() as cur:

                cur.execute(SQL_TABLE_SELECT.format(table=sql.Identifier(table)))

                cols = [col[0] for col in cur.description]
                rows = [dict(zip(cols, map(str, row))) for row in cur]

                out_data_tables[table] = {'columns': cols, 'data': rows}

    # Dump JSON data to output file
    json.dump({'tables': out_data_tables}, out_file, **out_json_opts)


# Script entry point

if __name__ == '__main__':

    # Open database connection
    _db_conn: psycopg2.extensions.connection = psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASS
    )

    # Open export file & selected tables execute export
    try:
        with open(EXPORT_JSON_PATH, mode='w', encoding=EXPORT_JSON_ENCODING) as _out_file:
                export_to_json(_db_conn, _out_file, EXPORT_TABLES, **EXPORT_JSON_OPTS)

    # Properly close the dataset file and database connection
    finally:
        _db_conn.close()
