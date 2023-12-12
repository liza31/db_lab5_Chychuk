import psycopg2
import psycopg2.extras
import psycopg2.sql as sql
import psycopg2.extensions

import csv


# PostgreSQL connection config

DB_HOST = 'localhost'   # PostgreSQL Database server host name
DB_PORT = '5432'        # PostgreSQL Database server port number

DB_NAME = 'db_lab'    # PostgreSQL Database name

DB_USER = 'postgres'          # PostgreSQL Database user name
DB_PASS = '1111'       # PostgreSQL Database user password


# General export config

# Tables to be exported
EXPORT_TABLES = [
    'attacks',
    'attack_groups',
    'potential_targets', 'group_targets',
    'launch_places', 'group_launch_places',
    'missiles', 'group_missiles'
]


# CSV export config

# Export CSV files paths by corresponding table names
EXPORT_CSV_PATHS = { table: f"export_{table}.csv" for table in EXPORT_TABLES }  
# Export CSV files encoding
EXPORT_CSV_ENCODING = 'UTF-8'

# Export CSV files CSV options
EXPORT_CSV_OPTS = dict(delimiter=',')


# Database table export to CSV function and corresponding SQL queries

SQL_TABLE_SELECT = sql.SQL("SELECT * FROM {table}")


def export_table_to_csv(db_conn: psycopg2.extensions.connection,
                        out_file,
                        table: str,
                        **out_csv_opts):
    """
    Performs export of database `table` table, obtained through the `db_conn` `psycopg2` connection
    to the `out_file` file in the CSV format using other keyword arguments as CSV formatting options.

    :param db_conn: `psycopg2` database connection
    :param out_file: object, supporting :class:`str` `write`, to write exported data
    :param table: name of table to export
    :param out_csv_opts: CSV options
    """

    out_writer = csv.writer(out_file, **out_csv_opts)

    with db_conn:

        cur: psycopg2.extensions.cursor

        with db_conn.cursor() as cur:

            cur.execute(SQL_TABLE_SELECT.format(table=sql.Identifier(table)))

            out_writer.writerow(col[0] for col in cur.description)

            for row in cur:
                out_writer.writerow(str(col) for col in row)


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

    # For each exporting table open export file & execute export
    try:
        for _table in EXPORT_TABLES:
            with open(EXPORT_CSV_PATHS[_table], mode='w', encoding=EXPORT_CSV_ENCODING, newline='') as _out_file:
                export_table_to_csv(_db_conn, _out_file, _table, **EXPORT_CSV_OPTS)

    # Properly close the dataset file and database connection
    finally:
        _db_conn.close()
