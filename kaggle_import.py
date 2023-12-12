from collections.abc import Callable, Iterable
from typing import Mapping, Any

import psycopg2
import psycopg2.sql as sql
import psycopg2.extensions

import csv

from datetime import datetime

from functools import wraps, lru_cache
from itertools import islice, chain
from inspect import cleandoc


# PostgreSQL connection config

DB_HOST = 'localhost'   # PostgreSQL Database server host name
DB_PORT = '5432'        # PostgreSQL Database server port number

DB_NAME = 'db_lab'    # PostgreSQL Database name

DB_USER = 'postgres'          # PostgreSQL Database user name
DB_PASS = '1111'          # PostgreSQL Database user password


# Kaggle dataset import config

IMPORT_DATASET_CSV_PATH = "missile_attacks_daily.csv"       # Source Kaggle dataset csv file path
IMPORT_DATASET_CSV_OPTS = dict(delimiter=',')               # Source Kaggle dataset csv file parse options

IMPORT_BLOCK_SIZE = 100                                     # Number of dataset rows, importing through one commit

IMPORT_SOURCES_URL_PREFIX = "https://www.facebook.com/"     # Prefix to be added to info source URLs while importing


# Auxiliary script functions

def db_atomic_search(table: str, pk_col: str, **kw_cols: str):
    """
    Decorator, creates an atomic database searcher function with the signature
    of given function (only signature will be utilized, any internal logic will be discarded).

        The resulting searcher will perform search across `table` table to find PK (column `pk_col`) of the row,
        which contains the same values, as passed to the search by keyword arguments (to map arguments keywords
        onto columns names, you should pass those names by corresponding kwargs into decorator - see `kw_cols`).

    :param table: database table name
    :param pk_col: name of PK column of the `table`
    :param kw_cols: columns names, passed with corresponding kwargs keywords
    """

    sql_get_pk = sql.SQL("SELECT {pk_col} FROM {table} WHERE {conds} LIMIT 1").format(
        pk_col=sql.Identifier(pk_col),
        table=sql.Identifier(table),
        conds=sql.SQL(" AND ").join(
            sql.SQL("{col_name} = {col_val}").format(col_name=sql.Identifier(col_name), col_val=sql.Placeholder(col_kw))
            for (col_kw, col_name) in kw_cols.items()
        ))

    sql_insert = sql.SQL("INSERT INTO {table} ({val_cols}) VALUES ({vals}) RETURNING {pk_col}").format(
        table=sql.Identifier(table),
        val_cols=sql.SQL(", ").join(map(sql.Identifier, kw_cols.values())),
        vals=sql.SQL(", ").join(map(sql.Placeholder, kw_cols.keys())),
        pk_col=sql.Identifier(pk_col))

    def decorator(func: Callable):

        @wraps(func)
        def db_searcher(conn: psycopg2.extensions.connection, **vals):

            cur: psycopg2.extensions.cursor

            with (conn.cursor()) as cur:

                cur.execute(sql_get_pk, vals)
                pk_row = cur.fetchone()

                if pk_row is None:
                    cur.execute(sql_insert, vals)
                    pk_row = cur.fetchone()

                return pk_row[0]

        return db_searcher

    return decorator


# Auxiliary database handling functions

# noinspection PyUnusedLocal
@lru_cache(maxsize=1)
@db_atomic_search('attacks', 'attack_id',
                  start_datetime='start_datetime',
                  end_datetime='end_datetime',
                  info_source="info_source")
def get_attack_id(conn: psycopg2.extensions.connection,
                  start_datetime: datetime,
                  end_datetime: datetime,
                  info_source: str) -> int:
    """
    Performs a search across database (using passed `conn` :class:`psycopg2.extensions.connection`)
    table `attacks` for PK of the row, containing the same values as passed (all kwargs after `conn`).

    If the corresponding row is not found, will insert a new row with the given values.

    :return: PK (`attack_id` :class:`int`) of the `attacks` table row, which contains given values
    """


# noinspection PyUnusedLocal
@lru_cache(maxsize=500)
@db_atomic_search('potential_targets', 'target_id',
                  general_name='general_name')
def get_target_id(conn: psycopg2.extensions.connection,
                  general_name: str) -> int:
    """
    Performs a search across database (using passed `conn` :class:`psycopg2.extensions.connection`)
    table `potential_targets` for PK of the row, containing the same values as passed (all kwargs after `conn`).

    If the corresponding row is not found, will insert a new row with the given values.

    :return: PK (`target_id` :class:`int`) of the `potential_targets` table row, which contains given values
    """


# noinspection PyUnusedLocal
@lru_cache(maxsize=500)
@db_atomic_search('launch_places', 'place_id',
                  general_name='general_name')
def get_place_id(conn: psycopg2.extensions.connection,
                 general_name: str):
    """
    Performs a search across database (using passed `conn` :class:`psycopg2.extensions.connection`)
    table `launch_places` for PK of the row, containing the same values as passed (all kwargs after `conn`).

    If the corresponding row is not found, will insert a new row with the given values.

    :return: PK (`place_id` :class:`int`) of the `launch_places` table row, which contains given values
    """


# noinspection PyUnusedLocal
@lru_cache(maxsize=500)
@db_atomic_search('missiles', 'missile_id',
                  model_name='model_name')
def get_missile_id(conn: psycopg2.extensions.connection,
                   model_name: str):
    """
    Performs a search across database (using passed `conn` :class:`psycopg2.extensions.connection`)
    table `missiles` for PK of the row, containing the same values as passed (all kwargs after `conn`).

    If the corresponding row is not found, will insert a new row with the given values.

    :return: PK (`missile_id` :class:`int`) of the `missiles` table row, which contains given values
    """


# Auxiliary data handling functions

def get_datetime(raw_str: str) -> datetime:
    """
    Handles dataset raw datetime strings into :class:`datetime` objects.

    :param raw_str: raw datetime string read from dataset
    :return: corresponding :class:`datetime` object
    """

    return datetime.strptime(raw_str, '%Y-%m-%d' + (' %H:%M' if ':' in raw_str else ''))


# Dataset import function and corresponding SQL queries

SQL_GROUPS_INSERT = cleandoc(
    """
    INSERT INTO attack_groups (attack_id, units_launched, units_destroyed) 
    VALUES (%(attack_id)s, %(units_launched)s, %(units_destroyed)s)
    RETURNING group_id
    """
)

SQL_GROUP_TARGETS_INSERT = cleandoc(
    """
    INSERT INTO group_targets (group_id, target_id) 
    VALUES (%(group_id)s, %(target_id)s)
    """
)

SQL_GROUP_PLACES_INSERT = cleandoc(
    """
    INSERT INTO group_launch_places (group_id, place_id) 
    VALUES (%(group_id)s, %(place_id)s)
    """
)

SQL_GROUP_MISSILES_INSERT = cleandoc(
    """
    INSERT INTO group_missiles (group_id, missile_id) 
    VALUES (%(group_id)s, %(missile_id)s)
    """
)


def import_dataset(db_conn: psycopg2.extensions.connection,
                   data_rows: Iterable[Mapping[str, Any]],
                   *,
                   block_size: int = 100,
                   src_url_prefix=''):
    """
    Provides importing data from original Kaggle dataset (represented by `data_rows` rows mappings iterable)
    into database through the `db_conn` `psycopg2` connection.

    :param db_conn: `psycopg2` database connection
    :param data_rows: iterable of columnName-value :class:`Mapping` objects by rows (primarily - :class:`csv.DictReader`)
    :param block_size: target (max) number of rows to be imported through a single commit
    :param src_url_prefix: prefix to be added to info source URLs (empty by default)
    """

    i_block = 0

    data_iter = iter(data_rows)

    while True:

        data_block = islice(data_iter, block_size)
        data_block_row_0 = next(data_block, None)

        if data_block_row_0 is None:
            break

        n_rows = 0

        with db_conn:

            for data_row in chain((data_block_row_0,), data_block):

                attack_id = get_attack_id(
                    db_conn,
                    start_datetime=get_datetime(data_row['time_start']),
                    end_datetime=get_datetime(data_row['time_end']),
                    info_source=src_url_prefix + data_row['source'])

                db_cur: psycopg2.extensions.cursor

                with db_conn.cursor() as db_cur:

                    db_cur.execute(SQL_GROUPS_INSERT, dict(
                        attack_id=attack_id,
                        units_launched=data_row['launched'],
                        units_destroyed=data_row['destroyed']))

                    group_id = db_cur.fetchone()[0]

                    for place_name in data_row['launch_place'].split(' and '):
                        db_cur.execute(SQL_GROUP_PLACES_INSERT, dict(
                            group_id=group_id,
                            place_id=get_place_id(db_conn, general_name=place_name)))

                    for target_name in data_row['target'].split(' and '):
                        db_cur.execute(SQL_GROUP_TARGETS_INSERT, dict(
                            group_id=group_id,
                            target_id=get_target_id(db_conn, general_name=target_name)))

                    for missile_model in data_row['model'].split(' and '):
                        db_cur.execute(SQL_GROUP_MISSILES_INSERT, dict(
                            group_id=group_id,
                            missile_id=get_missile_id(db_conn, model_name=missile_model)))

                n_rows += 1

        print(f"Block #{i_block} of {n_rows} rows just inserted")

        i_block += 1


# Script entry point

if __name__ == '__main__':

    # Open dataset file before establishing database connection


    # Open database connection
    _db_conn: psycopg2.extensions.connection = psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASS
    )

    # Open dataset file & execute dataset import
    try:
        with open(IMPORT_DATASET_CSV_PATH) as _data_file:
            import_dataset(db_conn=_db_conn, data_rows=csv.DictReader(_data_file, **IMPORT_DATASET_CSV_OPTS),
                           block_size=IMPORT_BLOCK_SIZE, src_url_prefix=IMPORT_SOURCES_URL_PREFIX)

    # Properly close the dataset file and database connection
    finally:
        _db_conn.close()
