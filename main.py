from collections.abc import Callable

import psycopg2
import psycopg2.extras
import psycopg2.sql as sql
import psycopg2.extensions

import numpy as np

import matplotlib.pyplot as plt
import matplotlib.ticker as ticker

from inspect import cleandoc


# PostgreSQL connection config

DB_HOST = 'localhost'   # PostgreSQL Database server host name
DB_PORT = '5432'        # PostgreSQL Database server port number

DB_NAME = 'db_lab'    # PostgreSQL Database name

DB_USER = 'postgres'          # PostgreSQL Database user name
DB_PASS = '1111'          # PostgreSQL Database user password


# Database queries 1-3 according to the laboratory task

# -- Query 1: info string, corresponding view name & sql query

QUERY_1_INFO = cleandoc(
    '''
    The number of attacks in which "Kalibr" cruise missiles were used by month
    Кількість атак з використанням крилатих ракет "Калібр" за місяцями
    '''
)

QUERY_1_VIEW = sql.Identifier("kalibr_attacks_number_by_months")

QUERY_1_SQL = sql.SQL(cleandoc(
    '''
    SELECT
        date_part('year', attacks.start_datetime)::integer AS year,
        date_part('month', attacks.start_datetime)::integer AS month,
        count(DISTINCT attacks.attack_id) AS attacks_count
    FROM
        attacks
        JOIN attack_groups USING (attack_id)
        JOIN group_missiles USING (group_id)
    WHERE
        group_missiles.missile_id = (SELECT missile_id FROM missiles WHERE model_name = 'Kalibr')
    GROUP BY
        year, month
    ORDER BY
        year, month
    '''
))

# -- Query 2: info string, corresponding view name & sql query

QUERY_2_INFO = cleandoc(
    '''
    Distribution of the total number of attacks by targets
    Розподіл загальної кількості атак за цілями
    '''
)

QUERY_2_VIEW = sql.Identifier("attacks_number_by_targets")

QUERY_2_SQL = sql.SQL(cleandoc(
    '''
    SELECT
        potential_targets.general_name as target,
        count(DISTINCT attack_id) as attacks_count
    FROM
        attacks
        JOIN attack_groups USING (attack_id)
        JOIN group_targets USING (group_id)
        JOIN potential_targets USING (target_id)
    GROUP BY
        target
    ORDER BY
        attacks_count desc
    '''
))

# -- Query 3: info string, corresponding view name & sql query

QUERY_3_INFO = cleandoc(
    '''
    Mass (total number of used missiles/drones) of attacks in which "Shahed-136/131" strike drones were used by month
    Масовість атак з використанням баражуючих боєприпасів "Shahed-136/131" за місяцями
    '''
)

QUERY_3_VIEW = sql.Identifier("shahed_attacks_mass_by_months")

QUERY_3_SQL = sql.SQL(cleandoc(
    '''
    SELECT
        date_part('year', attacks.start_datetime)::integer AS year,
        date_part('month', attacks.start_datetime)::integer AS month,
        sum(attack_groups.units_launched) AS missiles_count
    FROM
        attacks
        JOIN attack_groups USING (attack_id)
        JOIN group_missiles USING (group_id)
    WHERE
        group_missiles.missile_id = (SELECT missile_id FROM missiles WHERE model_name = 'Shahed-136/131')
    GROUP BY
        year, month
    ORDER BY
        year, month
    '''
))


# SQL queries patterns for views maintenance

SQL_VIEW_CREATE = sql.SQL("CREATE VIEW {view} AS {query}")

SQL_VIEW_DELETE = sql.SQL("DROP VIEW IF EXISTS {view}")

SQL_VIEW_SELECT = sql.SQL("SELECT * FROM {view}")


# Plotting function for each query

def plot_query_1_view(conn: psycopg2.extensions.connection, ax: plt.Axes):
    """
    Obtain data from Query 1 view through the `conn` `psycopg2` connection
    and plot results on the `ax` `matplotlib` `Axes`.
    """

    # Query data from database

    with conn:

        cur: psycopg2.extras.RealDictCursor

        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:

            cur.execute(SQL_VIEW_SELECT.format(view=QUERY_1_VIEW))
            data = cur.fetchall()

    # Prepare data for further plotting

    height = np.fromiter((row['attacks_count'] for row in data), int)
    x_range = np.arange(len(data))

    x_labels = np.fromiter((f"{row['year']:04d}-{row['month']:02d}" for row in data), 'datetime64[M]')

    # Plot data on the given `Axes`

    ax.set_title(QUERY_1_INFO)

    ax.set_xlabel('Year-Month')
    ax.set_ylabel('Attacks count')

    ax.yaxis.set_major_locator(ticker.MaxNLocator(integer=True))

    ax.bar(x=x_range, height=height)
    ax.grid(axis="y")

    ax.xaxis.set_ticks(x_range)
    ax.xaxis.set_ticklabels(x_labels)


def plot_query_2_view(conn: psycopg2.extensions.connection, ax: plt.Axes):
    """
    Obtain data from Query 2 view through the `conn` `psycopg2` connection
    and plot results on the `ax` `matplotlib` `Axes`.
    """

    # Query data from database

    with conn:

        cur: psycopg2.extras.RealDictCursor

        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:

            cur.execute(SQL_VIEW_SELECT.format(view=QUERY_2_VIEW))
            data = cur.fetchall()

    # Prepare data for further plotting

    vals = np.fromiter((row['attacks_count'] for row in data), int)

    vals_sum = vals.sum()
    vals_to_str: Callable[[float], str] = lambda pct: f"{pct:.1f}%\n({int(np.round(pct / 100. * vals_sum)):d})"

    labels = np.fromiter((row['target'] for row in data), 'U256')

    # Plot data on the given `Axes`

    ax.set_title(QUERY_2_INFO)

    ax.pie(
        x=vals,
        labels=labels,
        autopct=vals_to_str,
        explode=np.repeat(0.05, len(data)),
        shadow=True
    )


def plot_query_3_view(conn: psycopg2.extensions.connection, ax: plt.Axes):
    """
    Obtain data from Query 3 view through the `conn` `psycopg2` connection
    and plot results on the `ax` `matplotlib` `Axes`.
    """

    # Query data from database

    with conn:

        cur: psycopg2.extras.RealDictCursor

        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:

            cur.execute(SQL_VIEW_SELECT.format(view=QUERY_3_VIEW))
            data = cur.fetchall()

    # Prepare data for further plotting

    y_vals = np.fromiter((row['missiles_count'] for row in data), int)
    x_range = np.arange(len(data))

    x_labels = np.fromiter((f"{row['year']:04d}-{row['month']:02d}" for row in data), 'datetime64[M]')

    # Plot data on the given `Axes`

    ax.set_title(QUERY_3_INFO)

    ax.set_xlabel('Year-Month')
    ax.set_ylabel('Used missiles count')

    ax.yaxis.set_major_locator(ticker.MaxNLocator(integer=True))

    ax.plot(x_range, y_vals, marker='v')
    ax.grid(axis="both")

    ax.xaxis.set_ticks(x_range)
    ax.xaxis.set_ticklabels(x_labels)

    for x, val in zip(x_range, y_vals):
        ax.annotate(val, (x, val))


# Queries views visualization function

def plot_views(conn: psycopg2.extensions.connection):
    """
    Handles plotting Queries 1-3 views visualizations, obtaining data through `conn` `psycopg2` connection
    """

    # Execute Query 1 & plot results
    fig_query_1, ax_query_1 = plt.subplots()
    plot_query_1_view(conn=conn, ax=ax_query_1)

    # Execute Query 2 & plot results
    fig_query_2, ax_query_2 = plt.subplots()
    plot_query_2_view(conn=conn, ax=ax_query_2)

    # Execute Query 3 & plot results
    fig_query_3, ax_query_3 = plt.subplots()
    plot_query_3_view(conn=conn, ax=ax_query_3)


# Script `main()` function & entry point

def main(conn: psycopg2.extensions.connection):

    queries = [QUERY_1_SQL, QUERY_2_SQL, QUERY_3_SQL]
    views = [QUERY_1_VIEW, QUERY_2_VIEW, QUERY_3_VIEW]

    # Recreate all views if already exists
    for view, query in zip(views, queries):

        with conn:
            with conn.cursor() as cur:
                cur.execute(SQL_VIEW_DELETE.format(view=view))
                cur.execute(SQL_VIEW_CREATE.format(view=view, query=query))

    # Plot all views
    plot_views(conn)

    # Show plots
    plt.show()


if __name__ == '__main__':

    # Open database connection
    _conn: psycopg2.extensions.connection = psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASS
    )

    # Execute script logic
    try:
        main(conn=_conn)

    # Properly close the database connection
    finally:
        _conn.close()
