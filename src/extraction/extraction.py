from psycopg2 import sql
from datetime import datetime, timezone, timedelta
import pandas as pd
import awswrangler as wr
import logging
from utils.get_bucket_name import get_bucket_name
from utils.get_bucket_objects import get_bucket_objects
from utils.get_secret import get_secret
from utils.connect_to_server import connect_to_server


def set_start_time_minus_minutes(minutes):
    """
    Subtract the given number of minutes from the current time.

    Parameters:
    - minutes: The number of minutes to subtract from the current time.

    Returns:
    - A datetime object representing the current time minus the given number of minutes.
    """

    return datetime.now() - timedelta(minutes=minutes)


def create_folder_prefix(name):
    """
    Create a folder prefix string by appending the current UTC timestamp to the given name.

    Parameters:
    - name: The base string to which the timestamp will be appended.

    Returns:
    - A string in the format 'name_timestamp'.
    """

    current_date_utc = datetime.now(timezone.utc).timestamp()
    return f'{name}{current_date_utc}'


def get_column_names(cursor):
    """
    Get the names of the columns from the result of a SQL query.

    Parameters:
    - cursor: A psycopg2 cursor object that has been used to execute a SQL query.

    Returns:
    - A list of strings representing the names of the columns.
    """

    return [desc[0] for desc in cursor.description]


def get_all_table_rows(cursor, table_name):
    """
    Get all rows from a table in a database.

    Parameters:
    - cursor: A psycopg2 cursor object.
    - table_name: The name of the table from which to retrieve the rows.

    Returns:
    - A list of tuples representing the rows of the table.
    """

    sql_statement = sql.SQL(
        "SELECT * FROM {table};").format(table=sql.Identifier(table_name))
    cursor.execute(
        sql_statement)
    return cursor.fetchall()


def get_latest_table_rows(cursor, table_name, start_time):
    """
    Get the rows from a table in a database that were last updated after a certain time.

    Parameters:
    - cursor: A psycopg2 cursor object.
    - table_name: The name of the table from which to retrieve the rows.
    - start_time: The time after which the rows must have been last updated.

    Returns:
    - A list of tuples representing the rows of the table that meet the condition.
    """

    sql_statement = sql.SQL("SELECT * FROM {table} WHERE {table}.last_updated > {time};").format(
        table=sql.Identifier(table_name), time=sql.Literal(start_time))
    cursor.execute(
        sql_statement)
    return cursor.fetchall()


lst_table_names = ['transaction',
                   'payment_type',
                   'purchase_order',
                   'payment',
                   'address',
                   'sales_order',
                   'staff',
                   'design',
                   'department',
                   'currency',
                   'counterparty']


def handler(event, context):
    """
    Main function for extracting data from a database and saving it to an S3 bucket.

    Parameters:
    - event:  Not used in this function.
    - context:  Not used in this function.

    This function does not return anything.
    """
    logger = logging.getLogger("Extraction")

    s3_ingestion_bucket_name = get_bucket_name("nc-project-ingestion-zone-")
    datetime_start = set_start_time_minus_minutes(15)
    folder_prefix = create_folder_prefix("totesys_extraction_data_")
    cursor = None
    conn = None
    try:

        credentials = get_secret("pg-oltp-db")

        conn = connect_to_server(credentials)
        cursor = conn.cursor()

        objects = get_bucket_objects(s3_ingestion_bucket_name)
        for table in lst_table_names:
            rows = get_all_table_rows(
                cursor, table) if objects is None else get_latest_table_rows(
                cursor, table, datetime_start)
            if len(rows) > 0:
                column_names = get_column_names(cursor)
                df = pd.DataFrame(rows, columns=column_names)

                wr.s3.to_parquet(
                    df=df, path=f"s3://{s3_ingestion_bucket_name}/{folder_prefix}/{table}.parquet")

            else:
                logger.info(
                    f"No new data within {table} in from {datetime_start} to {datetime.now()}")

    except Exception as err:
        logger.error(err)
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()


if __name__ == "__main__":
    handler("test", "test")
