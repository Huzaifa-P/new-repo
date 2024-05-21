import awswrangler as wr
import logging
from psycopg2.sql import SQL, Identifier
import pandas as pd

from utils.get_secret import get_secret
from utils.connect_to_server import connect_to_server
from utils.get_bucket_name import get_bucket_name
from utils.get_bucket_objects import get_bucket_objects
from utils.renaming_folders import rename_word_in_folder_name
from utils.move_s3_objects import move_s3_objects_to_new_folder


def insert_transformed_data_into_warehouse(
        cursor, table_name, column_names, value_list):
    '''
    This function will take 4 arguments:
    cursor - Database Cursor
    table_name - Name of the table you want to insert data into
    colimn_names - List of column names the table has (must be SQL Identifiers)
    value_list - List of values to be inserted (must be SQL Literals)

    It will check if the table exists and then create an SQL statement and execute it to insert the data. Nothing will be returned.

    An exception will be raised if the table does not exist.
    '''

    logger = logging.getLogger('Loading')
    try:
        if table_name not in [
            'dim_date',
            'dim_staff',
            'dim_location',
            'dim_design',
            'dim_currency',
            'dim_payment_type',
            'dim_transaction',
            'dim_counterparty',
            'fact_sales_order',
            'fact_payment',
                'fact_purchase_order']:
            raise Exception('Invalid Table Name')

        placeholders = ", ".join(["%s"] * len(column_names))
        sql_statement = SQL("INSERT INTO {table} ({columns}) VALUES ({values}) ON CONFLICT DO NOTHING;").format(
            table=Identifier(table_name), columns=SQL(',').join(column_names), values=SQL(placeholders))

        cursor.executemany(sql_statement, value_list)

        if table_name == 'fact_payment':
            cursor.execute(
                'DELETE FROM project_team_6.fact_payment a USING project_team_6.fact_payment b WHERE a.payment_record_id > b.payment_record_id AND a.payment_id = b.payment_id;')

        if table_name == 'fact_purchase_order':
            cursor.execute('DELETE FROM project_team_6.fact_purchase_order a USING project_team_6.fact_purchase_order b WHERE a.purchase_record_id > b.purchase_record_id AND a.purchase_order_id = b.purchase_order_id;')

        if table_name == 'fact_sales_order':
            cursor.execute(
                'DELETE FROM project_team_6.fact_sales_order a USING project_team_6.fact_sales_order b WHERE a.sales_record_id > b.sales_record_id AND a.sales_order_id = b.sales_order_id;')

    except Exception as err:
        logger.error(err)
        raise Exception


def format_column_names_for_sql_query(headers):
    '''
    This function will take 1 argument:
    headers - list of table headers

    It will convert and return the list of headers into a list of SQL Identifiers to create a parametized query.

    It will raise an exception if the list of headers contains an item that is not a string.
    '''

    logger = logging.getLogger("Loading")

    try:
        column_names = [Identifier(header) for header in headers]
        return column_names

    except Exception as err:
        logger.error(err)
        raise Exception


def format_values_for_sql_query(values):
    '''
    This function will take 1 argument:
    values - list of values to insert

    It will convert and return the list of values into a list of SQL Literals to create a parametized query.
    If a value of NA or Null is passed, it will be converted to None.

    It will raise an exception if something goes wrong.
    '''

    logger = logging.getLogger("Loading")

    try:
        single_value_list = [None if pd.isna(
            value) else (value) for value in values]
        return single_value_list

    except Exception as err:
        logger.error(err)
        raise Exception


def rename_folder_in_s3_bucket_once_loaded(
        folder_name, object_keys, bucket_name):
    '''

    '''

    logger = logging.getLogger("Loading")
    try:
        if 'processed' not in folder_name:
            raise Exception

        loaded_folder_name = rename_word_in_folder_name(
            folder_name, 'processed', 'loaded')

        processed_files = [file for file in object_keys if 'processed' in file]

        if 'processed' in folder_name:
            loaded_folder_name = rename_word_in_folder_name(
                folder_name, 'processed', 'loaded')

        move_s3_objects_to_new_folder(
            processed_files,
            folder_name,
            loaded_folder_name,
            bucket_name)

    except Exception as err:
        logger.error(err)
        raise Exception


def handler(event, context):

    cursor = None
    db = None

    try:
        logger = logging.getLogger("Loading")
        secrets = get_secret("pg-olap-db")
        db = connect_to_server(secrets)

        db.autocommit = True
        cursor = db.cursor()

        bucket_name = get_bucket_name('nc-project-processed-data')
        object_keys = get_bucket_objects(bucket_name)
        files_to_load = [file for file in object_keys if 'processed' in file]

        if len(files_to_load) > 0:
            for file in files_to_load:

                folder_name, file_path = file.split('/')[0], file.split('/')[1]
                table_name = file_path.split('.')[0]
                data = wr.s3.read_parquet(path=f's3://{bucket_name}/{file}')

                data_list = data.values.tolist()
                headers = data.columns.tolist()

                for table_to_insert in ['dim_', 'fact_']:
                    if table_name.startswith(table_to_insert):
                        logger.info(f'Inserting into {table_name}')

                        column_names = format_column_names_for_sql_query(
                            headers)
                        value_lists = [
                            format_values_for_sql_query(values) for values in data_list]

                        insert_transformed_data_into_warehouse(
                            cursor, table_name, column_names, value_lists)

            rename_folder_in_s3_bucket_once_loaded(
                folder_name, files_to_load, bucket_name)

        else:
            logger.info('No new data to load.')
            return 'No new data to load.'

    except Exception as err:
        logger.error(err)
        raise Exception

    finally:
        if cursor:
            cursor.close()
            logger.info('Curser Closed')
        if db:
            db.close()
            logger.info('DB Connection Closed')


# handler('hello', 'world')
