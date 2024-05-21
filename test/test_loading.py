from src.loading.loading import insert_transformed_data_into_warehouse, format_column_names_for_sql_query, format_values_for_sql_query, rename_folder_in_s3_bucket_once_loaded, handler
from data.test_loading_data import single_processed_data_object_keys, multiple_processed_data_object_keys, single_loaded_data_object_keys, multiple_loaded_data_object_keys
from data.test_df_data import currency_data, design_data, paymenmt_type_data
from utils.get_bucket_objects import get_bucket_objects

from moto import mock_s3
import boto3
from unittest.mock import MagicMock, Mock, patch
from psycopg2.sql import SQL, Identifier, Literal
import pandas as pd
import numpy as np
from pytest import raises, mark
from argparse import Namespace


class TestSQLQueryFormattingColumnNameFunction:
    def test_function_returns_parametised_query_for_sql_statement_with_one_value(
            self):

        input = ['currency_id']
        excepted = [Identifier('currency_id')]
        result = format_column_names_for_sql_query(input)

        assert result == excepted

    def test_function_returns_parametised_query_for_sql_statement_with_multiple_values(
            self):

        input = [
            'transaction_id',
            'transaction_type',
            'sales_order_id',
            'purchase_order_id']

        excepted = [
            Identifier('transaction_id'),
            Identifier('transaction_type'),
            Identifier('sales_order_id'),
            Identifier('purchase_order_id')]

        result = format_column_names_for_sql_query(input)

        assert result == excepted

    def test_function_raises_an_exception_if_passed_a_column_name_that_is_not_a_string(
            self):

        input_with_number = ['transaction_id', 1]
        input_with_list = ['transaction_id', ['second_columns']]
        input_with_dictionary = ['transaction_id', {'key': 'value'}]

        with raises(Exception):
            format_column_names_for_sql_query(input_with_number)

        with raises(Exception):
            format_column_names_for_sql_query(input_with_list)

        with raises(Exception):
            format_column_names_for_sql_query(input_with_dictionary)


class TestSQLQueryFormattingValuesFunction:
    def test_function_returns_parametised_query_for_sql_statement_with_one_value(
            self):

        input = ['Hasan']
        excepted = ['Hasan']
        result = format_values_for_sql_query(input)

        assert result == excepted

    def test_function_returns_parametised_query_for_sql_statement_with_multiple_values(
            self):

        input = ['Hello', 55, '54 West Lane', 'Manchester']
        excepted = ['Hello', 55, '54 West Lane', 'Manchester']

        result = format_values_for_sql_query(input)

        assert result == excepted

    def test_function_returns_None_if_passed_an_na_value(
            self):

        input = ['Hello', 55, '54 West Lane', np.NaN]
        excepted = ['Hello', 55, '54 West Lane', None]

        result = format_values_for_sql_query(input)

        assert result == excepted


@mock_s3
class TestRenamingS3FolderOnceLoaded:
    def test_folder_in_s3_is_renamed_from_transformed_to_loaded_when_passed_one_object_not_loaded(
            self):

        s3 = boto3.client('s3', region_name='eu-west-2')
        folder_name = 'totesys_processed_data_1690986094.604827'
        object_keys = single_processed_data_object_keys
        bucket_name = 'nc-project-processed-data-20230802141305364600000002'

        s3.create_bucket(
            Bucket=bucket_name,
            CreateBucketConfiguration={'LocationConstraint': 'eu-west-2'}
        )

        df = pd.DataFrame(data=currency_data)
        transformed_currency = df.to_parquet()

        s3.put_object(
            Bucket=bucket_name,
            Key=f"{folder_name}/dim_currency.parquet",
            Body=transformed_currency
        )

        my_original_folder = get_bucket_objects(bucket_name)
        assert my_original_folder == single_processed_data_object_keys

        rename_folder_in_s3_bucket_once_loaded(
            folder_name, object_keys, bucket_name)

        my_renamed_folder = get_bucket_objects(bucket_name)
        assert my_renamed_folder == single_loaded_data_object_keys

    def test_folder_in_s3_is_renamed_from_transformed_to_loaded_when_passed_multiple_objects_not_loaded(
            self):

        s3 = boto3.client('s3', region_name='eu-west-2')
        folder_name = 'totesys_processed_data_1690986094.604827'
        object_keys = multiple_processed_data_object_keys
        bucket_name = 'nc-project-processed-data-20230802141305364600000002'

        s3.create_bucket(
            Bucket=bucket_name,
            CreateBucketConfiguration={'LocationConstraint': 'eu-west-2'}
        )

        currency_df = pd.DataFrame(data=currency_data)
        transformed_currency = currency_df.to_parquet()

        s3.put_object(
            Bucket=bucket_name,
            Key=f"{folder_name}/dim_currency.parquet",
            Body=transformed_currency
        )

        payment_type_df = pd.DataFrame(data=paymenmt_type_data)
        transformed_payment_type_df = payment_type_df.to_parquet()

        s3.put_object(
            Bucket=bucket_name,
            Key=f"{folder_name}/dim_payment_type.parquet",
            Body=transformed_payment_type_df
        )

        design_df = pd.DataFrame(data=design_data)
        transformed_design_df = design_df.to_parquet()

        s3.put_object(
            Bucket=bucket_name,
            Key=f"{folder_name}/dim_design.parquet",
            Body=transformed_design_df
        )

        rename_folder_in_s3_bucket_once_loaded(
            folder_name, object_keys, bucket_name)

        my_renamed_folders = get_bucket_objects(bucket_name)
        assert my_renamed_folders == multiple_loaded_data_object_keys

    def test_folder_in_s3_is_renamed_from_transformed_to_loaded_when_passed_multiple_objects_including_fact_files(
            self):

        s3 = boto3.client('s3', region_name='eu-west-2')
        folder_name = 'totesys_processed_data_1690986094.604827'
        object_keys = [
            'totesys_processed_data_1690986094.604827/fact_currency.parquet',
            'totesys_processed_data_1690986094.604827/fact_payment_type.parquet',
            'totesys_processed_data_1690986094.604827/fact_design.parquet']
        bucket_name = 'nc-project-processed-data-20230802141305364600000002'

        s3.create_bucket(
            Bucket=bucket_name,
            CreateBucketConfiguration={'LocationConstraint': 'eu-west-2'}
        )

        currency_df = pd.DataFrame(data=currency_data)
        transformed_currency = currency_df.to_parquet()

        s3.put_object(
            Bucket=bucket_name,
            Key=f"{folder_name}/fact_currency.parquet",
            Body=transformed_currency
        )

        payment_type_df = pd.DataFrame(data=paymenmt_type_data)
        transformed_payment_type_df = payment_type_df.to_parquet()

        s3.put_object(
            Bucket=bucket_name,
            Key=f"{folder_name}/fact_payment_type.parquet",
            Body=transformed_payment_type_df
        )

        design_df = pd.DataFrame(data=design_data)
        transformed_design_df = design_df.to_parquet()

        s3.put_object(
            Bucket=bucket_name,
            Key=f"{folder_name}/fact_design.parquet",
            Body=transformed_design_df
        )

        rename_folder_in_s3_bucket_once_loaded(
            folder_name, object_keys, bucket_name)

        my_renamed_folders = get_bucket_objects(bucket_name)
        assert my_renamed_folders == [
            'totesys_loaded_data_1690986094.604827/fact_currency.parquet',
            'totesys_loaded_data_1690986094.604827/fact_design.parquet',
            'totesys_loaded_data_1690986094.604827/fact_payment_type.parquet']

    def test_function_raises_an_error_if_processed_not_in_folder_name(self):

        s3 = boto3.client('s3', region_name='eu-west-2')
        folder_name = 'totesys_transformed_data_1690986094.604827'
        object_keys = single_processed_data_object_keys
        bucket_name = 'nc-project-processed-data-20230802141305364600000002'

        s3.create_bucket(
            Bucket=bucket_name,
            CreateBucketConfiguration={'LocationConstraint': 'eu-west-2'}
        )

        df = pd.DataFrame(data=currency_data)
        transformed_currency = df.to_parquet()

        s3.put_object(
            Bucket=bucket_name,
            Key=f"{folder_name}/dim_currency.parquet",
            Body=transformed_currency
        )

        with raises(Exception):
            rename_folder_in_s3_bucket_once_loaded(
                folder_name, object_keys, bucket_name)


class TestLoadingFunction:
    def test_inserts_data_into_warehouse(self):
        mock_cursor = Mock()

        table_name = 'dim_transaction'
        column_names = [
            Identifier('transaction_id'),
            Identifier('transaction_type'),
            Identifier('sales_order_id'),
            Identifier('purchase_order_id')]
        value_list = [1, 'Card', 2, 3]

        insert_transformed_data_into_warehouse(
            mock_cursor, table_name, column_names, value_list)

        placeholders = ", ".join(["%s"] * len(column_names))
        sql_statement = SQL("INSERT INTO {table} ({columns}) VALUES ({values}) ON CONFLICT DO NOTHING;").format(
            table=Identifier(table_name), columns=SQL(',').join(column_names), values=SQL(placeholders))

        mock_cursor.executemany.assert_called_once_with(
            sql_statement, value_list)

    def test_function_raises_an_exception_if_invalid_table_name_is_passed(
            self):
        mock_cursor = Mock()

        table_name = 'dim_dummy_table'
        column_names = [
            Identifier('transaction_id'),
            Identifier('transaction_type'),
            Identifier('sales_order_id'),
            Identifier('purchase_order_id')]
        single_value_list = [
            Literal(1),
            Literal('Card'),
            Literal(2),
            Literal(3)]

        with raises(Exception):
            insert_transformed_data_into_warehouse(
                mock_cursor, table_name, column_names, single_value_list)

    @mark.skip
    def test_function_raises_an_type_error_exception_if_column_names_are_invalid_data_type(
            self):
        mock_cursor = Mock()

        table_name = 'dim_dummy_table'
        column_names = [
            Identifier('transaction_id'),
            Identifier(3),
            Identifier('sales_order_id'),
            Identifier('purchase_order_id')]
        single_value_list = [
            Literal(1),
            Literal('Card'),
            Literal(2),
            Literal(3)]

        with raises(TypeError):
            insert_transformed_data_into_warehouse(
                mock_cursor, table_name, column_names, single_value_list)


@mock_s3
class TestHandlerFunction:

    @patch('src.loading.loading.get_secret',
           return_value={
               "host": "localhost",
               "port": 5432,
               "database": "test_db",
               "user": "test_user",
               "password": "test_password"
           })
    @patch('src.loading.loading.get_bucket_objects',
           return_value=single_processed_data_object_keys)
    def test_handler_calls_util_funcs_correct_number_of_times(
            self, caplog, capsys):

        mock_insert_data_fn = Mock()
        mock_renaming_folder_fn = Mock()

        mocked_cursor = MagicMock(
            return_value=Namespace(
                fetchall=lambda: [],
                execute=lambda a: None,
                description=[['person_id'], [
                    'forename'], ['surname'], ['date']],
                close=lambda: None
            ))

        mocked_connection = MagicMock(
            return_value=Namespace(
                cursor=mocked_cursor,
                close=lambda: None))

        s3 = boto3.client('s3', region_name='eu-west-2')
        folder_name = 'totesys_processed_data_1690986094.604827'
        bucket_name = 'nc-project-processed-data-8372747'

        s3.create_bucket(
            Bucket=bucket_name,
            CreateBucketConfiguration={'LocationConstraint': 'eu-west-2'}
        )

        df = pd.DataFrame(data=currency_data)
        transformed_currency = df.to_parquet()

        s3.put_object(
            Bucket=bucket_name,
            Key=f"{folder_name}/dim_currency.parquet",
            Body=transformed_currency
        )

        with patch('src.loading.loading.connect_to_server', return_value=mocked_connection):

            with patch('src.loading.loading.insert_transformed_data_into_warehouse') as mock_insert_data_fn:

                with patch('src.loading.loading.rename_folder_in_s3_bucket_once_loaded') as mock_renaming_folder_fn:

                    handler('event', 'context')

                    assert mock_insert_data_fn.call_count == 1

                    assert mock_renaming_folder_fn.call_count == 1

    @patch('src.loading.loading.get_secret',
           return_value={
               "host": "localhost",
               "port": 5432,
               "database": "test_db",
               "user": "test_user",
               "password": "test_password"
           })
    @patch('src.loading.loading.get_bucket_objects',
           return_value=multiple_loaded_data_object_keys)
    def test_handler_logs_message_if_no_new_data_to_load(self, caplog, capsys):

        mocked_cursor = MagicMock(
            return_value=Namespace(
                fetchall=lambda: [],
                execute=lambda a: None,
                description=[['person_id'], [
                    'forename'], ['surname'], ['date']],
                close=lambda: None
            ))

        mocked_connection = MagicMock(
            return_value=Namespace(
                cursor=mocked_cursor,
                close=lambda: None))

        s3 = boto3.client('s3', region_name='eu-west-2')
        bucket_name = 'nc-project-processed-data-8372747'

        s3.create_bucket(
            Bucket=bucket_name,
            CreateBucketConfiguration={'LocationConstraint': 'eu-west-2'}
        )

        with patch('src.loading.loading.connect_to_server', return_value=mocked_connection):

            result = handler('event', 'context')

            assert result == 'No new data to load.'

            # captured = capsys.readouterr()
            # # assert 'No new data to load.\n' in captured.out

            # # assert caplog.records[0].message == 'No new data to load.'
