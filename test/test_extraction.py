from src.extraction.extraction import (create_folder_prefix,
                                       set_start_time_minus_minutes,
                                       get_column_names,
                                       get_latest_table_rows,
                                       handler
                                       )
from freezegun import freeze_time
from datetime import datetime, timezone, timedelta
from moto import mock_s3, mock_secretsmanager
import boto3
from unittest.mock import Mock, MagicMock
from psycopg2 import sql
from argparse import Namespace
import logging


@freeze_time("2012-01-01")
class TestFolderPrefixFunc:
    def test_folder_prefix_func_returns_string(self):
        name_input = "Test"
        expected = str
        result = type(create_folder_prefix(name_input))

        assert result == expected

    def test_folder_prefix_returns_string(self):
        name_input = "Blahblah"
        current_date_time = datetime.now(timezone.utc).timestamp()

        expected = f'Blahblah{current_date_time}'

        result = create_folder_prefix(name_input)

        assert result == expected


@freeze_time("2012-01-01")
class TestSetStartTimeFunc:
    def test_returns_type_datetime(self):
        expected = datetime
        result = type(set_start_time_minus_minutes(10))

        assert result == expected

    def test_returns_new_date_time(self):
        input = 30
        expected = datetime.now() - timedelta(minutes=input)
        result = set_start_time_minus_minutes(input)

        assert result == expected


class TestGetColumnNamesFunc:
    def test_get_column_names_when_description_is_empty(self):
        mock_cursor = Mock()
        mock_cursor.description = []

        column_names = get_column_names(mock_cursor)

        assert column_names == []

    def test_get_column_names(self):
        mock_cursor = Mock()
        mock_cursor.description = [('column1',), ('column2',), ('column3',)]

        column_names = get_column_names(mock_cursor)

        assert column_names == ['column1', 'column2', 'column3']


class TestGetTableRowsFunc:
    def test_query_is_being_called_and_returns_values(self):
        sample_table_data = [
            (1, 'Huzaifa', 'Patel', '2023-07-25 12:00:00'),
            (2, 'Kabilan', 'T', '2023-07-26 09:30:00')
        ]

        mock_cursor = Mock()
        mock_cursor.fetchall.return_value = sample_table_data

        table_name = "test_table"
        start_time = "2023-07-25 00:00:00"

        result = get_latest_table_rows(mock_cursor, table_name, start_time)

        assert result == [(1, 'Huzaifa', 'Patel', '2023-07-25 12:00:00'),
                          (2, 'Kabilan', 'T', '2023-07-26 09:30:00')]

        mock_cursor.execute.assert_called_once_with(
            sql.SQL("SELECT * FROM {table} WHERE {table}.last_updated > {time};").format(
                table=sql.Identifier(table_name), time=sql.Literal(start_time)))


@freeze_time("2023-07-01")
@mock_secretsmanager
@mock_s3
class TestHandlerTest:
    def test_stores_into_mock_bucket(self, mocker):
        s3_client = boto3.client('s3', region_name='eu-west-2')
        s3_client.create_bucket(
            Bucket='nc-project-ingestion-zone-',
            CreateBucketConfiguration={
                'LocationConstraint': 'eu-west-2'
            }
        )
        secret_client = boto3.client("secretsmanager", region_name='eu-west-2')

        secret_client.create_secret(
            Name="pg-oltp-db", SecretString='''{
                "host": "test_host",
                "port": 5432,
                "database": "test_db",
                "user": "test_user",
                "password": "test_pass"
            }''')

        mocked_cursor = MagicMock(
            return_value=Namespace(
                fetchall=lambda: [
                    (1, 'Huzaifa', 'Patel', '2023-07-25 12:00:00'),
                    (2, 'Kabilan', 'T', '2023-07-26 09:30:00')
                ],
                execute=lambda a: None,
                description=[['person_id'], [
                    'forename'], ['sirname'], ['date']],
                close=lambda: None
            ))

        mocked_connection = MagicMock(
            return_value=Namespace(
                cursor=mocked_cursor,
                close=lambda: None))

        mocker.patch(
            "src.extraction.extraction.connect_to_server",
            new=mocked_connection)
        mocker.patch(
            "src.extraction.extraction.lst_table_names",
            new=['test_table'])
        handler('test', 'test')

        my_bucket = s3_client.list_objects_v2(
            Bucket='nc-project-ingestion-zone-')

        result = my_bucket['Contents'][0]['Key']

        assert result == 'totesys_extraction_data_1688169600.0/test_table.parquet'


@freeze_time("2023-07-01")
@mock_secretsmanager
@mock_s3
class TestLogging:
    def test_log(self, mocker, caplog):
        s3_client = boto3.client('s3', region_name='eu-west-2')
        s3_client.create_bucket(
            Bucket='nc-project-ingestion-zone-',
            CreateBucketConfiguration={
                'LocationConstraint': 'eu-west-2'
            }
        )
        secret_client = boto3.client("secretsmanager", region_name='eu-west-2')

        secret_client.create_secret(
            Name="pg-oltp-db", SecretString='''{
                "host": "test_host",
                "port": 5432,
                "database": "test_db",
                "user": "test_user",
                "password": "test_pass"
            }''')

        mocked_cursor = MagicMock(
            return_value=Namespace(
                fetchall=lambda: [],
                execute=lambda a: None,
                description=[['person_id'], [
                    'forename'], ['sirname'], ['date']],
                close=lambda: None
            ))

        mocked_connection = MagicMock(
            return_value=Namespace(
                cursor=mocked_cursor,
                close=lambda: None))

        mocker.patch(
            "src.extraction.extraction.connect_to_server",
            new=mocked_connection)
        mocker.patch(
            "src.extraction.extraction.lst_table_names",
            new=['test_table'])

        with caplog.at_level(logging.INFO):
            handler("test", "test")
        # or, if you really need to check the log-level
        assert caplog.records[-1].levelname == "INFO"
        assert caplog.records[-1].message == "No new data within test_table in from 2023-06-30 23:45:00 to 2023-07-01 00:00:00"
        handler("test", "test")
