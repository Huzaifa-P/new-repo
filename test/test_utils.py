from moto import mock_s3, mock_secretsmanager
import boto3
import pytest
import json
import logging
import pandas as pd
import awswrangler as wr

from utils.connect_to_server import connect_to_server
from utils.get_secret import get_secret
from utils.get_bucket_name import get_bucket_name

from utils.get_df import get_df
from utils.df_merge_tables import df_merge_tables
from utils.df_drop_column import df_drop_column
from utils.df_rename_column import df_rename_column
from utils.df_set_column_type import df_set_column_type


class TestConnectionToServer:
    def test_connect_to_server_with_mock(self, mocker):

        mock_psycopg2_connect = mocker.patch('psycopg2.connect')

        expected_connection = mocker.MagicMock()
        mock_psycopg2_connect.return_value = expected_connection

        credentials = {
            "host": "localhost",
            "port": 5432,
            "database": "test_db",
            "user": "test_user",
            "password": "test_password"
        }

        connection = connect_to_server(credentials)

        mock_psycopg2_connect.assert_called_once_with(
            host="localhost",
            port=5432,
            dbname="test_db",
            user="test_user",
            password="test_password"
        )

        assert connection == expected_connection


@mock_secretsmanager
class TestGetSecret:
    def test_get_secret_will_retrieve_list_of_secrets(self):
        secret_manager = boto3.client(
            'secretsmanager', region_name='eu-west-2')

        secret_key_value = json.dumps(
            {'user': 'Hasan', 'password': 'Password'})
        secret_manager.create_secret(
            Name='secret',
            SecretString=secret_key_value
        )

        result = get_secret('secret')
        expected = {'user': 'Hasan', 'password': 'Password'}

        assert result == expected

    def test_get_secret_raise_an_error_when_no_secret_exists_for_name_passed_to_func(
            self, caplog):
        with pytest.raises(Exception):
            get_secret('secret')

    def test_get_secret_logs_a_debug_when_no_secret_exists_for_name_passed_to_func(
            self,
            caplog):
        with pytest.raises(Exception):
            with caplog.at_level(logging.DEBUG):
                get_secret('secret')
        # or, if you really need to check the log-level
        assert caplog.records[-1].levelname == "DEBUG"
        assert caplog.records[-1].message == "This secret doesn't exist."


@mock_s3
class TestGetBucketName:
    # ADD ERROR HANDLING AFTER HAPPY TESTS
    def test_receives_string_output_and_correct_bucket_name(self):
        s3_client = boto3.client('s3')
        s3_client.create_bucket(
            Bucket='testbucket',
            CreateBucketConfiguration={
                'LocationConstraint': 'eu-west-2'
            }
        )
        expected = 'testbucket'
        result = get_bucket_name("testbucket")

        assert isinstance(result, str)
        assert result == expected

    def test_receives_correct_bucket_name_with_unique_identifier(self):
        s3_client = boto3.client('s3')
        s3_client.create_bucket(
            Bucket='mockbucket-45789',
            CreateBucketConfiguration={
                'LocationConstraint': 'eu-west-2'
            }
        )
        s3_client.create_bucket(
            Bucket='anotherbucket',
            CreateBucketConfiguration={
                'LocationConstraint': 'eu-west-2'
            }
        )
        s3_client.create_bucket(
            Bucket='andanotherone',
            CreateBucketConfiguration={
                'LocationConstraint': 'eu-west-2'
            }
        )

        expected = 'mockbucket-45789'
        result = get_bucket_name("mockbucket")
        assert result == expected

    def test_handles_error_when_s3_is_empty(self, caplog):

        with pytest.raises(IndexError):
            get_bucket_name("Error")

        assert len(caplog.records) == 1
        assert caplog.records[0].levelname == "ERROR"
        assert "Buckets need to be created before extraction" in caplog.records[0].message


@mock_s3
class TestGetDF:
    test_counterparty_df = pd.DataFrame(
        {
            'counterparty_id': [1, 2],
            'counterparty_legal_name': ['Dummy', 'Dummy'],
            'legal_address_id': ['Dummy', 'Dummy'],
            'commercial_contact': ['Dummy', 'Dummy'],
            'delivery_contact': ['Dummy', 'Dummy'],
            'created_at': ['Dummy', 'Dummy'],
            'last_updated': ['Dummy', 'Dummy']
        }
    )

    test_address_df = pd.DataFrame({
        'address_id': [1, 2],
        'address_line_1': ['Dummy', 'Dummy'],
        'address_line_2': ['Dummy', 'Dummy'],
        'district': ['Dummy', 'Dummy'],
        'city': ['Dummy', 'Dummy'],
        'postal_code': ['Dummy', 'Dummy'],
        'country': ['Dummy', 'Dummy'],
        'phone': [12345, 12345],
        'created_at': ['Dummy', 'Dummy'],
        'last_updated': ['Dummy', 'Dummy']
    })

    def test_reads_counterparty_df_from_s3_bucket(self):
        s3_client = boto3.client('s3', region_name='eu-west-2')
        s3_client.create_bucket(
            Bucket='test-extraction-bucket-',
            CreateBucketConfiguration={
                'LocationConstraint': 'eu-west-2'
            }
        )
        s3_client.create_bucket(
            Bucket='test-processed-bucket-',
            CreateBucketConfiguration={
                'LocationConstraint': 'eu-west-2'
            }
        )

        wr.s3.to_parquet(
            df=self.test_counterparty_df,
            path="s3://test-extraction-bucket-/totesys_extraction_data_/counterparty.parquet")

        wr.s3.to_parquet(
            df=self.test_address_df,
            path="s3://test-extraction-bucket-/totesys_extraction_data_/address.parquet")

        result = get_df(
            'test-extraction-bucket-',
            'totesys_extraction_data_/counterparty.parquet')

        result2 = get_df(
            'test-extraction-bucket-',
            'totesys_extraction_data_/address.parquet')

        pd.testing.assert_frame_equal(
            result, self.test_counterparty_df, check_dtype=False)

        pd.testing.assert_frame_equal(
            result2, self.test_address_df, check_dtype=False)


@mock_s3
class TestMergeTables:
    test_counterparty_df = pd.DataFrame(
        {
            'counterparty_id': [1, 2],
            'counterparty_legal_name': ['Dummy', 'Dummy'],
            'legal_address_id': ['Dummy', 'Dummy'],
            'commercial_contact': ['Dummy', 'Dummy'],
            'delivery_contact': ['Dummy', 'Dummy'],
            'created_at': ['Dummy', 'Dummy'],
            'last_updated': ['Dummy', 'Dummy']
        }
    )

    test_address_df = pd.DataFrame({
        'address_id': [1, 2],
        'address_line_1': ['Dummy', 'Dummy'],
        'address_line_2': ['Dummy', 'Dummy'],
        'district': ['Dummy', 'Dummy'],
        'city': ['Dummy', 'Dummy'],
        'postal_code': ['Dummy', 'Dummy'],
        'country': ['Dummy', 'Dummy'],
        'phone': [12345, 12345],
        'created_at': ['Dummy', 'Dummy'],
        'last_updated': ['Dummy', 'Dummy']
    })

    test_merged_df = pd.DataFrame({
        'counterparty_id': [1, 2],
        'counterparty_legal_name': ['Dummy', 'Dummy'],
        'legal_address_id': ['Dummy', 'Dummy'],
        'commercial_contact': ['Dummy', 'Dummy'],
        'delivery_contact': ['Dummy', 'Dummy'],
        'created_at_x': ['Dummy', 'Dummy'],
        'last_updated_x': ['Dummy', 'Dummy'],
        'address_id': [float("NaN"), float("NaN")],
        'address_line_1': [float("NaN"), float("NaN")],
        'address_line_2': [float("NaN"), float("NaN")],
        'district': [float("NaN"), float("NaN")],
        'city': [float("NaN"), float("NaN")],
        'postal_code': [float("NaN"), float("NaN")],
        'country': [float("NaN"), float("NaN")],
        'phone': [float("NaN"), float("NaN")],
        'created_at_y': [float("NaN"), float("NaN")],
        'last_updated_y': [float("NaN"), float("NaN")]
    })

    def test_merges_df_using_df_merge_tables(self):
        s3_client = boto3.client('s3', region_name='eu-west-2')
        s3_client.create_bucket(
            Bucket='test-extraction-bucket-',
            CreateBucketConfiguration={
                'LocationConstraint': 'eu-west-2'
            }
        )
        s3_client.create_bucket(
            Bucket='test-processed-bucket-',
            CreateBucketConfiguration={
                'LocationConstraint': 'eu-west-2'
            }
        )

        wr.s3.to_parquet(
            df=self.test_counterparty_df,
            path="s3://test-extraction-bucket-/totesys_extraction_data_/counterparty.parquet")

        wr.s3.to_parquet(
            df=self.test_address_df,
            path="s3://test-extraction-bucket-/totesys_extraction_data_/address.parquet")

        test_merge_counterparty = get_df(
            'test-extraction-bucket-',
            'totesys_extraction_data_/counterparty.parquet')

        test_merge_address = get_df(
            'test-extraction-bucket-',
            'totesys_extraction_data_/address.parquet')

        result = df_merge_tables(
            test_merge_counterparty,
            test_merge_address,
            'legal_address_id',
            'address_id',
            'left')
        expected = self.test_merged_df

        pd.testing.assert_frame_equal(
            result, expected, check_dtype=False)


@mock_s3
class TestDropTablesDF:
    test_df = pd.DataFrame(
        {
            'counterparty_id': [1, 2],
            'counterparty_legal_name': ['Dummy', 'Dummy'],
            'legal_address_id': ['Dummy', 'Dummy'],
            'commercial_contact': ['Dummy', 'Dummy'],
            'delivery_contact': ['Dummy', 'Dummy'],
            'created_at': ['Dummy', 'Dummy'],
            'last_updated': ['Dummy', 'Dummy']
        }
    )

    test_single_table_dropped = pd.DataFrame(
        {
            'counterparty_id': [1, 2],
            'counterparty_legal_name': ['Dummy', 'Dummy'],
            'legal_address_id': ['Dummy', 'Dummy'],
            'commercial_contact': ['Dummy', 'Dummy'],
            'delivery_contact': ['Dummy', 'Dummy'],
            'created_at': ['Dummy', 'Dummy'],
        }
    )

    test_multiple_tables_dropped = pd.DataFrame(
        {
            'counterparty_id': [1, 2],
            'legal_address_id': ['Dummy', 'Dummy'],
            'delivery_contact': ['Dummy', 'Dummy'],
            'created_at': ['Dummy', 'Dummy'],
        }
    )

    def test_single_table_dropped_using_df_drop_column(self):
        to_drop_table = self.test_df

        result = df_drop_column(to_drop_table, ['last_updated'])

        expected = self.test_single_table_dropped

        pd.testing.assert_frame_equal(
            result, expected, check_dtype=False)

    def test_multiple_tables_dropped_using_df_drop_column(self):

        to_drop_table = self.test_df

        result = df_drop_column(to_drop_table, ['counterparty_legal_name',
                                                'commercial_contact',
                                                'last_updated'])

        expected = self.test_multiple_tables_dropped

        pd.testing.assert_frame_equal(
            result, expected, check_dtype=False)


class TestRenameColumnDF:
    def test_rename_single_column_in_df(self):
        test_table_to_rename = pd.DataFrame(
            {
                'counterparty_id': [1, 2],
                'counterparty_legal_name': ['Dummy', 'Dummy'],
                'legal_address_id': ['Dummy', 'Dummy'],
                'commercial_contact': ['Dummy', 'Dummy'],
                'delivery_contact': ['Dummy', 'Dummy'],
                'created_at': ['Dummy', 'Dummy'],
                'last_updated': ['Dummy', 'Dummy']
            }
        )
        df_rename_column(
            test_table_to_rename, {
                'created_at': 'renamed_created_at'})

        result = test_table_to_rename
        expected = pd.DataFrame({
            'counterparty_id': [1, 2],
            'counterparty_legal_name': ['Dummy', 'Dummy'],
            'legal_address_id': ['Dummy', 'Dummy'],
            'commercial_contact': ['Dummy', 'Dummy'],
            'delivery_contact': ['Dummy', 'Dummy'],
            'renamed_created_at': ['Dummy', 'Dummy'],
            'last_updated': ['Dummy', 'Dummy']
        })

        pd.testing.assert_frame_equal(
            result, expected, check_dtype=False)

    def test_rename_multiple_columns_in_df(self):
        test_table_to_rename = pd.DataFrame(
            {
                'counterparty_id': [1, 2],
                'counterparty_legal_name': ['Dummy', 'Dummy'],
                'legal_address_id': ['Dummy', 'Dummy'],
                'commercial_contact': ['Dummy', 'Dummy'],
                'delivery_contact': ['Dummy', 'Dummy'],
                'created_at': ['Dummy', 'Dummy'],
                'last_updated': ['Dummy', 'Dummy']
            }
        )

        df_rename_column(test_table_to_rename,
                         {'counterparty_legal_name': 'renamed_counterparty_legal_name',
                          'delivery_contact': 'renamed_delivery_contact',
                          'created_at': 'renamed_created_at'})

        expected = pd.DataFrame({
            'counterparty_id': [1, 2],
            'renamed_counterparty_legal_name': ['Dummy', 'Dummy'],
            'legal_address_id': ['Dummy', 'Dummy'],
            'commercial_contact': ['Dummy', 'Dummy'],
            'renamed_delivery_contact': ['Dummy', 'Dummy'],
            'renamed_created_at': ['Dummy', 'Dummy'],
            'last_updated': ['Dummy', 'Dummy']
        })

        result = test_table_to_rename

        pd.testing.assert_frame_equal(
            result, expected, check_dtype=False)


class TestSetColumnTypeDF:

    # errortesting

    def test_set_one_column_type(self):

        test_table = pd.DataFrame({'payment_id': [1, 2], 'created_at': [
                                  '2022-11-03 00:00:00.000', '2022-11-04 00:00:00.000']})

        expected_table = pd.DataFrame(({'payment_id': [3, 4], 'created_at': [
                                      '2022-11-04 00:00:00.000', '2022-11-05 00:00:00.000']}))
        expected_table['created_at'] = pd.to_datetime(
            expected_table['created_at'])
        expected = expected_table.dtypes

        result_table = df_set_column_type(
            test_table, ['created_at'], ['datetime64[ns]'])

        result = result_table.dtypes

        pd.testing.assert_series_equal(
            result, expected)

    def test_set_multiple_column_type(self):
        test_table = pd.DataFrame({'payment_id': [1, 2],
                                   'payment_amount': [123, 123],
                                   'counterparty_legal_name': ['Dummy1', 'Dummy2'],
                                   })

        expected_table = pd.DataFrame(({'payment_id': [3, 4],
                                        'payment_amount': [231.12, 231.12],
                                        'counterparty_legal_name': ['Dummy3', 'Dummy4'],
                                        }))

        expected_table = expected_table.astype({'payment_id': 'int32'})

        expected = expected_table.dtypes

        result_table = df_set_column_type(
            test_table, ['payment_id', 'payment_amount'], ['int32', 'float64'])
        result = result_table.dtypes

        pd.testing.assert_series_equal(
            result, expected)
