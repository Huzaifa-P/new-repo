from moto import mock_s3
import boto3
from pytest import raises
import pandas as pd

from utils.move_s3_objects import move_s3_objects_to_new_folder
from data.test_df_data import currency_data
from utils.get_bucket_objects import get_bucket_objects


@mock_s3
class TestMoveS3ObjectsToNewFolder:
    def test_function_copies_s3_objects_into_new_folder_with_one_item(self):

        s3 = boto3.client('s3', region_name='eu-west-2')

        bucket_name = 'test_bucket'
        current_folder_name = 'totesys_processed_data'
        new_folder_name = 'totesys_loaded_data'
        files = ['totesys_processed_data/test.parquet']
        expected_output = ['totesys_loaded_data/test.parquet']

        s3.create_bucket(
            Bucket=bucket_name,
            CreateBucketConfiguration={'LocationConstraint': 'eu-west-2'}
        )

        df = pd.DataFrame(data=currency_data)
        transformed_currency = df.to_parquet()

        s3.put_object(
            Bucket=bucket_name,
            Key='totesys_processed_data/test.parquet',
            Body=transformed_currency
        )

        my_original_folder = get_bucket_objects(bucket_name)
        assert my_original_folder == files

        move_s3_objects_to_new_folder(
            files,
            current_folder_name,
            new_folder_name,
            bucket_name)

        my_new_folder = get_bucket_objects(bucket_name)
        assert my_new_folder == expected_output

    def test_function_copies_s3_objects_into_new_folder_with_multiple_items(
            self):

        s3 = boto3.client('s3', region_name='eu-west-2')

        bucket_name = 'test_bucket'
        current_folder_name = 'totesys_processed_data'
        new_folder_name = 'totesys_loaded_data'
        files = [
            'totesys_processed_data/data1.parquet',
            'totesys_processed_data/data2.parquet',
            'totesys_processed_data/data3.parquet']
        expected_output = [
            'totesys_loaded_data/data1.parquet',
            'totesys_loaded_data/data2.parquet',
            'totesys_loaded_data/data3.parquet']

        s3.create_bucket(
            Bucket=bucket_name,
            CreateBucketConfiguration={'LocationConstraint': 'eu-west-2'}
        )

        df1 = pd.DataFrame(data=currency_data)
        transformed_data1 = df1.to_parquet()

        df2 = pd.DataFrame(data=currency_data)
        transformed_data2 = df2.to_parquet()

        df3 = pd.DataFrame(data=currency_data)
        transformed_data3 = df3.to_parquet()

        for data in [{'file': 'totesys_processed_data/data1.parquet',
                      'body': transformed_data1},
                     {'file': 'totesys_processed_data/data2.parquet',
                      'body': transformed_data2},
                     {'file': 'totesys_processed_data/data3.parquet',
                      'body': transformed_data3}]:
            s3.put_object(
                Bucket=bucket_name,
                Key=data['file'],
                Body=data['body']
            )

        my_original_folder = get_bucket_objects(bucket_name)
        assert my_original_folder == files

        move_s3_objects_to_new_folder(
            files,
            current_folder_name,
            new_folder_name,
            bucket_name)

        my_new_folder = get_bucket_objects(bucket_name)
        assert my_new_folder == expected_output

    def test_function_deletes_original_s3_objects_from_old_folder(self):

        s3 = boto3.client('s3', region_name='eu-west-2')

        bucket_name = 'test_bucket'
        current_folder_name = 'totesys_processed_data'
        new_folder_name = 'totesys_loaded_data'
        files = ['totesys_processed_data/test.parquet']

        s3.create_bucket(
            Bucket=bucket_name,
            CreateBucketConfiguration={'LocationConstraint': 'eu-west-2'}
        )

        df = pd.DataFrame(data=currency_data)
        transformed_currency = df.to_parquet()

        s3.put_object(
            Bucket=bucket_name,
            Key='totesys_processed_data/test.parquet',
            Body=transformed_currency
        )

        my_original_folder = get_bucket_objects(bucket_name)
        assert my_original_folder == files

        move_s3_objects_to_new_folder(
            files,
            current_folder_name,
            new_folder_name,
            bucket_name)

        my_new_folder = get_bucket_objects(bucket_name)
        assert files[0] not in my_new_folder

    def test_function_raises_an_exception_when_passed_a_bucket_name_or_current_folder_or_files_that_do_not_exist(
            self):

        s3 = boto3.client('s3', region_name='eu-west-2')

        bucket_name = 'test_bucket'
        current_folder_name = 'totesys_processed_data'
        new_folder_name = 'totesys_loaded_data'
        files = ['totesys_processed_data/test.parquet']

        s3.create_bucket(
            Bucket=bucket_name,
            CreateBucketConfiguration={'LocationConstraint': 'eu-west-2'}
        )

        df = pd.DataFrame(data=currency_data)
        transformed_currency = df.to_parquet()

        s3.put_object(
            Bucket=bucket_name,
            Key='totesys_processed_data/test.parquet',
            Body=transformed_currency
        )

        with raises(Exception):
            move_s3_objects_to_new_folder(
                [1, 2, 3, 4, 5],
                current_folder_name,
                new_folder_name,
                bucket_name)

        with raises(Exception):
            move_s3_objects_to_new_folder(
                files,
                'current_folder_name',
                new_folder_name,
                bucket_name)

        with raises(Exception):
            move_s3_objects_to_new_folder(
                files,
                current_folder_name,
                new_folder_name,
                'bucket_name')
