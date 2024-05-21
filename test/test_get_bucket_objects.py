from moto import mock_s3
import boto3
import pandas as pd
from pytest import raises

from utils.get_bucket_objects import get_bucket_objects
from data.test_df_data import currency_data


@mock_s3
class TestGetBucketObjects:

    def test_function_returns_None_if_bucket_is_empty(self):

        s3 = boto3.client('s3', region_name='eu-west-2')
        bucket_name = 'test_bucket'

        s3.create_bucket(
            Bucket=bucket_name,
            CreateBucketConfiguration={'LocationConstraint': 'eu-west-2'}
        )

        my_objects = get_bucket_objects(bucket_name)
        assert my_objects is None

    def test_function_returns_object_inside_a_bucket_with_one_item(self):

        s3 = boto3.client('s3', region_name='eu-west-2')
        bucket_name = 'test_bucket'

        s3.create_bucket(
            Bucket=bucket_name,
            CreateBucketConfiguration={'LocationConstraint': 'eu-west-2'}
        )

        df = pd.DataFrame(data=currency_data)
        transformed_currency = df.to_parquet()

        s3.put_object(
            Bucket=bucket_name,
            Key='test.parquet',
            Body=transformed_currency
        )

        my_objects = get_bucket_objects(bucket_name)
        assert my_objects == ['test.parquet']

    def test_function_returns_object_inside_a_bucket_with_multiple_items(self):

        s3 = boto3.client('s3', region_name='eu-west-2')
        bucket_name = 'test_bucket'

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

        for data in [{'file': 'data1.parquet',
                      'body': transformed_data1},
                     {'file': 'data2.parquet',
                      'body': transformed_data2},
                     {'file': 'data3.parquet',
                      'body': transformed_data3}]:
            s3.put_object(
                Bucket=bucket_name,
                Key=data['file'],
                Body=data['body']
            )

        my_objects = get_bucket_objects(bucket_name)
        assert my_objects == [
            'data1.parquet',
            'data2.parquet',
            'data3.parquet']

    def test_function_raises_exception_if_bucket_name_does_not_exist(self):

        s3 = boto3.client('s3', region_name='eu-west-2')
        bucket_name = 'test_bucket'

        s3.create_bucket(
            Bucket=bucket_name,
            CreateBucketConfiguration={'LocationConstraint': 'eu-west-2'}
        )

        with raises(Exception):
            get_bucket_objects('hello')
