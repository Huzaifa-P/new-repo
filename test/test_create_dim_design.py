from src.transformation.transformation_utils.create_dim_design import (
    create_dim_design)

import pandas as pd
import boto3
import logging
from moto import mock_s3
import awswrangler as wr
from utils.get_df import get_df

test_design_df = pd.DataFrame(
    {
        'design_id': [1, 2],
        'created_at': ['Dummy', 'Dummy'],
        'design_name': ['Dummy', 'Dummy'],
        'file_location': ['Dummy', 'Dummy'],
        'file_name': ['Dummy', 'Dummy'],
        'last_updated': ['Dummy', 'Dummy']
    }
)

test_final_df = pd.DataFrame(
    {
        'design_id': [1, 2],
        'design_name': ['Dummy', 'Dummy'],
        'file_location': ['Dummy', 'Dummy'],
        'file_name': ['Dummy', 'Dummy']
    }
)


@mock_s3
def test_function_creates_new_folder_with_new_design_dim_table():
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
        df=test_design_df,
        path="s3://test-extraction-bucket-/totesys_extraction_data_/design.parquet")

    bucket_one = "test-extraction-bucket-"
    key = "totesys_extraction_data_/design.parquet"
    bucket_two = "test-processed-bucket-"

    create_dim_design(
        new_folder='dim_folder',
        key=key,
        bucket_one=bucket_one,
        bucket_two=bucket_two)

    my_bucket = s3_client.list_objects_v2(
        Bucket='test-processed-bucket-')

    result = my_bucket['Contents'][0]['Key']

    assert result == 'dim_folder/dim_design.parquet'

    to_check = get_df('test-processed-bucket-',
                      'dim_folder/dim_design.parquet')

    pd.testing.assert_frame_equal(
        to_check.reset_index(
            drop=True), test_final_df.reset_index(
            drop=True), check_dtype=False)


@mock_s3
def test_dim_design_error_handling(caplog):

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

    bucket_one = "test-extraction-bucket-"
    key = "totesys_extraction_data_/design.parquet"
    bucket_two = "test-processed-bucket-"

    with caplog.at_level(logging.INFO):
        create_dim_design(
            new_folder='dim_folder',
            key=key,
            bucket_one=bucket_one,
            bucket_two=bucket_two)

    assert caplog.records[0].levelname == "INFO"
    assert caplog.records[1].levelname == "ERROR"

    assert caplog.records[0].message == "File not found in bucket"
    assert caplog.records[1].message == "No files Found on: s3://test-extraction-bucket-/totesys_extraction_data_/design.parquet."
