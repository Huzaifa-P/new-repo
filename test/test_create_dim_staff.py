from src.transformation.transformation_utils.create_dim_staff import (
    create_dim_staff)
import logging
import pandas as pd
import boto3
from moto import mock_s3
import awswrangler as wr
from utils.get_df import get_df

test_staff_df = pd.DataFrame(
    {
        'staff_id': [1, 2],
        'first_name': ['Dummy', 'Dummy'],
        'last_name': ['Dummy', 'Dummy'],
        'department_id': [1, 2],
        'email_address': ['Dummy', 'Dummy'],
        'created_at': ['Dummy', 'Dummy'],
        'last_updated': ['Dummy', 'Dummy']
    }
)

test_department_df = pd.DataFrame(
    {
        'department_id': [1, 2],
        'department_name': ['Dummy', 'Dummy'],
        'location': ['Dummy', 'Dummy'],
        'manager': ['Dummy', 'Dummy'],
        'created_at': ['Dummy', 'Dummy'],
        'last_updated': ['Dummy', 'Dummy']
    }
)

test_final_df = pd.DataFrame(
    {
        'staff_id': [1, 2],
        'first_name': ['Dummy', 'Dummy'],
        'last_name': ['Dummy', 'Dummy'],
        'email_address': ['Dummy', 'Dummy'],
        'department_name': ['Dummy', 'Dummy'],
        'location': ['Dummy', 'Dummy']
    }
)


@mock_s3
def test_function_creates_new_folder_with_new_staff_dim_table():
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
        df=test_staff_df,
        path="s3://test-extraction-bucket-/totesys_extraction_data_/staff.parquet")

    wr.s3.to_parquet(
        df=test_department_df,
        path="s3://test-extraction-bucket-/totesys_extraction_data_/department.parquet")

    bucket_one = "test-extraction-bucket-"
    staff_key = "totesys_extraction_data_/staff.parquet"
    department_key = "totesys_extraction_data_/department.parquet"
    bucket_two = "test-processed-bucket-"

    create_dim_staff(
        new_folder='dim_folder',
        staff_key=staff_key,
        department_key=department_key,
        bucket_one=bucket_one,
        bucket_two=bucket_two)

    my_bucket = s3_client.list_objects_v2(
        Bucket='test-processed-bucket-')

    result = my_bucket['Contents'][0]['Key']

    assert result == 'dim_folder/dim_staff.parquet'

    to_check = get_df('test-processed-bucket-',
                      'dim_folder/dim_staff.parquet')

    pd.testing.assert_frame_equal(
        to_check.reset_index(
            drop=True), test_final_df.reset_index(
            drop=True), check_dtype=False)


@mock_s3
def test_dim_staff_error_handling(caplog):

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
    staff_key = "totesys_extraction_data_/staff.parquet"
    department_key = "totesys_extraction_data_/department.parquet"
    bucket_two = "test-processed-bucket-"

    with caplog.at_level(logging.INFO):
        create_dim_staff(
            new_folder='dim_folder',
            staff_key=staff_key,
            department_key=department_key,
            bucket_one=bucket_one,
            bucket_two=bucket_two)

    assert caplog.records[0].levelname == "INFO"
    assert caplog.records[1].levelname == "ERROR"

    assert caplog.records[0].message == "File not found in bucket:"
    assert caplog.records[1].message == "No files Found on: s3://test-extraction-bucket-/totesys_extraction_data_/staff.parquet."
