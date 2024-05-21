from src.transformation.transformation_utils.create_fact_payment import create_fact_payment
import pandas as pd
import boto3
from moto import mock_s3
import awswrangler as wr
from utils.get_df import get_df
import logging

test_payment_df = pd.DataFrame(
    {
        'payment_id': [1, 2],
        'created_at': ['2022-11-03 00:00:00.000', '2022-11-04 00:00:00.000'],
        'last_updated': ['2022-11-03 00:00:00.000', '2022-11-04 00:00:00.000'],
        'transaction_id': [1, 2],
        'counterparty_id': [1, 2],
        'payment_amount': [123.12, 123.12],
        'currency_id': [1, 2],
        'payment_type_id': [1, 2],
        'paid': [True, False],
        'payment_date': ['2022-11-03 00:00:00.000', '2022-11-03 00:00:00.000'],
        'company_ac_number': [1, 2],
        'counterparty_ac_number': [1, 2]
    }
)

test_final_df = pd.DataFrame(
    {
        'payment_id': [1, 2],
        'created_date': pd.to_datetime(['2022-11-03', '2022-11-04']).date,
        'created_time': pd.to_datetime(['00:00:00.000', '00:00:00.000']).time,
        'last_updated_date': pd.to_datetime(['2022-11-03', '2022-11-04']).date,
        'last_updated_time': pd.to_datetime(['00:00:00.000', '00:00:00.000']).time,
        'transaction_id': [1, 2],
        'counterparty_id': [1, 2],
        'payment_amount': pd.to_numeric([123.12, 123.12]),
        'currency_id': [1, 2],
        'payment_type_id': [1, 2],
        'paid': [True, False],
        'payment_date': pd.to_datetime(['2022-11-03', '2022-11-03']).date
    }
)


@mock_s3
def test_function_creates_new_folder_with_new_payment_fact_table():
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
        df=test_payment_df,
        path="s3://test-extraction-bucket-/totesys_extraction_data_/payment.parquet")

    bucket_one = "test-extraction-bucket-"
    payment_key = "totesys_extraction_data_/payment.parquet"
    bucket_two = "test-processed-bucket-"

    create_fact_payment(
        new_folder='fact_folder',
        payment_key=payment_key,
        ingestion_bucket=bucket_one,
        processed_bucket=bucket_two)

    my_bucket = s3_client.list_objects_v2(
        Bucket='test-processed-bucket-')

    result = my_bucket['Contents'][0]['Key']

    assert result == 'fact_folder/fact_payment.parquet'

    to_check = get_df('test-processed-bucket-',
                      'fact_folder/fact_payment.parquet')

    pd.testing.assert_frame_equal(
        to_check.reset_index(
            drop=True), test_final_df.reset_index(
            drop=True), check_dtype=False)


@mock_s3
def test_fact_payment_nofilesfound_error_handling(caplog):

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
    payment_key = "totesys_extraction_data_/payment.parquet"
    bucket_two = "test-processed-bucket-"

    with caplog.at_level(logging.INFO):
        create_fact_payment(
            new_folder='fact_folder',
            payment_key=payment_key,
            ingestion_bucket=bucket_one,
            processed_bucket=bucket_two)

    assert caplog.records[0].levelname == "INFO"
    assert caplog.records[1].levelname == "WARNING"
    assert caplog.records[2].levelname == "ERROR"

    assert caplog.records[0].message == "File not found in bucket:"
    assert caplog.records[1].message == "The file 'totesys_extraction_data_/payment.parquet' was not found in the 'test-extraction-bucket-' bucket."
    assert caplog.records[2].message == "No files Found on: s3://test-extraction-bucket-/totesys_extraction_data_/payment.parquet."


# @mock_s3
# def test_fact_payment_emptydataerror_error_handling(caplog):
#     test_empty_df = BytesIO()

#     s3_client = boto3.client('s3', region_name='eu-west-2')
#     s3_client.create_bucket(
#         Bucket='test-extraction-bucket-',
#         CreateBucketConfiguration={
#             'LocationConstraint': 'eu-west-2'
#         }
#     )
#     s3_client.create_bucket(
#         Bucket='test-processed-bucket-',
#         CreateBucketConfiguration={
#             'LocationConstraint': 'eu-west-2'
#         }
#     )

#     wr.s3.to_parquet(
#         df=test_empty_df,
#         path="s3://test-extraction-bucket-/totesys_extraction_data_/payment.parquet")

#     bucket_one = "test-extraction-bucket-"
#     payment_key = "totesys_extraction_data_/payment.parquet"
#     bucket_two = "test-processed-bucket-"

#     with caplog.at_level(logging.INFO):
#         create_fact_payment(
#         new_folder='fact_folder',
#         payment_key=payment_key,
#         ingestion_bucket=bucket_one,
#         processed_bucket=bucket_two)

#     assert caplog.records[0].levelname == "WARNING"
#     # assert caplog.records[1].levelname == "WARNING"
#     # assert caplog.records[2].levelname == "ERROR"

#     assert caplog.records[0].message == "Empty data error:"
#     assert caplog.records[1].message == "The file 'totesys_extraction_data_/payment.parquet' in the 'test-extraction-bucket-' bucket is empty."
#     assert caplog.records[2].message == "No files Found on: s3://test-extraction-bucket-/totesys_extraction_data_/payment.parquet."
