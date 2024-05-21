import pandas as pd
import boto3
import awswrangler as wr
from moto import mock_s3
from src.transformation.transformation_utils.create_dim_counterparty import create_dim_counterparty
from utils.get_df import get_df

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

test_address_df = pd.DataFrame(
    {
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
    }
)

test_final_df = pd.DataFrame({
    'counterparty_id': [1, 2],
    'counterparty_legal_name': ['Dummy', 'Dummy'],
    'counterparty_legal_address_line_1': [float("NaN"), float("NaN")],
    'counterparty_legal_address_line_2': [float("NaN"), float("NaN")],
    'counterparty_legal_district': [float("NaN"), float("NaN")],
    'counterparty_legal_city': [float("NaN"), float("NaN")],
    'counterparty_legal_postal_code': [float("NaN"), float("NaN")],
    'counterparty_legal_country': [float("NaN"), float("NaN")],
    'counterparty_legal_phone_number': [float("NaN"), float("NaN")],
})


@mock_s3
def test_function_creates_new_folder_with_new_dim_table():
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
        df=test_address_df,
        path="s3://test-extraction-bucket-/totesys_extraction_data_/address.parquet")
    wr.s3.to_parquet(
        df=test_counterparty_df,
        path="s3://test-extraction-bucket-/totesys_extraction_data_/counterparty.parquet")

    bucket_one = "test-extraction-bucket-"
    department_key = 'totesys_extraction_data_/counterparty.parquet'
    address_key = 'totesys_extraction_data_/address.parquet'
    bucket_two = "test-processed-bucket-"

    create_dim_counterparty("dim_folder", department_key, address_key,
                            bucket_one, bucket_two)

    my_bucket = s3_client.list_objects_v2(
        Bucket='test-processed-bucket-')

    result = my_bucket['Contents'][0]['Key']

    assert result == 'dim_folder/dim_counterparty.parquet'

    to_check = get_df('test-processed-bucket-',
                      'dim_folder/dim_counterparty.parquet')

    pd.testing.assert_frame_equal(
        to_check.reset_index(
            drop=True), test_final_df.reset_index(
            drop=True), check_dtype=False)


# @mock_s3
# def test_error_handling(caplog):

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

#     bucket_one = "test-extraction-bucket-"
#     department_key = 'totesys_extraction_data_/counterparty.parquet'
#     address_key = 'totesys_extraction_data_/address.parquet'
#     bucket_two = "test-processed-bucket-"


#     with caplog.at_level(logging.INFO):
#         create_dim_counterparty("dim_folder", department_key, address_key,
#                             bucket_one, bucket_two)

#     assert caplog.records[0].levelname == "INFO"
#     assert caplog.records[0].message == "File not found in bucket:"
