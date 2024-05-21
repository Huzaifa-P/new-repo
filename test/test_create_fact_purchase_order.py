from src.transformation.transformation_utils.create_fact_purchase_order import create_fact_purchase_order

import pandas as pd
import boto3
from moto import mock_s3
import awswrangler as wr
from utils.get_df import get_df

test_purchase_order_df = pd.DataFrame(
    {
        'purchase_order_id': [1, 2],
        'created_at': ['2022-11-03 00:00:00.000', '2022-11-04 00:00:00.000'],
        'last_updated': ['2022-11-03 00:00:00.000', '2022-11-04 00:00:00.000'],
        'staff_id': [1, 2],
        'counterparty_id': [1, 2],
        'item_code': ['Dummy1', 'Dummy2'],
        'item_quantity': [1, 2],
        'item_unit_price': pd.to_numeric([123.12, 213.21]),
        'currency_id': [1, 2],
        'agreed_delivery_date': ['2022-11-03 00:00:00.000', '2022-11-04 00:00:00.000'],
        'agreed_payment_date': ['2022-11-03 00:00:00.000', '2022-11-04 00:00:00.000'],
        'agreed_delivery_location_id': [1, 2],
        'company_ac_number': [1, 2],
        'counterparty_ac_number': [1, 2]
    }
)

test_final_df = pd.DataFrame(
    {
        # 'purchase_record_id': [1, 2],
        'purchase_order_id': [1, 2],
        'created_date': pd.to_datetime(['2022-11-03', '2022-11-04']).date,
        'created_time': pd.to_datetime(['00:00:00.000', '00:00:00.000']).time,
        'last_updated_date': pd.to_datetime(['2022-11-03', '2022-11-04']).date,
        'last_updated_time': pd.to_datetime(['00:00:00.000', '00:00:00.000']).time,
        'staff_id': [1, 2],
        'counterparty_id': [1, 2],
        'item_code': ['Dummy1', 'Dummy2'],
        'item_quantity': [1, 2],
        'item_unit_price': pd.to_numeric([123.12, 213.21]),
        'currency_id': [1, 2],
        'agreed_delivery_date': pd.to_datetime(['2022-11-03', '2022-11-04']).date,
        'agreed_payment_date': pd.to_datetime(['2022-11-03', '2022-11-04']).date,
        'agreed_delivery_location_id': [1, 2],
    }
)


@mock_s3
def test_function_creates_new_folder_with_new_purchase_order_fact_table():
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
        df=test_purchase_order_df,
        path="s3://test-extraction-bucket-/totesys_extraction_data_/purchase_order.parquet")

    bucket_one = "test-extraction-bucket-"
    purchase_order_key = "totesys_extraction_data_/purchase_order.parquet"
    bucket_two = "test-processed-bucket-"

    create_fact_purchase_order(
        new_folder='fact_folder',
        purchase_order_key=purchase_order_key,
        ingestion_bucket=bucket_one,
        processed_bucket=bucket_two)

    my_bucket = s3_client.list_objects_v2(
        Bucket='test-processed-bucket-')

    result = my_bucket['Contents'][0]['Key']

    assert result == 'fact_folder/fact_purchase_order.parquet'

    to_check = get_df('test-processed-bucket-',
                      'fact_folder/fact_purchase_order.parquet')

    pd.testing.assert_frame_equal(
        to_check.reset_index(
            drop=True), test_final_df.reset_index(
            drop=True), check_dtype=False)
