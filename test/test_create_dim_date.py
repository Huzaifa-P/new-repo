import pandas as pd
import boto3
import awswrangler as wr
import logging
from src.transformation.transformation_utils.create_dim_date import (
    create_dim_date)
from moto import mock_s3
from utils.get_df import get_df


test_sales_order_df = pd.DataFrame(
    {
        'sales_order_id': [1, 2],
        'created_at': ['2022-11-03 00:00:00.000', '2022-11-03 00:00:00.000'],
        'last_updated': ['2022-11-03 00:00:00.000', '2022-11-03 00:00:00.000'],
        'design_id': [1, 2],
        'staff_id': [1, 2],
        'counterparty_id': [1, 2],
        'units_sold': [1, 2],
        'unit-price': [1, 2],
        'currency_id': [1, 2],
        'agreed_delivery_date': ['2022-11-03 00:00:00.000', '2022-11-03 00:00:00.000'],
        'agreed_payment_date': ['2022-11-03 00:00:00.000', '2022-11-03 00:00:00.000'],
        'agreed_delivery_location_id': [1, 2]
    }
)

test_payment_df = pd.DataFrame(
    {
        'payment_id': [1, 2],
        'created_at': ['2022-11-03 00:00:00.000', '2022-11-03 00:00:00.000'],
        'last_updated': ['2022-11-03 00:00:00.000', '2022-11-03 00:00:00.000'],
        'transaction_id': [1, 2],
        'counterparty_id': [1, 2],
        'payment_amount': [1, 2],
        'currency_id': [1, 2],
        'payment_type_id': [1, 2],
        'paid': [True, False],
        'payment_date': ['2022-11-03 00:00:00.000', '2022-11-03 00:00:00.000'],
        'company_ac_number': [1, 2],
        'counterparty_ac_number': [1, 2]
    }
)

test_purchase_order_df = pd.DataFrame(
    {
        'purchase_order_id': [1, 2],
        'created_at': ['2022-11-03 00:00:00.000', '2022-11-04 00:00:00.000'],
        'last_updated': ['2022-11-03 00:00:00.000', '2022-11-04 00:00:00.000'],
        'staff_id': [1, 2],
        'counterparty_id': [1, 2],
        'item_code': ['Dummy', 'Dummy'],
        'item_quantity': [1, 2],
        'item_unit_price': [1, 2],
        'currency_id': [1, 2],
        'agreed_delivery_date': ['2022-11-03 00:00:00.000', '2022-11-04 00:00:00.000'],
        'agreed_payment_date': ['2022-11-03 00:00:00.000', '2022-11-04 00:00:00.000'],
        'agreed_delivery_location_id': [1, 2]
    }
)

test_final_df = pd.DataFrame(
    {
        'date_id': pd.to_datetime(['2022-11-03', '2022-11-04']),
        'year': [2022, 2022],
        'month': [11, 11],
        'day': [3, 4],
        'day_of_week': [3, 4],
        'day_name': ['Thursday', 'Friday'],
        'month_name': ['November', 'November'],
        'quarter': [4, 4]
    }
)


@mock_s3
def test_function_creates_new_folder_with_new_date_dim_table():
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
        df=test_sales_order_df,
        path="s3://test-extraction-bucket-/totesys_extraction_data_/sales_order.parquet")

    wr.s3.to_parquet(
        df=test_payment_df,
        path="s3://test-extraction-bucket-/totesys_extraction_data_/payment.parquet")

    wr.s3.to_parquet(
        df=test_purchase_order_df,
        path="s3://test-extraction-bucket-/totesys_extraction_data_/purchase_order.parquet")

    create_dim_date('dim_folder',
                    'totesys_extraction_data_/sales_order.parquet',
                    'totesys_extraction_data_/payment.parquet',
                    'totesys_extraction_data_/purchase_order.parquet',
                    'test-extraction-bucket-',
                    'test-processed-bucket-')

    my_bucket = s3_client.list_objects_v2(
        Bucket='test-processed-bucket-')

    result = my_bucket['Contents'][0]['Key']

    assert result == 'dim_folder/dim_date.parquet'

    to_check = get_df('test-processed-bucket-',
                      'dim_folder/dim_date.parquet')

    pd.testing.assert_frame_equal(
        to_check.reset_index(
            drop=True), test_final_df.reset_index(
            drop=True), check_dtype=False)


@mock_s3
def test_dim_date_error_handling(caplog):

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

    with caplog.at_level(logging.INFO):
        create_dim_date('dim_folder',
                        'totesys_extraction_data_/sales_order.parquet',
                        'totesys_extraction_data_/payment.parquet',
                        'totesys_extraction_data_/purchase_order.parquet',
                        'test-extraction-bucket-',
                        'test-processed-bucket-')

    assert caplog.records[0].levelname == "INFO"
    assert caplog.records[1].levelname == "ERROR"

    assert caplog.records[0].message == "File not found in bucket"
    assert caplog.records[1].message == "No files Found on: s3://test-extraction-bucket-/totesys_extraction_data_/sales_order.parquet."
