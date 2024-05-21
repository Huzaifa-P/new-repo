import boto3
import pandas as pd
import logging
from utils.get_df import get_df
from awswrangler import exceptions as wrexceptions

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger()


def append_dates_to_df(target_df, target_df_column, input_df,  date_columns):
    for date_column in date_columns:
        date = pd.to_datetime(input_df[date_column]).dt.date
        target_df = pd.concat([target_df, pd.DataFrame(
            {target_df_column: date})], ignore_index=True)
    return target_df


def create_dim_date(new_folder, sales_order_key, payment_key, purchase_order_key, bucket_one, bucket_two):
    """
    Create a dimension table 'dim_date' by extracting unique date information from three dataframes:
    'sales_order_df', 'payment_df', and 'purchase_order_df', and store the result as a Parquet file in an S3 bucket.

    Parameters:
        new_folder (str): The name of the folder where the Parquet file will be stored in 'bucket_two'.
        sales_order_key (str): The key name for the 'sales_order_df' dataframe in 'bucket_one'.
        payment_key (str): The key name for the 'payment_df' dataframe in 'bucket_one'.
        purchase_order_key (str): The key name for the 'purchase_order_df' dataframe in 'bucket_one'.
        bucket_one (str): The name of the S3 bucket containing 'sales_order_df', 'payment_df', and 'purchase_order_df'.
        bucket_two (str): The name of the target S3 bucket where the 'dim_date.parquet' file will be stored.

    Raises:
        wrexceptions.NoFilesFound: If any of the specified files (keys) are not found in 'bucket_one'.
        Exception: If any other error occurs during the execution of the function.

    Returns:
        None

    Note:
        This function assumes that you have defined the following helper functions:
        - get_df(bucket_name, object_key): Retrieves a DataFrame from the specified S3 bucket and object key.
        - append_dates_to_df(df, date_column, source_df, date_columns): Appends unique dates from source_df[date_columns]
          to the date_column in the df DataFrame.
    
    Example:
        create_dim_date("new_folder_name", "sales_orders.csv", "payments.csv", "purchase_orders.csv", "source_bucket", "target_bucket")
    """
    try:

        dim_date_df = pd.DataFrame(columns=['date_id'])

        if sales_order_key:
            sales_order_df = get_df(bucket_one, sales_order_key)
            dim_date_df = append_dates_to_df(dim_date_df, 'date_id', sales_order_df, [
                                             'created_at', 'last_updated', 'agreed_delivery_date', 'agreed_payment_date'])

        if payment_key:
            payment_df = get_df(bucket_one, payment_key)
            dim_date_df = append_dates_to_df(dim_date_df, 'date_id', payment_df, [
                                             "created_at", "last_updated", "payment_date"])
        if purchase_order_key:
            purchase_order_df = get_df(bucket_one, purchase_order_key)
            dim_date_df = append_dates_to_df(dim_date_df, 'date_id', purchase_order_df, [
                                             "created_at", "last_updated", "agreed_delivery_date", "agreed_payment_date"])

        dim_date_df = dim_date_df.drop_duplicates(
            subset='date_id').reset_index(drop=True)

        # ADD EXTRA COLUMNS
        dim_date_df['date_id'] = pd.to_datetime(dim_date_df['date_id'])
        dim_date_df['year'] = dim_date_df['date_id'].dt.year
        dim_date_df['month'] = dim_date_df['date_id'].dt.month
        dim_date_df['day'] = dim_date_df['date_id'].dt.day
        dim_date_df['day_of_week'] = dim_date_df['date_id'].dt.dayofweek
        dim_date_df['day_name'] = dim_date_df['date_id'].dt.day_name()
        dim_date_df['month_name'] = dim_date_df['date_id'].dt.month_name()
        dim_date_df['quarter'] = dim_date_df['date_id'].dt.quarter

        # dim_date_df = dim_date_df.replace(np.nan, None)

        transformed_dim_date = dim_date_df.to_parquet()

        # Add parquet file to the processed data bucket
        s3 = boto3.client('s3')
        s3.put_object(Bucket=bucket_two,
                      Key=f"{new_folder}/dim_date.parquet",
                      Body=transformed_dim_date)

        logging.info("dim_date.parquet has been successfully created.")

    except wrexceptions.NoFilesFound as e:
        logging.info("File not found in bucket")
        logging.error(str(e))
    except Exception as e:
        logging.info("An error occurred:")
        logging.error(str(e))
        raise
