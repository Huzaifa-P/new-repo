import pandas as pd
import boto3
import logging
from utils.get_df import get_df
from utils.df_drop_column import df_drop_column
from awswrangler import exceptions as wrexceptions

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger()

def create_fact_purchase_order(new_folder, purchase_order_key, ingestion_bucket, processed_bucket):
    """
    Create a fact table 'fact_purchase_order' by processing data from a DataFrame and store it as a Parquet file
    in an S3 bucket.

    Parameters:
        new_folder (str): The name of the folder where the Parquet file will be stored in 'processed_bucket'.
        purchase_order_key (str): The key name for the purchase_order DataFrame in 'ingestion_bucket'.
        ingestion_bucket (str): The name of the S3 bucket containing the purchase_order DataFrame.
        processed_bucket (str): The name of the target S3 bucket where the 'fact_purchase_order.parquet' file will be stored.

    Raises:
        wrexceptions.NoFilesFound: If the specified file (purchase_order_key) is not found in 'ingestion_bucket'.
        pd.errors.EmptyDataError: If the file (purchase_order_key) in 'ingestion_bucket' is empty.
        pd.errors.ParserError: If an error occurs while parsing the file (purchase_order_key) in 'ingestion_bucket'.
        Exception: If any other error occurs during the execution of the function.

    Returns:
        None

    Note:
        This function assumes that you have defined the following helper functions:
        - get_df(bucket_name, object_key): Retrieves a DataFrame from the specified S3 bucket and object key.
        - df_drop_column(df, columns): Drops specified columns from the DataFrame.

    Example:
        create_fact_purchase_order("new_folder_name", "purchase_order_data.csv", "source_bucket", "target_bucket")
    """

    try:

        purchase_order_df = get_df(ingestion_bucket, purchase_order_key)

        purchase_order_df['created_at'] = pd.to_datetime(
            purchase_order_df['created_at'])
        purchase_order_df["created_date"] = purchase_order_df['created_at'].dt.date
        purchase_order_df["created_time"] = purchase_order_df['created_at'].dt.time
        df_drop_column(purchase_order_df, ['created_at'])

        purchase_order_df['last_updated'] = pd.to_datetime(
            purchase_order_df['last_updated'])
        purchase_order_df["last_updated_date"] = purchase_order_df['last_updated'].dt.date
        purchase_order_df["last_updated_time"] = purchase_order_df['last_updated'].dt.time

        purchase_order_df["agreed_delivery_date"] = pd.to_datetime(
            purchase_order_df["agreed_delivery_date"]).dt.date

        purchase_order_df["agreed_payment_date"] = pd.to_datetime(
            purchase_order_df["agreed_payment_date"]).dt.date

        purchase_order_df = purchase_order_df[["purchase_order_id", "created_date", "created_time", "last_updated_date", "last_updated_time",
                                               "staff_id", "counterparty_id", "item_code", "item_quantity", "item_unit_price", "currency_id", "agreed_delivery_date", "agreed_payment_date", "agreed_delivery_location_id"]]

        # purchase_order_df = purchase_order_df.replace(np.nan, None)

        transformed_fact_purchase_order = purchase_order_df.to_parquet()

        s3 = boto3.client('s3')
        s3.put_object(Bucket=processed_bucket,
                      Key=f"{new_folder}/fact_purchase_order.parquet",
                      Body=transformed_fact_purchase_order)

        logging.info("fact_purchase_order.parquet has been successfully created.")

    except wrexceptions.NoFilesFound as e:
        logging.info("File not found in bucket:")
        logging.warning(
            f"The file '{purchase_order_key}' was not found in the '{ingestion_bucket}' bucket.")
        logging.error(str(e))
    except pd.errors.EmptyDataError as e:
        logging.info("Empty data error:")
        logging.warning(
            f"The file '{purchase_order_key}' in the '{ingestion_bucket}' bucket is empty.")
        logging.error(str(e))
    except pd.errors.ParserError as e:
       
        logging.info(
            f"An error occurred while parsing the file '{purchase_order_key}' in the '{ingestion_bucket}' bucket.")
        logging.error(str(e))
    except Exception as e:
        logging.info("An error occurred:")
        logging.error(str(e))
        raise
