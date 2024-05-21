import pandas as pd
import boto3
import logging
from utils.get_df import get_df
from utils.df_drop_column import df_drop_column
from utils.df_set_column_type import df_set_column_type
from awswrangler import exceptions as wrexceptions

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger()


def create_dim_transaction(new_folder, transaction_key, ingestion_bucket, processed_bucket):
    """
    Create a dimension table 'dim_transaction' by processing data from a DataFrame and store it as a Parquet file
    in an S3 bucket.

    Parameters:
        new_folder (str): The name of the folder where the Parquet file will be stored in 'processed_bucket'.
        transaction_key (str): The key name for the transaction DataFrame in 'ingestion_bucket'.
        ingestion_bucket (str): The name of the S3 bucket containing the transaction DataFrame.
        processed_bucket (str): The name of the target S3 bucket where the 'dim_transaction.parquet' file will be stored.

    Raises:
        wrexceptions.NoFilesFound: If the specified file (transaction_key) is not found in 'ingestion_bucket'.
        pd.errors.EmptyDataError: If the file (transaction_key) in 'ingestion_bucket' is empty.
        pd.errors.ParserError: If an error occurs while parsing the file (transaction_key) in 'ingestion_bucket'.
        Exception: If any other error occurs during the execution of the function.

    Returns:
        None

    Note:
        This function assumes that you have defined the following helper functions:
        - get_df(bucket_name, object_key): Retrieves a DataFrame from the specified S3 bucket and object key.
        - df_drop_column(df, columns): Drops specified columns from the DataFrame.
        - df_set_column_type(df, columns, types): Sets specified columns to the provided data types.

    Example:
        create_dim_transaction("new_folder_name", "transaction_data.csv", "source_bucket", "target_bucket")
    """
    try:
        transaction_df = get_df(ingestion_bucket, transaction_key)

        df_drop_column(transaction_df, ['created_at', 'last_updated'])

        # transaction_df.drop(
        #     columns=['created_at', 'last_updated'], inplace=True)

        df_set_column_type(transaction_df, ['sales_order_id', 'purchase_order_id'], [
                           'Int64', 'Int64'])

        # not sure what this is doing
        transaction_df = transaction_df[[
            "transaction_id", "transaction_type", "sales_order_id", "purchase_order_id",]]

        transformed_dim_transaction = transaction_df.to_parquet()

        s3 = boto3.client('s3')
        s3.put_object(Bucket=processed_bucket,
                      Key=f"{new_folder}/dim_transaction.parquet",
                      Body=transformed_dim_transaction)

        logging.info("dim_transaction.parquet has been successfully created.")

    except wrexceptions.NoFilesFound as e:
        logging.info("File not found in bucket:")
        logging.warning(
            f"The file '{transaction_key}' was not found in the '{ingestion_bucket}' bucket.")
        logging.error(str(e))
    except pd.errors.EmptyDataError as e:
        logging.info("Empty data error:")
        logging.warning(
            f"The file '{transaction_key}' in the '{ingestion_bucket}' bucket is empty.")
        logging.error(str(e))
    except pd.errors.ParserError as e:
        logging.info("Parser error:")
        logging.warning(
            f"An error occurred while parsing the file '{transaction_key}' in the '{ingestion_bucket}' bucket.")
        logging.error(str(e))
    except Exception as e:
        logging.info("An error occurred:")
        logging.error(str(e))
        raise
