import pandas as pd
import boto3
import logging
from utils.get_df import get_df
from utils.df_drop_column import df_drop_column
from awswrangler import exceptions as wrexceptions

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger()


def create_fact_payment(new_folder, payment_key, ingestion_bucket, processed_bucket):
    """
    Create a fact table 'fact_payment' by processing data from a DataFrame and store it as a Parquet file
    in an S3 bucket.

    Parameters:
        new_folder (str): The name of the folder where the Parquet file will be stored in 'processed_bucket'.
        payment_key (str): The key name for the payment DataFrame in 'ingestion_bucket'.
        ingestion_bucket (str): The name of the S3 bucket containing the payment DataFrame.
        processed_bucket (str): The name of the target S3 bucket where the 'fact_payment.parquet' file will be stored.

    Raises:
        wrexceptions.NoFilesFound: If the specified file (payment_key) is not found in 'ingestion_bucket'.
        pd.errors.EmptyDataError: If the file (payment_key) in 'ingestion_bucket' is empty.
        pd.errors.ParserError: If an error occurs while parsing the file (payment_key) in 'ingestion_bucket'.
        Exception: If any other error occurs during the execution of the function.

    Returns:
        None

    Note:
        This function assumes that you have defined the following helper functions:
        - get_df(bucket_name, object_key): Retrieves a DataFrame from the specified S3 bucket and object key.
        - df_drop_column(df, columns): Drops specified columns from the DataFrame.

    Example:
        create_fact_payment("new_folder_name", "payment_data.csv", "source_bucket", "target_bucket")
    """
    try:
        payment_df = get_df(ingestion_bucket, payment_key)

        payment_df['created_at'] = pd.to_datetime(payment_df['created_at'])
        payment_df["created_date"] = payment_df['created_at'].dt.date
        payment_df["created_time"] = payment_df['created_at'].dt.time

        payment_df['last_updated'] = pd.to_datetime(payment_df['last_updated'])
        payment_df["last_updated_date"] = payment_df['last_updated'].dt.date
        payment_df["last_updated_time"] = payment_df['last_updated'].dt.time

        payment_df = df_drop_column(payment_df, ['created_at', 'last_updated'])

        payment_df["payment_date"] = pd.to_datetime(
            payment_df["payment_date"]).dt.date

        payment_df["payment_amount"] = pd.to_numeric(
            payment_df["payment_amount"])

        payment_df = payment_df[["payment_id", "created_date", "created_time", "last_updated_date", "last_updated_time",
                                 "transaction_id", "counterparty_id", "payment_amount", "currency_id", "payment_type_id", "paid", "payment_date",]]

        transformed_fact_payment = payment_df.to_parquet()

        s3 = boto3.client('s3')
        s3.put_object(Bucket=processed_bucket,
                      Key=f"{new_folder}/fact_payment.parquet",
                      Body=transformed_fact_payment)

        logging.info("fact_payment.parquet has been successfully created.")

    except wrexceptions.NoFilesFound as e:
        logging.info("File not found in bucket:")
        logging.warning(
            f"The file '{payment_key}' was not found in the '{ingestion_bucket}' bucket.")
        logging.error(str(e))
    except pd.errors.EmptyDataError as e:
        logging.info("Empty data error:")
        logging.warning(
            f"The file '{payment_key}' in the '{ingestion_bucket}' bucket is empty.")
        logging.error(str(e))
    except pd.errors.ParserError as e:
        logging.warning(
            f"An error occurred while parsing the file '{payment_key}' in the '{ingestion_bucket}' bucket.")
        logging.error(str(e))
    except Exception as e:

        logging.error(str(e))
        raise