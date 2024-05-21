import pandas as pd
import boto3
import logging
from utils.get_df import get_df
from utils.df_drop_column import df_drop_column
from awswrangler import exceptions as wrexceptions

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger()


def create_dim_payment_type(new_folder, payment_type_key, ingestion_bucket, processed_bucket):
    """
    Create a dimension table 'dim_payment_type' by processing data from a DataFrame and store it as a Parquet file
    in an S3 bucket.

    Parameters:
        new_folder (str): The name of the folder where the Parquet file will be stored in 'processed_bucket'.
        payment_type_key (str): The key name for the payment_type DataFrame in 'ingestion_bucket'.
        ingestion_bucket (str): The name of the S3 bucket containing the payment_type DataFrame.
        processed_bucket (str): The name of the target S3 bucket where the 'dim_payment_type.parquet' file will be stored.

    Raises:
        wrexceptions.NoFilesFound: If the specified file (payment_type_key) is not found in 'ingestion_bucket'.
        pd.errors.EmptyDataError: If the file (payment_type_key) in 'ingestion_bucket' is empty.
        pd.errors.ParserError: If an error occurs while parsing the file (payment_type_key) in 'ingestion_bucket'.
        Exception: If any other error occurs during the execution of the function.

    Returns:
        None

    Note:
        This function assumes that you have defined the following helper functions:
        - get_df(bucket_name, object_key): Retrieves a DataFrame from the specified S3 bucket and object key.
        - df_drop_column(df, columns): Drops specified columns from the DataFrame.

    Example:
        create_dim_payment_type("new_folder_name", "payment_type_data.csv", "source_bucket", "target_bucket")
    """
    try:
        payment_type_df = get_df(ingestion_bucket, payment_type_key)

        df_drop_column(payment_type_df, ['created_at', 'last_updated'])

        # payment_type_df = payment_type_df.replace(np.nan, None)

        # not sure what this line does
        payment_type_df = payment_type_df[[
            "payment_type_id", "payment_type_name",]]

        transformed_dim_payment_type = payment_type_df.to_parquet()

        s3 = boto3.client('s3')
        s3.put_object(Bucket=processed_bucket,
                      Key=f"{new_folder}/dim_payment_type.parquet",
                      Body=transformed_dim_payment_type)

        logging.info("dim_payment_type.parquet has been successfully created.")

    except wrexceptions.NoFilesFound as e:
        logging.info(
            f"The file '{payment_type_key}' was not found in the '{ingestion_bucket}' bucket.")
        logging.error(str(e))
    except pd.errors.EmptyDataError as e:
        logging.warning(
            f"The file '{payment_type_key}' in the '{ingestion_bucket}' bucket is empty.")
        logging.error(str(e))
    except pd.errors.ParserError as e:
        logging.warning(
            f"An error occurred while parsing the file '{payment_type_key}' in the '{ingestion_bucket}' bucket.")
        logging.error(str(e))
    except Exception as e:
        logging.warning("An error occurred:")
        logging.error(str(e))
        raise
