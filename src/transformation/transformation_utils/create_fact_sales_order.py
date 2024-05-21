import pandas as pd
import boto3
import logging
from utils.get_df import get_df
from utils.df_drop_column import df_drop_column
from utils.df_rename_column import df_rename_column
from awswrangler import exceptions as wrexceptions

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger()


def create_fact_sales_order(new_folder, key, bucket_one, bucket_two):
    """
    Create a fact table 'fact_sales_order' by processing data from a DataFrame and store it as a Parquet file
    in an S3 bucket.

    Parameters:
        new_folder (str): The name of the folder where the Parquet file will be stored in 'bucket_two'.
        key (str): The key name for the sales order DataFrame in 'bucket_one'.
        bucket_one (str): The name of the S3 bucket containing the sales order DataFrame.
        bucket_two (str): The name of the target S3 bucket where the 'fact_sales_order.parquet' file will be stored.

    Raises:
        wrexceptions.NoFilesFound: If the specified file (key) is not found in 'bucket_one'.
        pd.errors.EmptyDataError: If the file (key) in 'bucket_one' is empty.
        pd.errors.ParserError: If an error occurs while parsing the file (key) in 'bucket_one'.
        Exception: If any other error occurs during the execution of the function.

    Returns:
        None

    Note:
        This function assumes that you have defined the following helper functions:
        - get_df(bucket_name, object_key): Retrieves a DataFrame from the specified S3 bucket and object key.
        - df_drop_column(df, columns): Drops specified columns from the DataFrame.
        - df_rename_column(df, column_mapping): Renames columns in the DataFrame based on the provided mapping.

    Example:
        create_fact_sales_order("new_folder_name", "sales_order_data.csv", "source_bucket", "target_bucket")
    """
    try:
        # Read the sales order data from the specified S3 bucket
        df = get_df(bucket_one, key)

        # Perform data transformations
        df['created_at'] = pd.to_datetime(df['created_at'])
        # split the timestamp column into date and time columns
        df['created_date'] = df['created_at'].dt.date
        df['created_time'] = df['created_at'].dt.time
        # convert timestamp column to datetime format
        df['last_updated'] = pd.to_datetime(df['last_updated'])
        # split the timestamp column into date and time columns
        df['last_updated_date'] = df['last_updated'].dt.date
        df['last_updated_time'] = df['last_updated'].dt.time
        df = df_drop_column(df, ['created_at', 'last_updated'])
        # change the column name in place
        df_rename_column(df, {'staff_id': 'sales_staff_id'})

        # df = df.replace(np.nan, None)

        # Convert the transformed DataFrame to Parquet format
        transformed_sales_order = df.to_parquet()

        # Write the Parquet file to the destination bucket
        s3 = boto3.client('s3')
        s3.put_object(Bucket=bucket_two,
                      Key=f"{new_folder}/fact_sales_order.parquet",
                      Body=transformed_sales_order)

        logging.info("fact_sales_order.parquet has been successfully created.")

    except wrexceptions.NoFilesFound as e:
        logging.info("File not found in bucket:")
        logging.warning(f"The file '{key}' was not found in the '{bucket_one}' bucket.")
        logging.error(str(e))
    except pd.errors.EmptyDataError as e:
        logging.info("Empty data error:")
        logging.error(f"The file '{key}' in the '{bucket_one}' bucket is empty.")
        logging.error(str(e))
    except pd.errors.ParserError as e:
        logging.info("Parser error:")
        logging.error(f"An error occurred while parsing the file '{key}' in the '{bucket_one}' bucket.")
        logging.error(str(e))
    except Exception as e:
        logging.info("An error occurred:")
        logging.error(str(e))
        raise