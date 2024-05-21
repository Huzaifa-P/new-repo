import boto3
import logging
from utils.get_df import get_df
from utils.df_drop_column import df_drop_column
from awswrangler import exceptions as wrexceptions

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger()


def create_dim_design(new_folder, key, bucket_one, bucket_two):
    """
    Create a dimension table 'dim_design' by processing data from a DataFrame and store it as a Parquet file
    in an S3 bucket.

    Parameters:
        new_folder (str): The name of the folder where the Parquet file will be stored in 'bucket_two'.
        key (str): The key name for the design DataFrame in 'bucket_one'.
        bucket_one (str): The name of the S3 bucket containing the design DataFrame.
        bucket_two (str): The name of the target S3 bucket where the 'dim_design.parquet' file will be stored.

    Raises:
        wrexceptions.NoFilesFound: If the specified file (key) is not found in 'bucket_one'.
        Exception: If any other error occurs during the execution of the function.

    Returns:
        None

    Note:
        This function assumes that you have defined the following helper functions:
        - get_df(bucket_name, object_key): Retrieves a DataFrame from the specified S3 bucket and object key.
        - df_drop_column(df, columns): Drops specified columns from the DataFrame.

    Example:
        create_dim_design("new_folder_name", "design_data.csv", "source_bucket", "target_bucket")
    """
    try:
        df_design = get_df(bucket_one, key)

        df_design = df_drop_column(df_design, ['last_updated', 'created_at'])

        # df_design = df_design.replace(np.nan, None)

        transformed_dim_design = df_design.to_parquet()

        # Add parquet file to the processed data bucket
        s3 = boto3.client('s3')
        s3.put_object(Bucket=bucket_two,
                      Key=f"{new_folder}/dim_design.parquet",
                      Body=transformed_dim_design)

        logging.info("dim_design.parquet has been successfully created.")

    except wrexceptions.NoFilesFound as e:
        logging.info("File not found in bucket")
        logging.error(str(e))
    except Exception as e:
        logging.info("An error occurred:")
        logging.error(str(e))
        raise

