import boto3
import logging
from utils.get_df import get_df
from utils.df_merge_tables import df_merge_tables
from utils.df_drop_column import df_drop_column
from awswrangler import exceptions as wrexceptions
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger()


def create_dim_staff(new_folder, staff_key, department_key, bucket_one, bucket_two):
    """
    Create a dimension table 'dim_staff' by merging and processing data from two DataFrames: 'staff_df' and 'department_df',
    and store the result as a Parquet file in an S3 bucket.

    Parameters:
        new_folder (str): The name of the folder where the Parquet file will be stored in 'bucket_two'.
        staff_key (str): The key name for the 'staff_df' DataFrame in 'bucket_one'.
        department_key (str): The key name for the 'department_df' DataFrame in 'bucket_one'.
        bucket_one (str): The name of the S3 bucket containing 'staff_df' and 'department_df'.
        bucket_two (str): The name of the target S3 bucket where the 'dim_staff.parquet' file will be stored.

    Raises:
        wrexceptions.NoFilesFound: If any of the specified files (staff_key, department_key) are not found in 'bucket_one'.
        Exception: If any other error occurs during the execution of the function.

    Returns:
        None

    Note:
        This function assumes that you have defined the following helper functions:
        - get_df(bucket_name, object_key): Retrieves a DataFrame from the specified S3 bucket and object key.
        - df_drop_column(df, columns): Drops specified columns from the DataFrame.
        - df_merge_tables(left_df, right_df, left_on, right_on, how): Merges two DataFrames based on specific columns.

    Example:
        create_dim_staff("new_folder_name", "staff_data.csv", "department_data.csv", "source_bucket", "target_bucket")
    """
    try:
        # read parquet files
        staff_df = get_df(bucket_one, staff_key)

        department_df = get_df(bucket_one, department_key)

        staff_df = df_drop_column(staff_df, ['created_at', 'last_updated'])

        department_df = df_drop_column(department_df, [
                       'created_at', 'last_updated', 'manager'])

        df = df_merge_tables(staff_df, department_df,
                             'department_id', 'department_id', 'left')

        df = df_drop_column(df, 'department_id')

        # df = df.replace(np.nan, None)

        transformed_staff = df.to_parquet()
        s3 = boto3.client('s3')
        s3.put_object(Bucket=bucket_two,
                      Key=f"{new_folder}/dim_staff.parquet",
                      Body=transformed_staff)

        logging.info("dim_staff.parquet has been successfully created.")

    except wrexceptions.NoFilesFound as e:
        logging.info("File not found in bucket:")
        logging.error(str(e))
    except Exception as e:
        logging.info("An error occurred:")
        logging.error(str(e))
        raise