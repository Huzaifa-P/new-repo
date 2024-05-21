from utils.df_drop_column import df_drop_column
from utils.get_df import get_df
import boto3
import logging
from awswrangler import exceptions as wrexceptions

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger()


logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger()


def create_dim_currency(new_folder, key, bucket_one, bucket_two):
    """
    Create a dimension table 'dim_currency' by processing data from a DataFrame and store it as a Parquet file
    in an S3 bucket.

    Parameters:
        new_folder (str): The name of the folder where the Parquet file will be stored in 'bucket_two'.
        key (str): The key name for the currency DataFrame in 'bucket_one'.
        bucket_one (str): The name of the S3 bucket containing the currency DataFrame.
        bucket_two (str): The name of the target S3 bucket where the 'dim_currency.parquet' file will be stored.

    Raises:
        wrexceptions.NoFilesFound: If the specified file (key) is not found in 'bucket_one'.
        Exception: If any other error occurs during the execution of the function.

    Returns:
        None
    """
    try:
        df_currency = get_df(bucket_one, key)

        df_currency = df_drop_column(df_currency, ['last_updated', 'created_at'])
        
        # generate currency name column fro currency code
        
        convert_currency_code = {
            'GBP' : 'British Pound',
            'USD' : 'US Dollar',
            'EUR' : 'Euro'
        }

        df_currency['currency_name'] = df_currency['currency_code'].map(convert_currency_code)

        transformed_currency = df_currency.to_parquet()
        s3 = boto3.client('s3')
        s3.put_object(Bucket=bucket_two,
                      Key=f"{new_folder}/dim_currency.parquet",
                      Body=transformed_currency)

        logging.info("dim_currency.parquet has been successfully created.")

    except wrexceptions.NoFilesFound as e:
        logging.info("File not found in bucket")
        logging.error(str(e))
    except Exception as e:
        logging.info("An error occurred:")
        logging.error(str(e))
        raise


if __name__ == "__main__":

    bucket_one = "nc-project-ingestion-zone-20230807145606324400000002"
    key = "totesys_transformed_data_1691420262.308896/currency.parquet"
    bucket_two = "nc-project-processed-data-20230807145606324100000001"
    new_folder = 'a'
    create_dim_currency(new_folder,bucket_one=bucket_one, key=key, bucket_two=bucket_two)
