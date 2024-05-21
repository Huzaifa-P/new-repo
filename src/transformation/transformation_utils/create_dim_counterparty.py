import boto3
import logging
from utils.get_df import get_df
from utils.df_merge_tables import df_merge_tables
from utils.df_drop_column import df_drop_column
from utils.df_rename_column import df_rename_column

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger()


def create_dim_counterparty(new_folder, counterparty_key, address_key, bucket_one, bucket_two):
    
    """
    Create a dimension table 'dim_counterparty' by merging and transforming data from two dataframes,
    'counterparty_df' and 'address_df', and store the result as a Parquet file in an S3 bucket.

    Parameters:
        new_folder (str): The name of the folder where the Parquet file will be stored in 'bucket_two'.
        counterparty_key (str): The key name for the 'counterparty_df' dataframe in 'bucket_one'.
        address_key (str): The key name for the 'address_df' dataframe in 'bucket_one'.
        bucket_one (str): The name of the S3 bucket containing 'counterparty_df' and 'address_df'.
        bucket_two (str): The name of the target S3 bucket where the 'dim_counterparty.parquet' file will be stored.

    Raises:
        Exception: If any error occurs during the execution of the function.

    Returns:
        None
    """
    
    try:
        counterparty_df = get_df(bucket_one, counterparty_key)
        address_df = get_df(bucket_one, address_key)

        new_counterparty_df = df_merge_tables(
            counterparty_df, address_df, 'legal_address_id', 'address_id', 'left')

        new_counterparty_df = df_drop_column(new_counterparty_df, ['legal_address_id',
                                                                   'commercial_contact',
                                                                   'delivery_contact',
                                                                   'created_at_x',
                                                                   'last_updated_x',
                                                                   'created_at_y',
                                                                   'last_updated_y',
                                                                   'address_id'])

        df_rename_column(new_counterparty_df, {
            'address_line_1': 'counterparty_legal_address_line_1',
            'address_line_2': 'counterparty_legal_address_line_2',
            'district': 'counterparty_legal_district',
            'city': 'counterparty_legal_city',
            'postal_code': 'counterparty_legal_postal_code',
            'phone': 'counterparty_legal_phone_number',
            'country': 'counterparty_legal_country',
        })

        # new_counterparty_df = new_counterparty_df.replace(np.nan, None)
        # new_counterparty_df = new_counterparty_df.fillna(None)

        transformed_dim_counter_party = new_counterparty_df.to_parquet()
        s3 = boto3.client('s3')
        s3.put_object(Bucket=bucket_two,
                      Key=f"{new_folder}/dim_counterparty.parquet",
                      Body=transformed_dim_counter_party)

        logging.info("dim_counterparty.parquet has been successfully created.")

    except Exception as e:
        logging.info("An error occurred:")
        logging.error(str(e))

        raise

