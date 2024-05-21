import boto3
import logging

def get_bucket_name(name):

    logger = logging.getLogger('Utils')
    s3_client = boto3.client('s3')

    try:
        s3_bucket_list = [bucket['Name'] for bucket in s3_client.list_buckets()[
            'Buckets'] if name in bucket['Name']]
        return s3_bucket_list[0]
    
    except IndexError:
        logger.error("Buckets need to be created before extraction")
        raise IndexError("No buckets in list")