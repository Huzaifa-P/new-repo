import boto3
import logging


def get_bucket_objects(bucket_name):
    try:
        logger = logging.getLogger('Utils')

        s3 = boto3.client('s3')

        bucket_objects = s3.list_objects_v2(Bucket=bucket_name)
        
        if 'Contents' in bucket_objects.keys():

            object_keys = [
                object['Key'] for object in s3.list_objects_v2(
                    Bucket=bucket_name)['Contents']]
            return object_keys
        
        else:
            return None
    
    except Exception as err:
        logger.error(err)
        raise Exception
