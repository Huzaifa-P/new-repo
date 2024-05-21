import boto3
from botocore.exceptions import ClientError
import json
import logging


def get_secret(secret_name):

    logger = logging.getLogger('Utils')
    secret_manager = boto3.client('secretsmanager', region_name="eu-west-2")

    try:
        secret = secret_manager.get_secret_value(SecretId=secret_name)
        return json.loads(secret['SecretString'])

    except ClientError as err:
        if err.response['Error']['Code'] == 'ResourceNotFoundException':
            logger.debug("This secret doesn't exist.")
            raise Exception
        else:
            logger.error(err)
            raise Exception
