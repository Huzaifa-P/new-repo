import boto3
import logging

def move_s3_objects_to_new_folder(files, current_folder_name, new_folder_name, s3_bucket):    

    logger = logging.getLogger('Utils')

    try:
        for file in files:
            new_file_name = file.replace(
                current_folder_name, new_folder_name)
            s3 = boto3.client('s3')
            s3.copy_object(Bucket=s3_bucket,
                        CopySource={'Bucket': s3_bucket, 'Key': file},
                        Key=new_file_name)
            s3.delete_object(Bucket=s3_bucket, Key=file)

        logger.info(f"Folder {current_folder_name} has been renamed to {new_folder_name}.")
    
    except Exception as err:
        logger.error(err)
        raise Exception