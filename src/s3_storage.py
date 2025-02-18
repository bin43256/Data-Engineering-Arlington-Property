import boto3
import configparser
from transformation import logger
import datetime
config = configparser.ConfigParser()
config.read('src/config.cfg')


s3_resource = boto3.resource('s3',
                             region_name='us-east-1',
                             aws_access_key_id=config.get('AWS', 'KEY'),
                             aws_secret_access_key=config.get('AWS', 'SECRET'))
s3_client = boto3.client('s3',
                         region_name='us-east-1',
                         aws_access_key_id=config.get('AWS', 'KEY'),
                         aws_secret_access_key=config.get('AWS', 'SECRET'))


def S3_upload(dest_bucket: str, file: str, filename: str):
    logger.debug(f'Uploading {file} to the {dest_bucket} bucket')
    return s3_client.upload_fileobj(file, dest_bucket, filename)

def get_all_files(bucket: str):
    logger.debug(f'Getting all files from the {bucket} bucket')
    return s3_resource.Bucket(bucket).objects.all()