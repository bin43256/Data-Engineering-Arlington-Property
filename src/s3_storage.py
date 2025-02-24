import boto3
import configparser
import datetime
import io
import sys
import os

sys.path.append("/opt/airflow/src")
config = configparser.ConfigParser()
config.read('config.cfg')
s3_resource = boto3.resource('s3',
                             region_name='us-east-1',
                             aws_access_key_id=config.get('AWS', 'KEY'),
                             aws_secret_access_key=config.get('AWS', 'SECRET'))
s3_client = boto3.client('s3',
                         region_name='us-east-1',
                         aws_access_key_id=config.get('AWS', 'KEY'),
                         aws_secret_access_key=config.get('AWS', 'SECRET'))


def S3_upload(bucket: str, file: str, filename: str):
    print(f"Uploading file to {bucket}/{filename}")
    return s3_client.upload_fileobj(file, bucket, filename)

def get_file_content(bucket: str, key: str):
    """
    Get file content from S3 bucket
    param bucket: S3 bucket name
    param key: S3 object key (file path)
    return: file content as bytes in a BytesIO object
    """
    try:
        file_obj = io.BytesIO()
        s3_client.download_fileobj(bucket, key, file_obj)
        # Reset file pointer to beginning
        file_obj.seek(0)
        return file_obj
    except Exception as e:
        print(f"Error downloading file {key} from bucket {bucket}: {str(e)}")
        return None

def get_multiple_files(bucket: str, prefix: str = None):
    """
    Get multiple files from S3 bucket, optionally filtered by prefix
    param bucket: S3 bucket name
    param prefix: Optional prefix to filter files (folder path)
    return: Dictionary with file keys and their contents
    """
    try:
        files = {}
        objects = s3_client.list_objects_v2(Bucket=bucket, Prefix=prefix) if prefix else s3_client.list_objects_v2(Bucket=bucket)
        for obj in objects.get('Contents', []):
            key = obj['Key']
            print(f"get file {key} from bucket {bucket}")
            content = get_file_content(bucket, key)
            if content:
                files[key] = content

        return files
    except Exception as e:
        print(f"Error getting files from bucket {bucket}: {str(e)}")
        return {}
