import boto3
import configparser
from datetime import datetime
import sys
import json
import logging

sys.path.append("/opt/airflow/src")
config = configparser.ConfigParser()
config.read('config.cfg')
logging.getLogger("boto3").setLevel(logging.WARNING)
logging.getLogger("botocore").setLevel(logging.WARNING)

s3_client = boto3.client('s3',
                         region_name='us-east-1',
                         aws_access_key_id=config.get('AWS', 'KEY'),
                         aws_secret_access_key=config.get('AWS', 'SECRET'))


def S3_upload(bucket: str, file: str, filename: str):
    print(datetime.now().strftime("%Y-%m-%d %H:%M:%S") + f" Uploading file to {bucket}/{filename}")
    return s3_client.put_object(
    Bucket=bucket,
    Key=filename,
    Body=file,
    ContentType="application/json"
    )

def get_file_content(bucket: str, key: str):
    """
    Get file content from S3 bucket
    param bucket: S3 bucket name
    param key: S3 object key (file path)
    """
    try:
        response = s3_client.get_object(Bucket=bucket,Key=key)
        json_data = response["Body"].read().decode("utf-8")
        json_response = json.loads(json_data)
        return json_response
    except Exception as e:
        print(f"Error downloading file {key} from bucket {bucket}: {str(e)}")
        return None

def get_multiple_files(bucket: str, prefix: str = None):
    """
    Get multiple files from S3 bucket, optionally filtered by prefix
    param bucket: S3 bucket name
    param prefix: Optional prefix to filter files (folder path)
    """
    try:
        files = {}
        objects = s3_client.list_objects_v2(Bucket=bucket, Prefix=prefix)
        for obj in objects.get('Contents', []):
            key = obj['Key']
            content = get_file_content(bucket, key)
            files[key] = content
        return files
    except Exception as e:
        print(f"Error getting files from bucket {bucket}: {str(e)}")
        return {}
