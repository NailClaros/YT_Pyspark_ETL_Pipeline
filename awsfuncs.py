import os
import boto3
from dotenv import load_dotenv
from botocore.exceptions import BotoCoreError, ClientError
load_dotenv()

def get_s3_client():
    """Returns the offical production S3 client using credentials from environment variables."""
    return boto3.client(
        "s3",
        region_name=os.getenv("AWS_REGION"),
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY")
    )

def list_files(bucket, s3=None):
    """Lists all file names in the given S3 bucket and returns them.\n
    bucket: str : Name of the S3 bucket
    s3: boto3.client : Optional S3 client. If None, the proper client will be created for production use.
    """
    if s3 is None:
        s3 = get_s3_client()

    response = s3.list_objects_v2(Bucket=bucket)
    if "Contents" in response:
        keys = [obj["Key"] for obj in response["Contents"]]
        return keys
    else:
        print("Bucket is empty or doesn't exist.")
        return []
    
def file_exists_in_s3(bucket_name, key, s3_client=None):
    """Checks if a file exists in S3 using provided client.\n
    bucket_name: str : Name of the S3 bucket
    key: str : Key of the file to check
    s3_client: boto3.client : Optional S3 client. If None, the proper 
    production client will be created and used.
    """
    if s3_client is None:
        s3_client = get_s3_client()
    try:
        if key is None:
            return False
        if bucket_name is None:
            bucket_name = os.getenv("BUCKET_NAME")
        s3_client.head_object(Bucket=bucket_name, Key=key)
        return True
    except ClientError as e:
        if e.response['Error']['Code'] == '404':
            return False
        else:
            raise #Some other error occurred


def upload_file(bucket, filepath, key, s3_client=None):
    """Uploads a file to the S3 bucket using the given s3_client, or default global client.\n
    bucket: str : Name of the S3 bucket
    filepath: str : Path to the local file to upload
    key: str : Key (path) in the S3 bucket where the file will be stored
    s3_client: boto3.client : Optional S3 client. If None, the proper production client 
    will be created and used.
    """
    if s3_client is None:
        s3_client = get_s3_client()  # fallback to your global client

    try:
        exists_in_s3 = file_exists_in_s3(bucket, key, s3_client)
        exists_locally = os.path.exists(filepath)

        if not exists_locally or exists_in_s3:
            print(f"File '{filepath}' does not exist or already exists in S3 as '{key}'. Skipping upload.")
            print(f"File exists in S3: {exists_in_s3}")
            print(f"File exists locally: {exists_locally}")
            return

        print(f"Uploading {filepath} to s3://{key}")
        s3_client.upload_file(filepath, bucket, key)
        print(f"upload successful: {filepath} -> s3://{key}")

    except (BotoCoreError, ClientError) as e:
        print(f"Upload failed: {e}")
    except Exception as e:
        print(f"Unexpected error: {e}")
