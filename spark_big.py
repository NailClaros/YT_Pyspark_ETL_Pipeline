from pyspark.sql import SparkSession
import kagglehub
import os
from dotenv import load_dotenv
import pandas as pd
from awsfuncs import get_s3_client, upload_file, list_files, file_exists_in_s3

load_dotenv()

# Download the dataset locally and looks like this when downloaded
"""
['CAvideos.csv', 'CA_category_id.json', 'DEvideos.csv', 'DE_category_id.json', 'FRvideos.csv',
 'FR_category_id.json', 'GBvideos.csv', 'GB_category_id.json', 'INvideos.csv', 'IN_category_id.json', 
 'JPvideos.csv', 'JP_category_id.json', 'KRvideos.csv', 'KR_category_id.json', 'MXvideos.csv', 
 'MX_category_id.json', 'RUvideos.csv', 'RU_category_id.json', 'USvideos.csv', 'US_category_id.json']
"""
# These are all the unique nation codes
"""
['CA', 'DE', 'FR', 'GB', 'IN', 'JP', 'KR', 'MX', 'RU', 'US']
"""


# print(nation_dict) 
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_REGION = os.getenv("AWS_REGION")

spark = (
    SparkSession.builder
    .appName("YT_Pyspark")
    .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .config("spark.hadoop.fs.s3a.access.key", os.getenv("AWS_ACCESS_KEY_ID"))
    .config("spark.hadoop.fs.s3a.secret.key", os.getenv("AWS_SECRET_ACCESS_KEY"))
    .config("spark.hadoop.fs.s3a.endpoint", f"s3.{os.getenv('AWS_REGION')}.amazonaws.com")
    .getOrCreate()
)

# Download dataset
path = kagglehub.dataset_download("datasnaek/youtube-new")

#Test one file
upload_file(
    bucket=os.getenv("BUCKET_NAME"),
    filepath=f"{path}\\GBvideos.csv",
    key="youtube/GBvideos.csv"
)

print("Files in S3 Bucket after upload:")
print(f"Fill exists: {file_exists_in_s3(os.getenv('BUCKET_NAME'), f'youtube/GBvideos.csv')}")

df = spark.read.option("header", "true").csv(
    f"s3a://{os.getenv('BUCKET_NAME')}/youtube/GBvideos.csv"
)

df.show(5)

