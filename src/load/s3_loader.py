import os

import boto3

from interface.loader import Loader
from pyspark.conf import SparkConf
from utils.spark_setup import SparkSetup
from datetime import date
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from botocore.exceptions import ClientError


load_dotenv()

MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY")


class BucketExists:
    def __init__(self, bucket_name):
        self.bucket_name = bucket_name
        self.client = boto3.client(
            "s3",
            endpoint_url="http://minio:9000",
            aws_access_key_id=MINIO_ACCESS_KEY,
            aws_secret_access_key=MINIO_SECRET_KEY,
            config=boto3.session.Config(signature_version="s3v4"),
            verify=False,
        )

    def bucket_exists(self):
        response = self.client.list_buckets()
        buckets = [bucket["Name"] for bucket in response["Buckets"]]
        if self.bucket_name not in buckets:
            self.client.create_bucket(Bucket=self.bucket_name)


class S3Loader(Loader):
    def __init__(self, bucket_name):
        super().__init__()
        self.bucket_name = bucket_name

    def load(self, data):
        bucketExists = BucketExists(self.bucket_name)
        bucketExists.bucket_exists()

        conf = (
            SparkConf()
            .setAppName("s3_loader")
            .set("spark.hadoop.fs.s3a.access.key", os.getenv("MINIO_ACCESS_KEY"))
            .set("spark.hadoop.fs.s3a.secret.key", os.getenv("MINIO_SECRET_KEY"))
            .set("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")  # !
            .set("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
            .set("spark.hadoop.fs.s3a.path.style.access", "true")
            .set("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
            .set(
                "spark.hadoop.fs.s3a.aws.credentials.provider",
                "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
            )
        )

        setup = SparkSetup(conf)
        spark = setup.setupSpark()

        print(data.show(5))

        today = date.today()
        # spark = SparkSession.builder.getOrCreate()
        data.write.mode("overwrite").parquet(
            f"s3a://{self.bucket_name}/{today}.parquet"
        )

        spark.stop()
