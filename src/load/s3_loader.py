import os

import boto3

from interface.loader import Loader
from typing_extensions import override
from utils.spark_setup import SparkSetup
from datetime import date
from dotenv import load_dotenv
from pyspark.sql import SparkSession


load_dotenv()

MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY")


class S3Loader(Loader):
    def __init__(self, bucket_name):
        super().__init__()
        self.bucket_name = bucket_name

    @override
    def load(self, data):
        spark = (
            SparkSession.builder.appName("LOL")
            .config("spark.hadoop.fs.s3a.access.key", os.getenv("MINIO_ACCESS_KEY"))
            .config("spark.hadoop.fs.s3a.secret.key", os.getenv("MINIO_SECRET_KEY"))
            .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
            .config(
                "spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem"
            )
            .config("spark.hadoop.fs.s3a.path.style.access", "true")
            .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
            .config(
                "spark.hadoop.fs.s3a.aws.credentials.provider",
                "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
            )
            .getOrCreate()
        )
        print(data.show(5))

        today = date.today()
        # spark = SparkSession.builder.getOrCreate()
        data.write.mode("overwrite").parquet(
            f"s3a://{self.bucket_name}/{today}.parquet"
        )

        spark.stop()
