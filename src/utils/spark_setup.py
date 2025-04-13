from pyspark.sql import SparkSession


class SparkSetup:
    def __init__(self, app_name):
        self.app_name = app_name

    def setupSpark(self):
        spark = (
            SparkSession.builder.appName(self.app_name).master("local[2]").getOrCreate()
        )
        return spark
