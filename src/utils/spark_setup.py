from pyspark.sql import SparkSession


class SparkSetup:
    def __init__(self, conf):
        self.conf = conf

    def setupSpark(self):
        spark = SparkSession.builder.config(conf=self.conf).getOrCreate()
        return spark
