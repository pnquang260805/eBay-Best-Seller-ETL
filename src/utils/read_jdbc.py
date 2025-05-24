from .spark_setup import SparkSetup
from pyspark.conf import SparkConf
from utils.logger import Logging

logger = Logging()

class ReadJDBC:
    def __init__(self, user, password,):
        self.url = "jdbc:ch://clickhouse:8123/warehouse"
        self.user = user
        self.password = password
        self.driver = "com.clickhouse.jdbc.ClickHouseDriver"
        self.jar = "/opt/spark/jars/clickhouse-jdbc-0.7.0.jar"

    @logger.log
    def read_jdbc(self,  table):
        conf = SparkConf().setMaster("local[2]").set("spark.jars", self.jar)
        spark = SparkSetup(conf).setupSpark()
        query = f"SELECT * FROM {table}"
        df = (spark.read.format("jdbc")
              .option("driver", self.driver)
              .option("url", self.url)
              .option("user", self.user)
              .option("password", self.password)
              .option("query", query)
              .load())
        return df