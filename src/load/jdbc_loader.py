from interface.loader import Loader
from utils.spark_setup import SparkSetup
from pyspark.conf import SparkConf
from utils.logger import Logging

logger = Logging()

class JDBCLoader(Loader):
    def __init__(self, *args, **kwargs):
        self.properties = {
            "user": kwargs.get("user"),
            "password": kwargs.get("password"),
            "driver": kwargs.get("driver"),
            "jdbcCompliant": "false"
        }
        self.url = kwargs.get("url")

    @logger.log
    def load(self, table, data):
        conf = SparkConf().setAppName("Load_to_Warehouse").setMaster("local[2]")
        spark = SparkSetup(conf).setupSpark()
        data.write \
            .format("jdbc") \
            .option("url", self.url) \
            .option("dbtable", table) \
            .option("user", self.properties["user"]) \
            .option("password", self.properties["password"]) \
            .option("driver", self.properties["driver"]) \
            .mode("append") \
            .save()
        