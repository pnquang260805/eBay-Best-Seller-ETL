from interface.loader import Loader
from utils.spark_setup import SparkSetup
from pyspark.conf import SparkConf

class JDBCLoader(Loader):
    def __init__(self, *args, **kwargs):
        self.properties = {
            "user": kwargs.get("user"),
            "password": kwargs.get("password"),
            "driver": kwargs.get("driver")
        }
        self.table = kwargs.get("table")
        self.url = kwargs.get("url")

    def load(self, data):
        conf = SparkConf().setAppName("Load_to_Warehouse").setMaster("local[2]")
        spark = SparkSetup(conf).setupSpark()
        