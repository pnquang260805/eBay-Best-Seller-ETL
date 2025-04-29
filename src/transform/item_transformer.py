from interface.transformer import Transformer
from utils.spark_setup import SparkSetup
from pyspark.conf import SparkConf
from pyspark.sql.functions import *

class ItemTransform(Transformer):

    def transform(self, data):
        conf = SparkConf().setAppName("transform").setMaster("local[2]")
        spark = SparkSetup(conf).setupSpark()

        item_raw = data.select("itemId", "title", "condition", "adultOnly", "itemLocation", "itemOriginDate", "itemCreationDate")
        item = item_raw.withColumn("itemLocation", col("itemLocation.country"))
        item = item.withColumn("itemId", split(col("itemId"), "\\|")[1])
        item = item.fillna("No condition", subset=["condition"])
        return item