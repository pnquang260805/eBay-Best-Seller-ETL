from interface.transformer import Transformer
from utils.spark_setup import SparkSetup
from pyspark.conf import SparkConf
from pyspark.sql.functions import *
from utils.read_jdbc import ReadJDBC
from pyspark.sql.types import *
from utils.logger import Logging

logger = Logging()

class ItemTransform(Transformer):
    @logger.log
    def transform(self, data):
        conf = SparkConf().setAppName("transform").setMaster("local[2]")
        spark = SparkSetup(conf).setupSpark()

        username = "admin"
        password = "26082005qa"
        table = "dim_item"

        reader = ReadJDBC(username, password)
        df = reader.read_jdbc(table)
        df = df.select("item_sk","item_id")

        lastest_sk = df.select(max(col("item_sk"))).collect()[0][0]
        if lastest_sk is None:
            lastest_sk = 0

        columns_name = {
            "itemId" : "item_id",
            "itemLocation": "item_location",
            "adultOnly" : "adult_only",
            "itemOriginDate" : "item_origin_date",
            "itemCreationDate" : "item_creation_date"
        }

        item_raw = data.select("itemId", "title", "condition", "adultOnly", "itemLocation", "itemOriginDate", "itemCreationDate")
        for old_name, new_name in columns_name.items():
            item_raw = item_raw.withColumnRenamed(old_name, new_name)
        item = item_raw.withColumn("item_location", col("item_location.country"))
        item = item.withColumn("item_origin_date", col("item_origin_date").cast(StringType())) \
                    .withColumn("item_origin_date", col("item_origin_date").substr(0, 12)) \
                    .withColumn("item_origin_date", col("item_origin_date").cast(DateType())) \
                    .withColumn("item_creation_date", col("item_creation_date").cast(StringType())) \
                    .withColumn("item_creation_date", col("item_creation_date").substr(0, 12)) \
                    .withColumn("item_creation_date", col("item_creation_date").cast(DateType()))

        item = item.withColumn("item_id", split(col("item_id"), "\\|")[1])
        item = item.fillna("No condition", subset=["condition"])

        new_item = item.join(df, on="item_id", how="left_anti")
        new_item = new_item.withColumn("item_sk", monotonically_increasing_id() + lastest_sk + 1)

        return new_item