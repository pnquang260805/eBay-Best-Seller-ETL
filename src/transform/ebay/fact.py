from datetime import date

from interface.transformer import Transformer
from utils.spark_setup import SparkSetup
from pyspark.conf import SparkConf
from pyspark.sql.functions import *
from utils.read_jdbc import ReadJDBC
from utils.logger import Logging

logger = Logging()


class FactTransform(Transformer):
    @logger.log
    def transform(self, data):
        username = "admin"
        password = "26082005qa"
        item_table = "dim_item"
        seller_table = "dim_seller"
        date_key = date.today().strftime("%Y%m%d")

        reader = ReadJDBC(username, password)
        items = reader.read_jdbc(item_table)
        sellers = reader.read_jdbc(seller_table)

        data = data.select("itemId", col("seller.username").alias("seller_name"),
                           col("price.value").alias("price"),
                           col("topRatedBuyingExperience").alias("top_rated_buying_experience"),
                           col("listingMarketplaceId").alias("listing_marketplace_id")
                           ).withColumnRenamed("itemId", "item_id")

        data = data.withColumn("item_id", split(col("item_id"), "\\|")[1])
        join_item = data.join(items, on="item_id")
        res = join_item.join(sellers, on="seller_name")
        res = res.withColumn("date_key", lit(date_key))
        res = res.select("date_key", "item_sk", "seller_sk", "price", "top_rated_buying_experience",
                         "listing_marketplace_id")
        res = res.dropDuplicates()
        return res
