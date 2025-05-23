from interface.transformer import Transformer
from pyspark.sql.functions import *
from utils.read_jdbc import ReadJDBC
from utils.logger import Logging

logger = Logging()

class ItemCategory(Transformer):
    @logger.log
    def transform(self, data):
        username = "admin"
        password = "26082005qa"
        category_table = "dim_category"
        item_table = "dim_item"
        bridge_table = "bridge_item_category"

        category_df = ReadJDBC(username, password, category_table).read_jdbc()
        item_df = ReadJDBC(username, password, item_table).read_jdbc()
        jdbc_bridge_df = ReadJDBC(username, password, bridge_table).read_jdbc()

        category_df = category_df.select("category_sk", "category_id")
        item_df = item_df.select("item_sk", "item_id")

        raw_df = data.select("itemId", "categories")
        df = raw_df.withColumn("itemId", split(col("itemId"), "\\|")[1])
        df = df.withColumn("category", explode("categories")).drop("categories")
        df = df.withColumn("categoryId", col("category.categoryId")).drop("category")
        df = df.withColumnRenamed("categoryId", "category_id") \
                .withColumnRenamed("itemId", "item_id")
        join_df = df.join(category_df, on="category_id")
        join_df = join_df.join(item_df, on="item_id")
        join_df = join_df.drop("category_id", "item_id")
        join_df = join_df.dropDuplicates()

        res = join_df.join(jdbc_bridge_df.select("item_sk", "category_sk"),
                           on=["item_sk", "category_sk"],
                           how="left_anti")  # Lọc ra các bộ (dòng) mà join_df (dữ liệu mới) có mà dữ liệu cũ ko có
        return res