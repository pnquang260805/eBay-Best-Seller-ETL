from interface.transformer import Transformer
from utils.spark_setup import SparkSetup
from pyspark.conf import SparkConf
from pyspark.sql.functions import *
from utils.read_jdbc import ReadJDBC
from utils.logger import Logging

logger = Logging()

class CategoryTransform(Transformer):
    @logger.log
    def transform(self, data):
        conf = SparkConf().setAppName("transform").setMaster("local[2]")
        spark = SparkSetup(conf).setupSpark()

        username = "admin"
        password = "26082005qa"
        table = "dim_category"

        reader = ReadJDBC(username, password)
        df = reader.read_jdbc(table)
        lastest_sk = df.select(max(col("category_sk"))).collect()[0][0]
        df = df.select("category_id")
        if lastest_sk is None:
            lastest_sk = 0

        columns_name = {
            "categoryId": "category_id",
            "categoryName": "category_name"
        }

        selected_df = data.select("categories")
        exploded_df = selected_df.withColumn("category", explode("categories")).drop("categories")
        categories_df = (exploded_df.withColumn("categoryId", col("category.categoryId"))
                         .withColumn("categoryName", col("category.categoryName"))
                         .drop("category"))
        for old_name, new_name in columns_name.items():
            categories_df = categories_df.withColumnRenamed(old_name, new_name)

        res = categories_df.join(df, on="category_id", how="left_anti")
        res = res.dropDuplicates()
        res = res.withColumn("category_sk", monotonically_increasing_id() + lastest_sk + 1)

        return res