from interface.transformer import Transformer
from utils.spark_setup import SparkSetup
from pyspark.conf import SparkConf
from pyspark.sql.functions import *

class CategoryTransform(Transformer):
    def transform(self, data):
        conf = SparkConf().setAppName("transform").setMaster("local[2]")
        spark = SparkSetup(conf).setupSpark()

        selected_df = data.select("categories")
        exploded_df = selected_df.withColumn("category", explode("categories")).drop("categories")
        categories_df = (exploded_df.withColumn("categoryId", col("category.categoryId"))
                         .withColumn("categoryName", col("category.categoryName"))
                         .drop("category"))
        return categories_df