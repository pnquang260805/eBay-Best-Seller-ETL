from interface.transformer import Transformer
from pyspark.sql.functions import *

class ItemCategory(Transformer):
    def transform(self, data):
        raw_df = data.select("itemId", "categories")
        df = raw_df.withColumn("itemId", split(col("itemId"), "\\|")[1])
        df = df.withColumn("category", explode("categories")).drop("categories")
        df = df.withColumn("categoryId", col("category.categoryId")).drop("category")
        df = df.dropDuplicates()
        return df