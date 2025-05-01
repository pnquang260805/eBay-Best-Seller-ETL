from interface.transformer import Transformer
from pyspark.sql.functions import *
from pyspark.sql.types import *

class SellerTransform(Transformer):
    def transform(self, data):
        seller_df = data.select("seller")
        seller_df = (seller_df.withColumn("sellerName", col("seller.username"))
                     .withColumn("feedbackPercentage", col("seller.feedbackPercentage"))
                     .withColumn("feedbackScore", col("seller.feedbackScore"))
                     .drop("seller"))
        return_df = seller_df.withColumn("feedbackPercentage", col("feedbackPercentage").cast(FloatType())).withColumn("feedbackScore", col("feedbackScore").cast(IntegerType()))

        return return_df