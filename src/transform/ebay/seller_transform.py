from interface.transformer import Transformer
from pyspark.sql.functions import *
from pyspark.sql.types import *
from utils.read_jdbc import ReadJDBC
from pyspark.sql.window import Window
from utils.logger import Logging

logger = Logging()

class SellerTransform(Transformer):
    @logger.log
    def transform(self, data):
        username = "admin"
        password = "26082005qa"
        table = "dim_seller"

        df = ReadJDBC(username, password, table).read_jdbc()
        lastest_sk = df.select(max(col("seller_sk"))).collect()[0][0]
        if lastest_sk is None:
            lastest_sk = 1

        seller_df = data.select("seller")
        seller_df = (seller_df.withColumn("seller_name", col("seller.username"))
                     .withColumn("feedback_percentage", col("seller.feedbackPercentage"))
                     .withColumn("feedback_score", col("seller.feedbackScore"))
                     .drop("seller"))
        seller_df = seller_df.dropDuplicates()
        seller_df = seller_df.withColumn("seller_sk", monotonically_increasing_id() + lastest_sk)
        return_df = seller_df.withColumn("feedback_percentage", col("feedback_percentage").cast(FloatType())).withColumn("feedback_score", col("feedback_score").cast(IntegerType()))

        return return_df