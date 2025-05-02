from datetime import date
from utils.spark_setup import SparkSetup
from pyspark.conf import SparkConf

class DimDateCreation:
    def create_dim_date(self):
        key_date = date.today().strftime("%Y%m%d")
        year = date.today().year
        month = date.today().month
        day = date.today().day
        day_of_week = date.today().strftime("%A")
        print(day_of_week)
        quarter = (date.today().month - 1) // 3 + 1

        conf = SparkConf().setAppName("create-dim-date").setMaster("local[2]")
        spark = SparkSetup(conf).setupSpark()
        schema = "key_date STRING, year INT, month INT, day INT, day_of_week STRING, quarter INT"
        date_data = [(key_date, year, month, day, day_of_week, quarter)]
        date_df = spark.createDataFrame(date_data, schema=schema)
        return date_df