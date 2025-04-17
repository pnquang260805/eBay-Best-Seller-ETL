from interface.transformer import Transformer
from utils.spark_setup import SparkSetup
from pyspark.conf import SparkConf
from pyspark.sql.functions import *


class EbayTransform(Transformer):

    def transform(self, data):
        conf = SparkConf().setAppName("Ebay_transform").setMaster("local[2]")

        res = data.json()
        itemSummaries = res.get("itemSummaries", [])

        sparkSetup = SparkSetup(conf)
        spark = sparkSetup.setupSpark()
        rdd = spark.sparkContext.parallelize(itemSummaries)
        df = spark.createDataFrame(rdd)
        df = df.drop(
            "additionalImages",
            "itemCreationDate",
            "image",
            "listingMarketplaceId",
            "thumbnailImages",
            "adultOnly",
            "topRatedBuyingExperience",
            "priorityListing",
            "itemHref",
            "seller",
            "shippingOptions",
            "leafCategoryIds",
            "itemWebUrl",
            "conditionId",
            "epid",
        )

        # df = df.withColumn("itemLocation", explode(col("itemLocation")))
        # df = df.withColumn(
        #     "itemLocation", col("itemLocation").getField("country")
        # ).withColumn("price", col("price.value"))
        df = df.withColumn(
            "itemLocation", col("itemLocation").getField("country")
        ).withColumn("price", col("price.value"))

        categories = df.select(
            "itemId", explode(col("categories")).alias("category")
        ).drop("categories")
        categories = categories.withColumn(
            "categoryName", col("category.categoryName")
        ).withColumn("categoryId", col("category.categoryId"))
        categories = categories.drop("categories", "category")

        df = df.drop("categories")

        categories = categories.groupBy("itemId").agg(
            collect_list("categoryName").alias("categoriesName"),
            collect_list("categoryId").alias("categoriesId"),
        )
        processed_data = df.join(categories, categories.itemId == df.itemId).drop(
            categories["itemId"]
        )

        processed_data = processed_data.fillna("NULL")
        print(processed_data.show(5))
        # spark.stop()

        return processed_data
