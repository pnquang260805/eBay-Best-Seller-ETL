from interface.transformer import Transformer
from typing_extensions import override
from utils.spark_setup import SparkSetup


class EbayTransform(Transformer):
    def __init__(self):
        pass

    @override
    def transform(self, data):
        spark = SparkSetup("ebay_transformer")
        rdd = spark.sparkContext.parallelize([data])
        df = spark.read.json(rdd)
