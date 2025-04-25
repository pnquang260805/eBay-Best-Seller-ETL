import requests
import os

from utils.spark_setup import SparkSetup
from utils.logger import Logging
from interface.extractor import Extractor

from pyspark.conf import SparkConf
from typing_extensions import override
from dotenv import load_dotenv


load_dotenv()
logger = Logging()


class EbayExtractor(Extractor):

    def __init__(self):
        super().__init__()
        self.token = os.getenv("EBAY_TOKEN")
        self.market_place = "EBAY_US"
        conf = SparkConf().setAppName("eBay_extractor").setMaster("local[2]")
        setup = SparkSetup(conf)
        self.spark = setup.setupSpark()

    @logger.log  # = log.get(log).
    @override
    def extract(self):
        """
        Return a Pyspark DataFrame
        """
        url = "https://api.ebay.com/buy/browse/v1/item_summary/search?q=best_seller&limit=200"
        header = {
            "Authorization": "Bearer " + self.token,
            "X-EBAY-C-MARKETPLACE-ID": self.market_place,
        }
        res = requests.get(url, headers=header)
        data = res.json()
        item_summaries = data.get("itemSummaries", [])
        rdd = self.spark.sparkContext.parallelize(item_summaries)
        df = self.spark.createDataFrame(rdd)
        return df
