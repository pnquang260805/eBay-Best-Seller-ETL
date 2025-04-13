import requests
import os

from typing_extensions import override
from dotenv import load_dotenv
from interface.extractor import Extractor

load_dotenv()


class EbayExtractor(Extractor):

    def __init__(self):
        self.token = os.getenv("EBAY_TOKEN")
        self.market_place = "EBAY_US"

    @override
    def extract(self):
        url = "https://api.ebay.com/buy/browse/v1/item_summary/search?q=best_seller"
        header = {
            "Authorization": "Bearer " + self.token,
            "X-EBAY-C-MARKETPLACE-ID": self.market_place,
        }
        res = requests.get(url, headers=header)
        return res
