import os
import sys
import unittest

from transform.ebay.seller_transform import SellerTransform
from .pyspark_testcases import PySparkTestCase
from pyspark.testing.utils import assertDataFrameEqual

os.environ["PYSPARK_PYTHON"] = sys.executable


class TestSellerTransform(PySparkTestCase):
    def test_seller(self):
        schema = "sellerName STRING, feedbackPercentage FLOAT, feedbackScore INT"

        expected_data = [{
            "sellerName": "american-mall",
            "feedbackPercentage": 98.4,
            "feedbackScore": 9122
        }]
        expected_df = self.spark.createDataFrame(expected_data, schema=schema)
        raw_data = [{
            "itemId": "v1|155227403497|0",
            "title": "Devoted Creations GAME OVER Ultra-Dark Black .FREE SHIPPING!!!! BEST SELLER!!!!",
            "leafCategoryIds": [
                "31776"
            ],
            "categories": [
                {
                    "categoryId": "31776",
                    "categoryName": "Tanning Lotion"
                },
                {
                    "categoryId": "26395",
                    "categoryName": "Health & Beauty"
                },
                {
                    "categoryId": "31772",
                    "categoryName": "Sun Protection & Tanning"
                }
            ],
            "image": {
                "imageUrl": "https://i.ebayimg.com/images/g/iUsAAOSwMlNjWu0G/s-l225.jpg"
            },
            "price": {
                "value": "11.00",
                "currency": "USD"
            },
            "itemHref": "https://api.ebay.com/buy/browse/v1/item/v1%7C155227403497%7C0",
            "seller": {
                "username": "american-mall",
                "feedbackPercentage": "98.4",
                "feedbackScore": 9122
            },
            "condition": "New",
            "conditionId": "1000",
            "thumbnailImages": [
                {
                    "imageUrl": "https://i.ebayimg.com/images/g/iUsAAOSwMlNjWu0G/s-l1600.jpg"
                }
            ],
            "shippingOptions": [
                {
                    "shippingCostType": "FIXED",
                    "shippingCost": {
                        "value": "0.00",
                        "currency": "USD"
                    }
                }
            ],
            "buyingOptions": [
                "FIXED_PRICE",
                "BEST_OFFER"
            ],
            "itemAffiliateWebUrl": "https://www.ebay.com/itm/155227403497?_skw=best_seller&hash=item24244634e9%3Ag%3AiUsAAOSwMlNjWu0G&mkevt=1&mkcid=1&mkrid=711-53200-19255-0&campid=%253CePNCampaignId%253E&customid=%253CreferenceId%253E&toolid=10049",
            "itemWebUrl": "https://www.ebay.com/itm/155227403497?_skw=best_seller&hash=item24244634e9:g:iUsAAOSwMlNjWu0G",
            "itemLocation": {
                "postalCode": "112**",
                "country": "US"
            },
            "adultOnly": False,
            "legacyItemId": "155227403497",
            "availableCoupons": False,
            "itemOriginDate": "2022-10-27T20:42:56.000Z",
            "itemCreationDate": "2022-10-27T20:42:56.000Z",
            "topRatedBuyingExperience": False,
            "priorityListing": False,
            "listingMarketplaceId": "EBAY_US"
        }]
        data = self.spark.createDataFrame(raw_data)
        df = SellerTransform().transform(data)
        assertDataFrameEqual(df, expected_df)
        for col in df.columns:
            print(col, "\t", df.filter(df[col].isNull()).count())


if __name__ == "__main__":
    unittest.main()
