import os
import sys
import unittest

from transform.ebay.category_transform import CategoryTransform
from .pyspark_testcases import PySparkTestCase
from pyspark.testing.utils import assertDataFrameEqual
from pyspark.sql.types import StructType, StructField, StringType, BooleanType

os.environ["PYSPARK_PYTHON"] = sys.executable


class TestCategoryTransform(PySparkTestCase):
    def test_category_transform(self):
        schema = StructType([
            StructField("categoryId", StringType(), False),
            StructField("categoryName", StringType(), False)
        ])

        expected_data = [{
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
            }]
        expected_df = self.spark.createDataFrame(expected_data, schema=schema)

        data = [{
            "itemId": "v1|1233456|0",
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
                "imageUrl": "Link"
            },
            "price": {
                "value": "11.00",
                "currency": "USD"
            },
            "itemHref": "Link",
            "seller": {
                "username": "mall",
                "feedbackPercentage": "98.4",
                "feedbackScore": 9122
            },
            "condition": "New",
            "conditionId": "1000",
            "thumbnailImages": [
                {
                    "imageUrl": "Link"
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
            "itemAffiliateWebUrl": "Link",
            "itemWebUrl": "Link",
            "itemLocation": {
                "postalCode": "112**",
                "country": "US"
            },
            "adultOnly": False,
            "legacyItemId": "ID",
            "availableCoupons": False,
            "itemOriginDate": "2022-10-27T20:42:56.000Z",
            "itemCreationDate": "2022-10-27T20:42:56.000Z",
            "topRatedBuyingExperience": False,
            "priorityListing": False,
            "listingMarketplaceId": "EBAY_US"
        }]
        raw_df = self.spark.createDataFrame(data)
        df = CategoryTransform().transform(raw_df)
        assertDataFrameEqual(df, expected_df)

if __name__ == "__main__":
    unittest.main()