import os
import sys
import unittest

from transform.item_transformer import ItemTransform
from .pyspark_testcases import PySparkTestCase
from pyspark.testing.utils import assertDataFrameEqual
from pyspark.sql.types import StructType, StructField, StringType, BooleanType

os.environ["PYSPARK_PYTHON"] = sys.executable


class TestTransform(PySparkTestCase):
    def test_transform(self):
        schema = StructType([
            StructField("itemId", StringType(), True),
            StructField("title", StringType(), True),
            StructField("condition", StringType(), True),
            StructField("adultOnly", BooleanType(), True),
            StructField("itemLocation", StringType(), True),
            StructField("itemOriginDate", StringType(), True),
            StructField("itemCreationDate", StringType(), True),
        ])
        expected_data = [{"itemId": '1233456',
                          "title": 'Devoted Creations GAME OVER Ultra-Dark Black .FREE SHIPPING!!!! BEST SELLER!!!!',
                          "condition": 'New', "adultOnly": False, "itemLocation": 'US',
                          "itemOriginDate": "2022-10-27T20:42:56.000Z", "itemCreationDate": "2022-10-27T20:42:56.000Z"}]

        expected_df = self.spark.createDataFrame(expected_data, schema=schema)

        raw_data = [{
            "itemId": "v1|1233456|0",
            "title": "Devoted Creations GAME OVER Ultra-Dark Black .FREE SHIPPING!!!! BEST SELLER!!!!",
            "leafCategoryIds": [
                "31776"
            ],
            "categories": [
                {
                    "categoryId": "1",
                    "categoryName": "Tanning Lotion"
                },
                {
                    "categoryId": "2",
                    "categoryName": "Health & Beauty"
                },
                {
                    "categoryId": "3",
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
        raw_df = self.spark.createDataFrame(raw_data)
        transformer = ItemTransform()
        transformed_df = transformer.transform(raw_df)
        assertDataFrameEqual(transformed_df, expected_df)


if __name__ == '__main__':
    unittest.main()
