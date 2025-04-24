import os
import sys
import unittest

from unittest.mock import MagicMock, patch
from src.test.pyspark_testcases import PySparkTestCase
from src.extract.ebay_extractor import EbayExtractor
from pyspark.testing.utils import assertDataFrameEqual

os.environ["PYSPARK_PYTHON"] = sys.executable

class TestEbayExtractor(PySparkTestCase):
    @patch("src.extract.ebay_extractor.requests.get")
    def test_ebay_extractor(self, mock_get):
        mock_response = MagicMock()
        mock_response.json.return_value = {
            "itemSummaries": [
                {"id": "1", "name": "item1"},
                {"id": "2", "name": "item2"},
            ]
        }

        mock_get.return_value = mock_response

        extractor = EbayExtractor()
        df = extractor.extract()

        expected_data = [{"id": "1", "name": "item1"},
                        {"id": "2", "name": "item2"}]

        expected_df = self.spark.createDataFrame(expected_data)
        assertDataFrameEqual(df, expected_df)

if __name__ == '__main__':
    unittest.main()