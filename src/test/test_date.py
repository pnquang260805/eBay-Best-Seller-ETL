import os
import sys
import unittest

from .pyspark_testcases import PySparkTestCase
from transform.dim_date import DimDateCreation
from pyspark.testing.utils import assertDataFrameEqual
from unittest.mock import MagicMock, patch
from datetime import date

os.environ["PYSPARK_PYTHON"] = sys.executable

class TestDimDateCreation(PySparkTestCase):
    @patch("transform.dim_date.date")
    def test_dim_date(self, mock_date):
        mock_date.today.return_value = date(2025, 5, 1)
        expected_data = [("20250501", 2025, 5, 1, "Thursday", 2)]
        schema = "key_date STRING, year INT, month INT, day INT, day_of_week STRING, quarter INT"
        expected_df = self.spark.createDataFrame(expected_data, schema=schema)

        df = DimDateCreation().create_dim_date()
        assertDataFrameEqual(df, expected_df)

if __name__ == "__main__":
    unittest.main()