import os
import sys
import unittest

from utils.read_jdbc import ReadJDBC
from .pyspark_testcases import PySparkTestCase
from pyspark.testing.utils import assertDataFrameEqual
from unittest.mock import MagicMock, patch

os.environ["PYSPARK_PYTHON"] = sys.executable


class TestReadJDBC(PySparkTestCase):
    @patch("utils.read_jdbc.SparkSetup")
    @patch("utils.read_jdbc.SparkConf")
    def test_read_jdbc(self, mock_config, mock_spark_setup):
        url = "jdbc:ch://localhost:8123/warehouse"
        user = "test_user"
        password = "test_pass"
        driver = "org.postgresql.Driver"
        table = "test_table"
        jar = "path/to/jdbc.jar"

        mock_spark = MagicMock()
        mock_spark_setup.return_value.setupSpark.return_value = mock_spark
        mock_reader = MagicMock()
        mock_spark.read.format.return_value = mock_reader
        mock_reader.option.return_value = mock_reader
        mock_reader.load.return_value = "test_dataframe"
        jdbc_reader = ReadJDBC(url, user, password, driver, table, jar)
        res = jdbc_reader.read_jdbc()

        mock_spark.read.format.assert_called_with("jdbc")
        mock_reader.option.assert_any_call("driver", driver)
        mock_reader.option.assert_any_call("url", url)
        mock_reader.option.assert_any_call("user", user)
        mock_reader.option.assert_any_call("password", password)
        mock_reader.option.assert_any_call("query", f"SELECT * FROM {table}")
        mock_reader.load.assert_called_once()
        self.assertEqual(res, "test_dataframe")

if __name__ == '__main__':
    unittest.main()
