from interface.loader import Loader
from typing_extensions import override
from utils.spark_setup import SparkSetup


class LocalLoader(Loader):
    def __init__(self):
        super().__init__()

    @override
    def load(self, data):
        spark = SparkSetup("local_loader")

        df = data
        df.coalesce(1).write.option("header", True).option("nullValue", "N/A").csv(
            "/opt/app/src/data_test.csv", mode="overwrite"
        )

        spark.stop()
