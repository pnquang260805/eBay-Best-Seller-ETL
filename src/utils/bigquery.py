from google.cloud import bigquery
from google.cloud.bigquery import dbapi

class BigQuery:
    @classmethod
    def setup_class(cls):
        cls.client = bigquery.Client()
        cls.connection = dbapi.Connection(cls.client)
        cls.cursor = cls.connection.cursor()

    @classmethod
    def teardown_class(cls):
        # bạn phải gọi cái này, nó không tự chạy đâu
        cls.client.close()

    def perform_query(self, query):
        self.cursor.execute(query)
        result = self.cursor.fetchall()
        return result