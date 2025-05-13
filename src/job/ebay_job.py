import sys

sys.path.append("/opt/app/src")
sys.path.append("./src")

from extract.ebay_extractor import EbayExtractor
from transform.ebay.seller_transform import SellerTransform
from load.jdbc_loader import JDBCLoader

if __name__ == "__main__":
    print("=====================Extracting Data=====================")
    extractor = EbayExtractor()
    raw_data = extractor.extract()
    print("=========================================================")
    print("\n=====================Transforming Data=====================")
    seller_transform = SellerTransform().transform(raw_data)
    seller_transform.show(5)
    print("=========================================================")
    print("\n=====================Load Data=====================")
    user = "admin"
    password = "26082005qa"
    driver = "com.clickhouse.jdbc.ClickHouseDriver"
    table = "dim_seller"
    url = "jdbc:ch://clickhouse:8123/warehouse"
    loader = JDBCLoader(user=user, password=password, driver=driver, table=table, url=url)
    loader.load(seller_transform)