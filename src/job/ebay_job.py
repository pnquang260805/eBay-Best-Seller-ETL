import sys

sys.path.append("/opt/app/src")
sys.path.append("./src")

from extract.ebay_extractor import EbayExtractor
from transform.ebay.seller_transform import SellerTransform
from transform.ebay.item_transformer import ItemTransform
from load.jdbc_loader import JDBCLoader

if __name__ == "__main__":
    print("=====================Extracting Data=====================")
    extractor = EbayExtractor()
    raw_data = extractor.extract()
    print("=========================================================")
    print("\n=====================Transforming Data=====================")
    seller_transform = SellerTransform().transform(raw_data)
    seller_transform.show(5)
    item_transform = ItemTransform().transform(raw_data)
    item_transform.show(5)
    print("=========================================================")
    print("\n=====================Load Data=====================")
    user = "admin"
    password = "26082005qa"
    driver = "com.clickhouse.jdbc.ClickHouseDriver"
    url = "jdbc:ch://clickhouse:8123/warehouse"
    seller_loader = JDBCLoader(user=user, password=password, driver=driver, table="dim_seller", url=url)
    seller_loader.load(seller_transform)
    seller_loader = JDBCLoader(user=user, password=password, driver=driver, table="dim_item", url=url)
    seller_loader.load(item_transform)