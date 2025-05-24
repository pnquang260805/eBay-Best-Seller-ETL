import sys
from os import truncate

from unicodedata import category

sys.path.append("/opt/app/src")
sys.path.append("./src")

from extract.ebay_extractor import EbayExtractor
from transform.ebay.seller_transform import SellerTransform
from transform.ebay.item_transformer import ItemTransform
from transform.ebay.category_transform import CategoryTransform
from transform.dim_date import DimDateCreation
from transform.ebay.item_category_bridge import ItemCategory
from transform.ebay.fact import FactTransform
from load.jdbc_loader import JDBCLoader

if __name__ == "__main__":
    extractor = EbayExtractor()
    raw_data = extractor.extract()

    seller_transform = SellerTransform().transform(raw_data)
    item_transform = ItemTransform().transform(raw_data)
    category_transform = CategoryTransform().transform(raw_data)
    date_creation = DimDateCreation().create_dim_date()
    bridge = ItemCategory().transform(raw_data)
    fact_df = FactTransform().transform(raw_data)

    user = "admin"
    password = "26082005qa"
    driver = "com.clickhouse.jdbc.ClickHouseDriver"
    url = "jdbc:ch://clickhouse:8123/warehouse"

    loader = JDBCLoader(user=user, password=password, driver=driver, url=url)

    loader.load("dim_seller",seller_transform)
    loader.load("dim_item", item_transform)
    loader.load("dim_category",category_transform)
    loader.load("dim_date", date_creation)
    loader.load("bridge_item_category", bridge)
    loader.load("eBay_best_seller_fact", fact_df)