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
from load.jdbc_loader import JDBCLoader

if __name__ == "__main__":
    print("=====================Extracting Data=====================")
    extractor = EbayExtractor()
    raw_data = extractor.extract()
    print("=========================================================")
    print("\n=====================Transforming Data=====================")
    seller_transform = SellerTransform().transform(raw_data)

    item_transform = ItemTransform().transform(raw_data)

    category_transform = CategoryTransform().transform(raw_data)

    date_creation = DimDateCreation().create_dim_date()

    bridge = ItemCategory().transform(raw_data)

    print("=========================================================")
    print("\n=====================Load Data=====================")
    user = "admin"
    password = "26082005qa"
    driver = "com.clickhouse.jdbc.ClickHouseDriver"
    url = "jdbc:ch://clickhouse:8123/warehouse"

    seller_loader = JDBCLoader(user=user, password=password, driver=driver, table="dim_seller", url=url)
    seller_loader.load(seller_transform)

    item_loader = JDBCLoader(user=user, password=password, driver=driver, table="dim_item", url=url)
    item_loader.load(item_transform)

    category_loader = JDBCLoader(user=user, password=password, driver=driver, table="dim_category", url=url)
    category_loader.load(category_transform)

    date_loader = JDBCLoader(user=user, password=password, driver=driver, table="dim_date", url=url)
    date_loader.load(date_creation)

    bridge_loader = JDBCLoader(user=user, password=password, driver=driver, table="bridge_item_category", url=url)
    bridge_loader.load(bridge)