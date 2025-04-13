import sys

sys.path.append("/opt/app/src")

from extract.ebay_extractor import EbayExtractor
from load.local_loader import LocalLoader
from transform.ebay_transformer import EbayTransform

if __name__ == "__main__":
    print("=====================Extracting Data=====================")
    extractor = EbayExtractor()
    raw_data = extractor.extract()
    print("=========================================================")

    print("\n=====================Transforming Data=====================")
    transformer = EbayTransform()
    data = transformer.transform(raw_data)
    print("=========================================================")

    print("\n=====================Loading Data=====================")
    loader = LocalLoader()
    loader.load(data)
    print("=========================================================")
