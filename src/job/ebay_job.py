import sys

sys.path.append("/opt/app/src")
sys.path.append("./src")

from extract.ebay_extractor import EbayExtractor
from load.s3_loader import S3Loader
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
