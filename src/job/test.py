from pyspark.sql import SparkSession

if __name__ == "__main__":
    # Tạo Spark session
    spark = (SparkSession.builder
        .appName("MySparkApp")
        .master("local[2]") # để 2 threads (cho đồng bộ với spark-worker core = 2)
        .getOrCreate())

    # Ví dụ: đọc dữ liệu từ file CSV và tính toán đơn giản
    print("====================Start reading kaggle file with local 2 ==================================")
    df = spark.read.csv("/opt/app/data/kaggle.csv", header=True, inferSchema=True).repartition(4)
    print("====================End reading file==================================")
    print(df.count())

    spark.stop()