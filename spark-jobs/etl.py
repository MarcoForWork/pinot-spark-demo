from pyspark.sql import SparkSession
from pyspark.sql.functions import col, unix_timestamp, hour, dayofweek, month

# Khởi tạo Spark Session
spark = SparkSession.builder \
    .appName("NYC Taxi ETL for Pinot") \
    .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

print("=" * 50)
print("Đọc file NYC Taxi CSV...")
print("=" * 50)

# Đọc CSV
df = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv("/opt/spark/data/yellow_tripdata_2016-03.csv")

print(f"Tổng số dòng RAW: {df.count():,}")
df.printSchema()

print("\nBắt đầu ETL - Làm sạch dữ liệu...")

# Làm sạch & transform
df_clean = df \
    .filter(col("trip_distance") > 0) \
    .filter(col("total_amount") > 0) \
    .filter(col("passenger_count") > 0) \
    .filter(col("tpep_pickup_datetime").isNotNull()) \
    .withColumn(
        "tpep_pickup_datetime",
        (unix_timestamp(col("tpep_pickup_datetime"),
         "yyyy-MM-dd HH:mm:ss") * 1000).cast("long")
    ) \
    .withColumn(
        "tpep_dropoff_datetime",
        (unix_timestamp(col("tpep_dropoff_datetime"),
         "yyyy-MM-dd HH:mm:ss") * 1000).cast("long")
    ) \
    .withColumn("pickup_hour", hour(
        (col("tpep_pickup_datetime") / 1000).cast("timestamp"))
    ) \
    .withColumn("pickup_dayofweek", dayofweek(
        (col("tpep_pickup_datetime") / 1000).cast("timestamp"))
    ) \
    .select(
        col("VendorID").cast("int"),
        col("tpep_pickup_datetime").cast("long"),
        col("tpep_dropoff_datetime").cast("long"),
        col("passenger_count").cast("int"),
        col("trip_distance").cast("float"),
        col("RatecodeID").cast("int"),
        col("store_and_fwd_flag").cast("string"),
        col("payment_type").cast("int"),
        col("fare_amount").cast("float"),
        col("extra").cast("float"),
        col("mta_tax").cast("float"),
        col("tip_amount").cast("float"),
        col("tolls_amount").cast("float"),
        col("improvement_surcharge").cast("float"),
        col("total_amount").cast("float"),
        col("pickup_hour").cast("int"),
        col("pickup_dayofweek").cast("int")
    )

print(f"Tổng số dòng sau khi làm sạch: {df_clean.count():,}")

print("\nThống kê nhanh:")
df_clean.groupBy("VendorID") \
    .count() \
    .orderBy("VendorID") \
    .show()

print("\nXuất ra Parquet...")
df_clean.coalesce(1) \
    .write \
    .mode("overwrite") \
    .parquet("/opt/spark/data/nyc_taxi_clean")

print("=" * 50)
print("ETL hoàn thành! Data đã được lưu vào /data/nyc_taxi_clean/")
print("=" * 50)

spark.stop()