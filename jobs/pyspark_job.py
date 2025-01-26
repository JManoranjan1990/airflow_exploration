from pyspark.sql import SparkSession


spark=SparkSession.builder.appName("test")\
    .config("spark.jars","opt/bitnami/spark/data/postgresql-42.4.4.jar")\
    .config("spark.driver.extraClassPath", "/opt/bitnami/spark/data/postgresql-42.4.4.jar") \
    .config("spark.executor.extraClassPath", "/opt/bitnami/spark/data/postgresql-42.4.4.jar") \
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")\
    .master("local[*]") \
    .getOrCreate()

data=spark.read.format("parquet").load("/opt/bitnami/spark/data/Newyork_taxidriver/Janurary/yellow_tripdata_2024-01.parquet")
print(data.count())
print(data.printSchema())

spark.stop()