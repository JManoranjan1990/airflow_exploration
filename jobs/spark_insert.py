from pyspark.sql import SparkSession
import os
import sys
from newyorktaxi import schema
from pyspark.sql.functions import current_timestamp,input_file_name,split,col,element_at



spark=SparkSession.builder.appName("TRIP_DATA_DATABASE")\
    .config("spark.jars","opt/bitnami/spark/data/postgresql-42.4.4.jar")\
    .config("spark.driver.extraClassPath", "/opt/bitnami/spark/data/postgresql-42.4.4.jar") \
    .config("spark.executor.extraClassPath", "/opt/bitnami/spark/data/postgresql-42.4.4.jar") \
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")\
    .master("local[*]") \
    .getOrCreate()
    # .master("spark://spark-master:7077")\

url = "jdbc:postgresql://postgres_db_service:5432/transaction_records"
properties = {
    "user": "Mano",
    "password": "Mano",
    "driver": "org.postgresql.Driver"
}

# print(schema.schema["yellow_tripdata"])
file_path = sys.argv[1]
file = sys.argv[2]
print(f"printing file path is {file_path}")
print(f"printing tablename {file}")
data=spark\
    .read\
    .format("parquet")\
    .load(file_path+"/*.parquet")

data1 = data.withColumn("inserted_date", current_timestamp()) \
          .withColumn("file_name", input_file_name()) \
          .withColumn("file_name", split(col("file_name"), "/"))\
          .withColumn("file_name", element_at("file_name", -1))

data1.write.jdbc(url=url, table=file, mode="append", properties=properties)
print(data1.head(1))



# spark.stop()







