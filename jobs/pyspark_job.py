from pyspark.sql import SparkSession


spark=SparkSession.builder.appName("Orders").getOrCreate()

data=spark.read.format("csv").load("/opt/bitnami/spark/data/orders.csv")
print(data.count())

spark.stop()