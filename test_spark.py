from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("VSCodeTest") \
    .master("local[*]") \
    .getOrCreate()

print("Spark Version:", spark.version)

spark.range(5).show()

spark.stop()