from pyspark.sql import SparkSession

# Create SparkSession (recommended entry point)
spark = SparkSession.builder \
    .appName("HybridApp") \
    .getOrCreate()

# For DataFrame operations, use spark
df = spark.createDataFrame([(1, "Alice"), (2, "Bob"), (3, "Charlie")], ["id", "name"])

# For RDD operations, access SparkContext
sc = spark.sparkContext
rdd = sc.parallelize([1, 2, 3])

# You can convert between RDD and DataFrame
df_from_rdd = rdd.map(lambda x: (x,)).toDF(["value"])
rdd_from_df = df.rdd

df_from_rdd.show()
print(rdd_from_df.collect())