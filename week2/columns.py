from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,lit
)

spark = SparkSession.builder.appName("Exercise: Columns").master("local[*]").getOrCreate()

data = [
    (1, "  Alice Smith  ", "alice@company.com", 75000, None),

    (2, "  Bob Johnson  ", "bob.j@company.com", 65000, "NY"),

    (3, "  Charlie Brown  ", "charlie@company.com", 80000, "CA"),

    (4, "  Diana Prince  ", None, 70000, "TX")
]

df = spark.createDataFrame(data, ["id","name","email", "salary", "state"])
#df.show()


#df.withColumn("country", lit("USA")).show()
#df.withColumn("bonus", col("salary")*0.10).show()

df.withColumn("email", col("email").replace("@company.com","mycompany.com"))

df.withColumn()

spark.stop()