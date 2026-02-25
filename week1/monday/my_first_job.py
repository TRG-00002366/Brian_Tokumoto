from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as spark_sum

def main():

    # Step 1: Create SparkSession
    spark = SparkSession.builder \
        .appName("MyFirstJob") \
        .master("local[*]") \
        .getOrCreate()
    
    # Step 2: YOUR CODE HERE - Create some data

    data = [
        ("Laptop", "Electronics", 999.99, 5),
        ("Mouse", "Electronics", 29.99, 50),
        ("Desk", "Furniture", 199.99, 10),
        ("Chair", "Furniture", 149.99, 20),
        ("Monitor", "Electronics", 299.99, 15),
    ]

    columns = ["product", "category", "price", "quantity"]

    df = spark.createDataFrame(data, columns)

    # show the data
    df.show()
    
    # show total number of products
    total_products = df.count()
    print("Total number of products:", total_products, "\n")

    # show revenue per product
    df = df.withColumn("revenue", col("price") * col("quantity"))
    print("Revenue per product:")
    df.show()

    # filter electronics
    electronics_df = df.filter(col("category") == "Electronics")

    print("Electronics Only:")
    electronics_df.show()

    # aggregate revenue by category
    revenue_by_category = df.groupBy("category").agg(spark_sum("revenue"))

    print("Total Revenue by Category:")
    revenue_by_category.show()

    # Step 5: Clean up
    spark.stop()

if __name__ == "__main__":
    main()