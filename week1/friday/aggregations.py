"""
Exercise: Aggregations
======================
Week 2, Tuesday

Practice groupBy and aggregate functions on sales data.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, avg, count, min, max, countDistinct

# =============================================================================
# SETUP - Do not modify
# =============================================================================

spark = SparkSession.builder.appName("Exercise: Aggregations").master("local[*]").getOrCreate()

# Sample sales data
sales = [
    ("2023-01", "Electronics", "Laptop", 1200, "Alice"),
    ("2023-01", "Electronics", "Phone", 800, "Bob"),
    ("2023-01", "Electronics", "Tablet", 500, "Alice"),
    ("2023-01", "Clothing", "Jacket", 150, "Charlie"),
    ("2023-01", "Clothing", "Shoes", 100, "Diana"),
    ("2023-02", "Electronics", "Laptop", 1300, "Eve"),
    ("2023-02", "Electronics", "Phone", 850, "Alice"),
    ("2023-02", "Clothing", "Jacket", 175, "Bob"),
    ("2023-02", "Clothing", "Pants", 80, "Charlie"),
    ("2023-03", "Electronics", "Laptop", 1100, "Frank"),
    ("2023-03", "Electronics", "Phone", 750, "Grace"),
    ("2023-03", "Clothing", "Shoes", 120, "Alice")
]

df = spark.createDataFrame(sales, ["month", "category", "product", "amount", "salesperson"])

print("=== Exercise: Aggregations ===")
print("\nSales Data:")
df.show()

# =============================================================================
# TASK 1: Simple Aggregations (15 mins)
# =============================================================================

print("\n--- Task 1: Simple Aggregations ---")

# TODO 1a: Calculate total, average, min, and max amount across ALL sales
# Use agg() without groupBy
df.agg(sum("amount").alias("total_sales"), avg("amount").alias("average_sale"), min("amount").alias("min_sale"), max("amount").alias("max_sale")).show()


# TODO 1b: Count the total number of sales transactions
df.agg(count("*")).show()

# TODO 1c: Count distinct categories
df.agg(countDistinct("category")).show()


# =============================================================================
# TASK 2: GroupBy with Single Aggregation (15 mins)
# =============================================================================

print("\n--- Task 2: GroupBy Single Aggregation ---")

# TODO 2a: Total sales amount by category
df.groupBy("category").agg(sum("amount")).show()


# TODO 2b: Average sale amount by month
df.groupBy("month").agg(avg("amount")).show()


# TODO 2c: Count of transactions by salesperson
df.groupBy("salesperson").agg(count("*")).show()


# =============================================================================
# TASK 3: GroupBy with Multiple Aggregations (20 mins)
# =============================================================================

print("\n--- Task 3: GroupBy Multiple Aggregations ---")

# TODO 3a: For each category, calculate:
# - Number of transactions
# - Total revenue
# - Average sale amount
# - Highest single sale
# Use meaningful aliases!

df.groupBy("category").agg( count("*").alias("transaction_count"),
    sum("amount").alias("total_revenue"),
    avg("amount").alias("avg_sale_amount"),
    max("amount").alias("highest_single_sale"),
).show()



# TODO 3b: For each salesperson, calculate:
# - Number of sales
# - Total revenue
# - Distinct products sold (countDistinct)

df.groupBy("salesperson").agg(
    count("*").alias("total_sales"),
    sum("amount").alias("total_revenue"),
    countDistinct("product").alias("distinct_prod_sold")
).show()


# =============================================================================
# TASK 4: Multi-Column GroupBy (15 mins)
# =============================================================================

print("\n--- Task 4: Multi-Column GroupBy ---")

# TODO 4a: Calculate total sales by month AND category
df.groupBy("month", "category").agg(sum("amount").alias("total_sales")).show()


# TODO 4b: Find the top salesperson by month (hint: use multi-column groupBy)
# im stuck with this one
df.groupBy("month", "salesperson").agg(sum("amount").alias("total_sales")).groupBy("month").agg(max("total_sales")).show()



# =============================================================================
# TASK 5: Filtering After Aggregation (15 mins)
# =============================================================================

print("\n--- Task 5: Filtering After Aggregation ---")

# TODO 5a: Find categories with total revenue > 2000
df.groupBy("category").agg(sum("amount").alias("total_revenue")).filter(col("total_revenue") > 2000).show()

# TODO 5b: Find salespeople who made more than 2 transactions
df.groupBy("salesperson").agg(count("*").alias("count_transactions")).filter(col("count_transactions") > 2).show()

# TODO 5c: Find month-category combinations with average sale > 500
df.groupBy("month", "category").agg(avg("amount").alias("avg_sale")).filter(col("avg_sale") > 500).show()

# =============================================================================
# CHALLENGE: Business Questions (20 mins)
# =============================================================================

print("\n--- Challenge: Business Questions ---")

# TODO 6a: Which category had the highest average transaction value?


# TODO 6b: Who is the top salesperson by total revenue?


# TODO 6c: Which month had the most diverse products sold?
# HINT: Use countDistinct on product column


# =============================================================================
# CLEANUP
# =============================================================================

spark.stop()