"""
Exercise: Joins
===============
Week 2, Tuesday

Practice all join types with customer and order data.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, broadcast

# =============================================================================
# SETUP - Do not modify
# =============================================================================

spark = SparkSession.builder.appName("Exercise: Joins").master("local[*]").getOrCreate()

# Customers
customers = spark.createDataFrame([
    (1, "Alice", "alice@email.com", "NY"),
    (2, "Bob", "bob@email.com", "CA"),
    (3, "Charlie", "charlie@email.com", "TX"),
    (4, "Diana", "diana@email.com", "FL"),
    (5, "Eve", "eve@email.com", "WA")
], ["customer_id", "name", "email", "state"])

# Orders
orders = spark.createDataFrame([
    (101, 1, "2023-01-15", 150.00),
    (102, 2, "2023-01-16", 200.00),
    (103, 1, "2023-01-17", 75.00),
    (104, 3, "2023-01-18", 300.00),
    (105, 6, "2023-01-19", 125.00),  # customer_id 6 does not exist!
    (106, 2, "2023-01-20", 180.00)
], ["order_id", "customer_id", "order_date", "amount"])

# Products (for multi-table join)
products = spark.createDataFrame([
    (101, "Laptop"),
    (102, "Phone"),
    (103, "Mouse"),
    (104, "Keyboard"),
    (107, "Monitor")  # Not in any order!
], ["order_id", "product_name"])

print("=== Exercise: Joins ===")
print("\nCustomers:")
customers.show()
print("Orders:")
orders.show()
print("Products:")
products.show()

# =============================================================================
# TASK 1: Inner Join (15 mins)
# =============================================================================

print("\n--- Task 1: Inner Join ---")

# TODO 1a: Join customers and orders (only matching records)
# Show customer name, order_id, order_date, amount
customers.join(orders, on="customer_id").show()

# TODO 1b: How many orders have matching customers?
# HINT: Compare this count to total orders
matching_orders = customers.join(orders, on="customer_id", how="inner").count()
total_orders = orders.count()
print(f"matching orders: {matching_orders}, total orders: {total_orders}")

# =============================================================================
# TASK 2: Left and Right Joins (20 mins)
# =============================================================================

print("\n--- Task 2: Left and Right Joins ---")

# TODO 2a: LEFT JOIN - All customers, with order info where available
# Who has NOT placed any orders?
customers.join(orders, how="left", on = "customer_id").show()

# TODO 2b: RIGHT JOIN - All orders, with customer info where available
# Which order has no matching customer?
customers.join(orders, how="right", on = "customer_id").show()

# TODO 2c: What is the difference between the two results?
# Answer in a comment:
#
# match all customers using order_id in customer table giving all orders that match.
# The second one matches orders with customers but because one of the orders has a
# customer id that doesnt exist, it gets filled with null because it has no match.

# =============================================================================
# TASK 3: Full Outer Join (10 mins)
# =============================================================================

print("\n--- Task 3: Full Outer Join ---")

# TODO 3a: Perform a FULL OUTER join between customers and orders
# All customers AND all orders should appear
customers.join(orders, how="outer", on= "customer_id").show()

# TODO 3b: Filter to show only rows where there is a mismatch
# (customer without order OR order without customer)
customers.join(orders, how="outer", on= "customer_id").filter(col("name").isNull() | col("order_id").isNull()).show()

# =============================================================================
# TASK 4: Semi and Anti Joins (15 mins)
# =============================================================================

print("\n--- Task 4: Semi and Anti Joins ---")

# TODO 4a: LEFT SEMI JOIN - Customers who HAVE placed orders
# Only customer columns should appear
customers.join(orders, how="left_semi", on="customer_id").show()

# TODO 4b: LEFT ANTI JOIN - Customers who have NOT placed orders
customers.join(orders, how="left_anti", on="customer_id").show()

# TODO 4c: When would you use anti join in real data work?
# Answer in a comment:
#
# You would use them to answer quesitons about missing data since
# it gives records that do not havea a match in the other table.

# =============================================================================
# TASK 5: Handling Duplicate Columns (15 mins)
# =============================================================================

print("\n--- Task 5: Handling Duplicate Columns ---")

# After joining customers and orders, both have customer_id

# TODO 5a: Join and then DROP the duplicate customer_id column
customers.join(orders, on="customer_id").drop(orders.customer_id).show()

# TODO 5b: Alternative: Use aliases to reference specific columns
# HINT: customers.alias("c"), orders.alias("o")
customers.alias("c").join(orders.alias("o"), col("c.customer_id") == col("o.customer_id")).select("c.name", "o.order_id", "o.amount").show()

# =============================================================================
# TASK 6: Multi-Table Join (15 mins)
# =============================================================================

print("\n--- Task 6: Multi-Table Join ---")

# TODO 6a: Join customers -> orders -> products
# Show: customer name, order_id, amount, product_name


# TODO 6b: What kind of join should you use when some orders might not have products?


# =============================================================================
# CHALLENGE: Real-World Scenarios (20 mins)
# =============================================================================

print("\n--- Challenge: Real-World Scenarios ---")

# TODO 7a: Find the total spending per customer (only customers with orders)
# Use join + groupBy + sum


# TODO 7b: Find customers from CA who placed orders > $150


# TODO 7c: Find orders without valid product information
# (anti join pattern)


# =============================================================================
# CLEANUP
# =============================================================================

spark.stop()