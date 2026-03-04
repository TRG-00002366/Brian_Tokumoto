import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,lit, regexp_replace
)

def clear_screen():
    """Clears the terminal screen."""
    # Check the operating system name
    if os.name == 'nt':
        # Command for Windows
        _ = os.system('cls')
    else:
        # Command for Linux/macOS (posix, etc.)
        _ = os.system('clear')
 
clear_screen()
 
# Set up
spark=SparkSession.builder.appName("Demo Column Mamangement").master("local[*]").getOrCreate()
 
# Sample data

data = [

    (1, "Alice", 34, "Engineering", 75000, "NY"),

    (2, "Bob", 45, "Marketing", 65000, "CA"),

    (3, "Charlie", 29, "Engineering", 80000, "NY"),

    (4, "Diana", 31, None, 55000, "TX"),

    (5, "Eve", 38, "Marketing", None, "CA")

]

df = spark.createDataFrame(data, ["id","name", "age", "job", "salary", "state"])