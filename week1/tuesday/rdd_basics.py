from pyspark import SparkContext

sc = SparkContext("local[*]", "RDDBasics")

# 1. Create RDD from a Python list
numbers = sc.parallelize([1, 2, 3, 4, 5, 6, 7, 8, 9, 10])
print(f"Numbers: {numbers.collect()}")
print(f"Partitions: {numbers.getNumPartitions()}")

# 2. Create RDD with explicit partitions
# YOUR CODE: Create the same list with exactly 4 partitions
numbers_4 = sc.parallelize([1,2,3,4,5,6,7,8,9,10], 4)
print("Partitions:", numbers_4.getNumPartitions())

# 3. Create RDD from a range
# YOUR CODE: Create RDD from range(1, 101)
range_rdd = sc.parallelize(range(1, 101))
print("Range count:", range_rdd.count())


# Given: numbers RDD [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]

# Task A: Square each number
# Expected: [1, 4, 9, 16, 25, 36, 49, 64, 81, 100]
squared = numbers.map(lambda x: x**2)
print("Squared:", squared.collect())

# Task B: Convert to strings with prefix
# Expected: ["num_1", "num_2", "num_3", ...]
prefixed = numbers.map(lambda x: f"num_{x}")
print("Prefixed:", prefixed.collect())

# Task A: Keep only even numbers
# Expected: [2, 4, 6, 8, 10]
evens = numbers.filter(lambda x: x%2 ==0)
print("Evens:", evens.collect())

# Task B: Keep numbers greater than 5
# Expected: [6, 7, 8, 9, 10]
greater_than_5 = numbers.filter(lambda x : x > 5)
print("Greater than 5:", greater_than_5.collect())

# Task C: Combine - even AND greater than 5
# Expected: [6, 8, 10]
combined = numbers.filter(lambda x: x%2 ==0 and x > 5)
print("Even and greater than 5:", combined.collect())


# Given sentences
sentences = sc.parallelize([
    "Hello World",
    "Apache Spark is Fast",
    "PySpark is Python plus Spark"
])

# Task A: Split into words (use flatMap)
# Expected: ["Hello", "World", "Apache", "Spark", ...]
words = sentences.flatMap(lambda x: x.split())
print("Words:", words.collect())

# Task B: Create pairs of (word, length)
# Expected: [("Hello", 5), ("World", 5), ...]
word_lengths = sentences.flatMap(lambda x: x.split()).map(lambda y: (y, len(y)))
print("Word Lengths:", word_lengths.collect())

# Given: log entries
logs = sc.parallelize([
    "INFO: User logged in",
    "ERROR: Connection failed",
    "INFO: Data processed",
    "ERROR: Timeout occurred",
    "DEBUG: Cache hit"
])

# Pipeline: Extract only ERROR messages, convert to uppercase words
# 1. Filter to keep only ERROR lines
# 2. Split each line into words
# 3. Convert each word to uppercase
# Expected: ["ERROR:", "CONNECTION", "FAILED", "ERROR:", "TIMEOUT", "OCCURRED"]
error_words = logs.filter(lambda x: "Error:" in x)
error_words = error_words.flatMap(lambda x: x.split())
error_words = error_words.map(lambda x: x.upper())
print("Error Words:", error_words.collect())


sc.stop()