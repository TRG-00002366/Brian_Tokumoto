from pyspark.sql import SparkSession
from pathlib import Path
import re
from typing import Iterable, Tuple


# helpers

STOPWORDS = {
    "the", "a", "an", "is", "are", "was", "were", "and", "or", "but", "to",
    "of", "in", "that", "it"
}

TITLE_START = "A SCANDAL IN BOHEMIA"
TITLE_END = "THE RED-HEADED LEAGUE"

NON_LETTER = re.compile(r"[^a-z\s]+") # letters n whitespace

NON_SENTENCE_CHARS = re.compile(r"[^a-z\s.!?]+") # keep punctuation

SENTENCE_SPLIT = re.compile(r"[.!?]+")

def normalize_for_words(line: str) -> str:
    lowered = line.lower()
    cleaned = NON_LETTER.sub(" ", lowered)
    return cleaned


def normalize_for_sentences(line: str) -> str:
    lowered = line.lower()
    cleaned = NON_SENTENCE_CHARS.sub(" ", lowered)
    return cleaned

def split_into_sentences(line: str) -> list[str]:
    lowered = line.lower()
    cleaned = NON_SENTENCE_CHARS.sub(" ", lowered)
    parts = SENTENCE_SPLIT.split(cleaned)
    return [p.strip() for p in parts if p.strip()]



def main() -> None:
    spark = (
        SparkSession.builder
        .appName("mini_project")
        .master("local[*]")
        .getOrCreate()
    )
    sc = spark.sparkContext

    # O6 handle missing file and load text from local path
    base_dir = Path(__file__).resolve().parent
    input_path = base_dir / "sherlock_holmes.txt"
    if not input_path.exists():
        print(f"ERROR: Input file not found at: {input_path}")
        spark.stop()
        return

    # Raw lines
    lines = sc.textFile(str(input_path))

    # O1: broadcast stopwords
    stopwords_b = sc.broadcast(STOPWORDS)

    # O2: global metric for blank lines
    blank_line_count = sc.accumulator(0)

    def count_blank_lines(line: str) -> str:
        if not line or not line.strip():
            blank_line_count.add(1)
        return line
    
    lines_counted = lines.map(count_blank_lines)
    

    # T1: normalize words
    normalized_words = lines_counted.map(normalize_for_words)

    # T2: Split into words
    words = normalized_words.flatMap(lambda s: s.split())

    # T3: filter common stop words
    filtered_words = words.filter(lambda w: w and w not in stopwords_b.value)

    # T4: Count total characters in filtered words
    total_characters = lines_counted.map(len).sum()

    # T5: Find the longest sentence
    sentences = lines_counted.flatMap(split_into_sentences)
    longest_sentence = sentences.reduce(lambda a, b: a if len(a) > len(b) else b)
    longest_len = len(longest_sentence)

    # T6: Extract sentences containing "Watson"
    Watson_sentences =  sentences.filter(lambda x: "watson" in x).collect()

    # T&: Get how many unique words are in the text
    uniques_words = words.distinct().count()


    # T8: Get the top 10 most common words
    word_counts = words.map(lambda x: (x, 1)).reduceByKey(lambda a, b: a + b)
    top_word_counts= word_counts.sortBy(lambda x: x[1], ascending=False).take(10)

    # T9: Get the count of starting words in sentences
    first_word = sentences.map(lambda x: x.strip().split()[0] if x.strip() else "").filter(lambda x: x != "").map(lambda x: (x, 1)).reduceByKey(lambda a, b: a + b).sortBy(lambda x: x[1], ascending=False).take(5)

    # T10: average word length
    word_count = words.map(lambda x: len(x.split())).sum()
    char_count = words.map(lambda x: len(x)).sum()

    average_length = char_count / word_count
    
    # T11: Count how many words have N letters
    length_counts = words.map(lambda x: (len(x), 1)).reduceByKey(lambda a, b: a + b).collect()

    # filter "A SCANDAL IN BOHEMIA" and "THE RED-HEADED LEAGUE"
    lines_between = lines.zipWithIndex().filter(lambda x: TITLE_START in x[0] or TITLE_END in x[0]).map(lambda x: x[1]).collect()

    #O3: pair words with their len
    word_len_pair = words.map(lambda x: (len(x), x))
    
    # O4: 
    letters = normalized_words.flatMap(lambda line: [ch for ch in line if "a" <= ch <= "z"])
    letter_freq = (letters.map(lambda ch: (ch, 1)).reduceByKey(lambda a, b: a + b).collect())

    # O5: group by words's starting letters
    grouped_by_first_letter = (
        filtered_words.map(lambda w: (w[0], w)).groupByKey()
        .mapValues(lambda ws: list(ws)[:5])  # cap this cuz too big
        .sortByKey()
        .collect()
    )

    
    print(f"T4 Total characters in book: {int(total_characters)}")
    print(f"T5 Longest line length: {longest_len}")
    print(f"T5 Longest line: {longest_sentence[:120].strip()}...")

    print(f"\nT6 Lines containing 'Watson': {len(Watson_sentences)}")
    for line in Watson_sentences[:5]:
        print(line.strip())

    print(f"\nT7 Unique words: {uniques_words}")
    print(f"\nT8 Top 10 frequent words: {top_word_counts}")

    print(f"\nT9 First word counts : {first_word}")

    print(f"\nT10 Average word length: {average_length}")

    print("\nT11 Word length distribution preview:")
    print(length_counts[:15])

    print(f"\nT12 Extracted chapters lines at: {lines_between}")

    print("\nO3 Sample word len pairs:", word_len_pair.take(10))
    print("\nO4 Letter frequency :", letter_freq[:10])
    print("\nO5 Grouped words by starting letter (sample lists capped):")
    for letter, ws in grouped_by_first_letter[:5]:
        print(letter, ws)


    spark.stop()




if __name__ == "__main__":
    main()