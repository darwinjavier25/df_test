from pyspark.sql import SparkSession
from pyspark.sql import functions as func

spark = SparkSession.builder.appName("darwin_word_count").getOrCreate()


read_df = spark.read.text("./resources/book.txt")
read_df.show(10, False)

words = read_df.select(func.explode(func.split(read_df.value, "\\W+")).alias("word"))
words.show(10, False)
words.filter(words.word.like("%Employment%")).show()
wordsWithoutEmptyString = words.filter(words.word != "")
lowerCaseWords = wordsWithoutEmptyString.select(func.lower(wordsWithoutEmptyString.word).alias("word"))
lowerCaseWords.show(10, False)

wordCounts = lowerCaseWords.groupBy("word").count()
wordCountSorted = wordCounts.sort("count")
wordCountSorted.show(wordCountSorted.count())