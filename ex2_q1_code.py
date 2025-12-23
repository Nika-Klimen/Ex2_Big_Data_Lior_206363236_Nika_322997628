import sys
from pyspark import SparkContext, SparkConf
if __name__ == "__main__":
 if len(sys.argv) != 2:
 print("Usage: wordcount <input_folder>", file=sys.stderr)
 sys.exit(-1)
 conf = SparkConf().setAppName("python-word-count")
 sc = SparkContext(conf=conf)
 text_file = sc.textFile("hdfs://" + sys.argv[1])
 words = text_file.flatMap(lambda line: line.split(" ")).cache()
 counts = words.map(lambda word: (word, 1)) \
 .repartition(5) \
 .reduceByKey(lambda a, b: a + b)
 list_ordered_40 = counts.takeOrdered(40, key=lambda x: -x[1])
 print("--------------------------------------------")
 print(*list_ordered_40, sep="\n")
 print("--------------------------------------------")
 distinct_words = words.distinct().count()
 print("--------------------------------------------")
 print("Number of distinct words:", distinct_words)
 print("----------------------------------------