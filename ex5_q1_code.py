import sys
from pyspark import SparkContext, SparkConf
if __name__ == "__main__":
 if len(sys.argv) != 2:
 print("Usage: count_words_in_line <input_bucket>", file=sys.stderr)
 sys.exit(-1)
 conf = SparkConf().setAppName("count-words-in-line")
 sc = SparkContext(conf=conf)
 text_file = sc.textFile("s3a://" + sys.argv[1] + "/*")
 line_counts = text_file.map(lambda line: (len([w for w in line.split(" ") if w != ""]), line))
 max_line = line_counts.reduce(lambda a, b: a if a[0] >= b[0] else b)
 print("--------------------------------------------")
 print("Max words in a line:", max_line[0])
 print("The line:", max_line[1])
 print("--------------------------------------------")