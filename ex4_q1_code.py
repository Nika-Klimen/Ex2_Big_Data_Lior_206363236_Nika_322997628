import sys
from pyspark import SparkContext, SparkConf
if __name__ == "__main__":
 if len(sys.argv) != 2:
 print("Usage: wordcount <input_folder>", file=sys.stderr)
 sys.exit(-1)
 conf = SparkConf().setAppName("python-word-count")
 sc = SparkContext(conf=conf)
 text_file = sc.textFile("s3a://" + sys.argv[1] + "/*")

 words = text_file.flatMap(lambda line: line.split(" "))
 cleaned_words = words.map(lambda w: w.rstrip(".,")).filter(lambda w: w != "")
 alpha_words = cleaned_words.filter(lambda w: w.isalpha())
 longest_word = alpha_words.reduce(lambda a, b: a if len(a) >= len(b) else b)
 print("--------------------------------------------")
 print("Longest word:", longest_word)
 print("--------------------------------------------")