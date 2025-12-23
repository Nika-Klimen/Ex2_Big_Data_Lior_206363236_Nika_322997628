if __name__ == "__main__":
 if len(sys.argv) != 2:
 print("Usage: wordcount <input_folder>", file=sys.stderr)
 sys.exit(-1)
 conf = SparkConf().setAppName("python-word-count")
 sc = SparkContext(conf=conf)
 text_file = sc.textFile("hdfs://" + sys.argv[1])
 counts = text_file.flatMap(lambda line: line.split(" ")) \
.map(lambda word: (word, 1)) \
.repartition(5) \
.reduceByKey(lambda a, b: a + b) \
.filter(lambda x: len(x[0]) > 5)
 list = counts.takeOrdered(40, key = lambda x: -x[1])
 print("--------------------------------------------")
 print(*list, sep="\n")
 print("--------------------------------------------")