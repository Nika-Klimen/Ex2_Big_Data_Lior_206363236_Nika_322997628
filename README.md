# Spark Training – Exercises 1–5

**Students:**
- Lior Berlin – 206363236  
- Nika Klimenchuk – 322997628  

---

## Exercise 1 
### Q1 – Code

```python
import sys
from pyspark import SparkContext, SparkConf

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
```

### Q2 - Screenshot
![Exercise 1 – Result](https://github.com/Nika-Klimen/Ex2_Big_Data_Lior_206363236_Nika_322997628/blob/main/Ex1.png)

---

## Exercise 2 
### Q1 – Code

```python
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
    print("--------------------------------------------")
```

### Q2 - Screenshot
![Exercise 2 – Result](https://github.com/Nika-Klimen/Ex2_Big_Data_Lior_206363236_Nika_322997628/blob/main/Ex2.png)

---

## Exercise 3 
### Screenshot
![Exercise 3 – DAG](https://github.com/Nika-Klimen/Ex2_Big_Data_Lior_206363236_Nika_322997628/blob/main/Ex3.png)

---

## Exercise 4
### Q1 – Code

```python
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
```

### Q2 - Screenshot
![Exercise 4 – Result](https://github.com/Nika-Klimen/Ex2_Big_Data_Lior_206363236_Nika_322997628/blob/main/Ex4.png)

---

## Exercise 5
### Q1 – Code

```python
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
```

### Q2 - Screenshot
![Exercise 5 – Result](https://github.com/Nika-Klimen/Ex2_Big_Data_Lior_206363236_Nika_322997628/blob/main/Ex5.png)
