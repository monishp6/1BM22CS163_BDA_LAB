sc = SparkContext("local", "SimpleWordCount")

rdd = sc.textFile("file.txt")  

counts = (rdd.flatMap(lambda line: line.split())
             .map(lambda word: (word, 1))
             .reduceByKey(lambda a, b: a + b)
             .filter(lambda x: x[1] > 4))

for word, count in counts.collect():
    print(word, count)

sc.stop()
