__author__ = 'roshaninagmote'

import pyspark
import sys
sc = pyspark.SparkContext()
#path="/Users/roshaninagmote/Documents/Big Data/big.txt"
path=sys.argv[1]
sentence = sc.textFile(path)\
    .glom().map(lambda x: " ".join(x)).flatMap(lambda x: x.split("."))\

bigrams = sentence.map(lambda x:x.split()).flatMap(lambda x: [((x[i],x[i+1]),1) for i in range(0,len(x)-1)])\
    .filter(lambda x:len(x[0][0])>5).filter(lambda x:len(x[0][1])>5)

bigrams_count = bigrams.reduceByKey(lambda x,y:x+y).map(lambda x:(x[1],x[0]))\
    .sortByKey(False)

print bigrams_count.take(10)

