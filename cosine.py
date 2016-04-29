from __future__ import print_function
from collections import Counter
import operator
import pyspark
import sys
import os
import numpy
from pyspark import SparkFiles
from numpy import array, corrcoef
from numpy import array
from numpy.linalg import norm
import math

sc = pyspark.SparkContext()
path = '/Users/roshaninagmote/Documents/Big Data/books/*.txt'

#files = sc.wholeTextFiles(path).map(lambda (x,y):x,y)

def countWords(input):
    c=Counter(input)
    return (c.most_common(10))

def vector_create(input):
    if (input.u, (input.v!=0)):
        math.abs(input.u - input.v)
    elif(input.u, None):
        math.abs(input.u)


def calcVector(wordList):
    #print(wordList)
    global featureWords
    a1 = [0]*len(featureWords)
    for i in range(len(featureWords)):
        #print(featureWords)

        for (word,count) in wordList:
            if featureWords[i] == word:
                a1[i] = count
                break

    return (a1/norm(a1))

files = sc.wholeTextFiles(path).map(lambda (x,y):(x,y.split(" ")))
#    .flatMap(lambda (x,y):(x,[(y[i],1) for i in range(0,len(y)-1)]))
#    .reduceByKey(lambda (x,(a,b)):(x,a+b)).sortByKey(False)

words = files.map(lambda (names,words): wordList)
fileNames = files.map(lambda (names,words): names)
words = words.flatMap(lambda x:x)
featureWords = words.map(lambda word: (word,1)).reduceByKey(lambda x,y: x+y).sortBy(lambda x: x[1], False).keys().take(1000)
wordList = files.mapValues(countWords)
featureVector = wordList.mapValues(calcVector)
print(wordList)
vectorPairs = featureVector.cartesian(featureVector)
cSim = vectorPairs.map(lambda ((file1,vector1),(file2,vector2)):(file1,file2,operator.mul(vector1,vector2)))\
    .map(lambda (file1,file2,vector):(file1,file2,sum(vector)))#.reduce(lambda a,b: operator.add(a,b))

cSim.foreach(print)

#freq=files.mapValues(countWords)
#word_freq=freq.leftOuterJoin(freq).mapValues(lambda x:vector_create(x[0])).mean()
#word_freq.foreach(print)


#a = array([1.0, 1.0, 0.6])
#b = a/norm(a)

#final=word_list1.cartesian(word_list2).map(lambda (x,y):corrcoef(x,y)[0,1]).collect()
#final
