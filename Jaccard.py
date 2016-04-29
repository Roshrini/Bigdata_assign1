import pyspark
import sys
import os
import numpy
import matplotlib.pyplot as plt
import matplotlib.image as mpimg
from pyspark import SparkFiles
from numpy import array, corrcoef

sc = pyspark.SparkContext()
path = sys.argv[1]

files = sc.wholeTextFiles(path).map(lambda (x,y): (x,y.split()))
fileNames = files.keys().collect()
fileNameDictionary = {y.encode('utf8'):x for x,y in enumerate(fileNames)}

def jaccardCalculator(set_1, set_2):
   set_1 = set(set_1)
   set_2 = set(set_2)
   n = len(set_1.intersection(set_2))
   values = n / float(len(set_1) + len(set_2) - n)
   return values

cp = files.cartesian(files)
js = cp.map(lambda x: ((x[0][0],x[1][0]),jaccardCalculator(x[0][1],x[1][1]))).collect()

print js

# Code to plot similarity matrix
result = numpy.zeros(shape=(len(fileNames),len(fileNames),3))
for value in js:
   x,y,v = value[0][0],value[0][1],value[1]
   x,y = fileNameDictionary[x],fileNameDictionary[y]
   result[x][y][0],result[x][y][1],result[x][y][2] = v,v,v

for i in range(0,len(result)):
   for j in range(0,len(result)):
      print(result[i][j][0])
      print("\t")
   print("\n")

plt.imshow(result)
plt.show()
