from pyspark import SparkConf, SparkContext
import sys


def f(x): print(x)

if __name__ == "__main__" :
 conf = SparkConf().setMaster("local").setAppName("test01")
 sc = SparkContext(conf = conf)

 lines = sc.textFile("/home/ec2-user/SparkCourse/ml-100k/u.data")
 lineLengths = lines.map(lambda s : len(s))
 lineTwo = lines.map(lambda s : s.split()[2])
 #lineLengths.foreach(f)

 print(lineLengths.take(3))
 print(lineTwo.take(3))
 print(lines.take(3))
# lines.foreach(f)
 print(lines.count(),lineTwo.count())
 totalLength = lineLengths.reduce(lambda a, b: a + b)
 print(totalLength)
