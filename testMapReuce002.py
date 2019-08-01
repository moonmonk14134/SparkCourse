from pyspark import SparkConf,SparkContext
import sys
import collections

conf = SparkConf().setMaster("local").setAppName("testMapReduce002")
sc = SparkContext(conf = conf)

lines = sc.textFile("/home/ec2-user/SparkCourse/ml-100k/u.data")
ratings = line.map( lambda x: x.split()[2])
result = ratings.countByValue()

def f(x): print(x)

result.foreach(f)
