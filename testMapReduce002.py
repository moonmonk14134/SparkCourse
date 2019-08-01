from pyspark import SparkConf,SparkContext
import sys
import collections

conf = SparkConf().setMaster("local").setAppName("testMapReduce002")
sc = SparkContext(conf = conf)

lines = sc.textFile("/home/ec2-user/SparkCourse/ml-100k/u.data")
ratings = lines.map( lambda x: x.split()[2])
result = ratings.countByValue()

sortedResults = collections.OrderedDict(sorted(result.items()))
for key, value in sortedResults.items():
    print("%s %i" % (key, value))

print("="*10)

filter_1 = lines.filter(lambda x: int(x.split()[2]) > 2).map( lambda x: x.split()[2])
result_1 = filter_1.countByValue()
sortResults_1 = collections.OrderedDict(sorted(result_1.items()))
for key, value in sortResults_1.items():
    print("%s %i" % (key, value))
