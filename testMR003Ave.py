from pyspark import SparkConf, SparkContext
import collections

conf = SparkConf().setMaster("local").setAppName("FriendsByAge")
sc = SparkContext(conf = conf)

def parseLine(line):
    fields = line.split(',')
    age = int(fields[2])
    numFriends = int(fields[3])
    return (age, numFriends)

lines = sc.textFile("/home/ec2-user/SparkCourse/fakefriends.csv")
rdd = lines.map(parseLine)
totalsByAge = rdd.mapValues(lambda x: (x, 1)).reduceByKey(lambda x, y: (x[0]+y[0], x[1]+y[1]))
averagesByAge = totalsByAge.mapValues(lambda x: x[0]/x[1])  ## RDD
averagesByAgeS10 = averagesByAge.take(10)  ## sample is not a RDD
print(averagesByAgeS10)

results = averagesByAge.sortByKey(lambda x:x).collect() ## collect() : list
#results = averagesByAgeS10.sortByKey(lambda x:x).collect() ## collect() : list
# averagesByAgeS10 is not a RDD, so sortByKey does not support #

for result in results:
    print(" Age: %i , Average: %8.2f"  % (result[0],result[1]) )

## ============================
##[(33, 325.3333333333333), (26, 242.05882352941177), (55, 295.53846153846155), (40, 250.8235294117647), (68, 269.6), (59, 220.0), (37, 249.33333333333334), (54, 278.0769230769231), (38, 193.53333333333333), (27, 228.125)]
## Age: 18 , Average:   343.38
## Age: 19 , Average:   213.27
## Age: 20 , Average:   165.00
## Age: 21 , Average:   350.88
## Age: 22 , Average:   206.43
## Age: 23 , Average:   246.30
