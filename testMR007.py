from pyspark import SparkConf, SparkContext
import collections

conf = SparkConf().setMaster("local").setAppName("testMR007")
sc = SparkContext(conf = conf)

def countCoOccurences(line):
    elements = line.split()
    return (int(elements[0]), len(elements) - 1)

def parseNames(line):
    fields = line.split('\"')
    return (int(fields[0]), fields[1].encode("utf8"))

names = sc.textFile("/home/ec2-user/SparkCourse/Marvel-Names.txt")
namesRdd = names.map(parseNames)

lines = sc.textFile("/home/ec2-user/SparkCourse/Marvel-Graph05.txt")


pairings = lines.map(countCoOccurences)
#returns = pairings.collect()
#for ret in returns:
#  print(" %i %i" % (int(ret[0]), ret[1]) )

totalFriendsByCharacter = pairings.reduceByKey(lambda x, y : x + y)
flipped = totalFriendsByCharacter.map(lambda xy : (xy[1], xy[0]))

mostPopular = flipped.max()

mostPopularName = namesRdd.lookup(mostPopular[1])[0]

print(str(mostPopularName) + " is the most popular superhero, with " + \
    str(mostPopular[0]) + " co-appearances.")
    
