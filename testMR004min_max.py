from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("testMR004min")
sc = SparkContext(conf = conf)

def parseLine(line):
    fields = line.split(',')
    stationID = fields[0]
    entryType = fields[2]
    temperature = float(fields[3]) * 0.1 * (9.0 / 5.0) + 32.0
    return (stationID, entryType, temperature)

lines = sc.textFile("/home/ec2-user/SparkCourse/1800.csv")
parseLines = lines.map(parseLine)

minTemps = parseLines.filter(lambda x: "TMIN" in x[1])
stationTemps = minTemps.map(lambda x: (x[0], x[2]))
minTemps = stationTemps.reduceByKey(lambda x,y : min(x,y))
results = minTemps.collect()
for result in results:
    print("Min temporary: "+ result[0] + "\t{:.2f}F".format(result[1]))

maxTemps = parseLines.filter(lambda x: "TMAX" in x[1])
stationTemps = maxTemps.map(lambda x: (x[0],x[2]))
maxTemps = stationTemps.reduceByKey(lambda x,y : max(y,x))
results = maxTemps.collect()

for result in results:
    print("Max temporary: "+ result[0] + "\t{:.2f}F".format(result[1]))
