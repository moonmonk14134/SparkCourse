from pyspark import SparkConf, SparkContext

def loadMovieNames():
    movieNames = {}
    with open("/home/ec2-user/SparkCourse/ml-100k/u.item", encoding = "ISO-8859-1") as f:
        for line in f:
            fields = line.split('|')
            movieNames[int(fields[0])] = fields[1]

    return movieNames

conf = SparkConf().setMaster("local").setAppName("testMR006SwapBroadcast")
sc = SparkContext(conf = conf)
nameDict = sc.broadcast(loadMovieNames())

lines = sc.textFile("/home/ec2-user/SparkCourse/ml-100k/u.data")
movies = lines.map(lambda x: (int(x.split()[1]),1))
movieCounts = movies.reduceByKey(lambda x, y : x + y)

#flipped = movieCounts.map(lambda (x, y) : (y, x) )
flipped = movieCounts.map( lambda x: (x[1],x[0]) )
sortedMovies = flipped.sortByKey()
sortedMoviesWithNames = sortedMovies.map(lambda x : (nameDict.value[x[1]], x[0]))

results = sortedMoviesWithNames.collect()

for result in results:
    print( "Movie Name: %40s   Count: %5i ." %(result[0], result[1]) )