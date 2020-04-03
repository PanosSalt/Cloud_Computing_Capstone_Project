#!/usr/bin/python
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from operator import itemgetter
import sys
import time

def exception_handler(exception_type, exception, traceback):
    print "%s: %s" % (exception_type.__name__, exception)

def printResults(rdd):
    print "----------------- TOP 10 ----------------------"
    for line in rdd.take(10):
        print line
    print "SIZE: %d" % rdd.count()

def saveResults(rdd):
    topten = rdd.take(10)
    if len(topten) == 0:
		return
    file = open(sys.argv[2], 'w')
    file.writelines(["%s  %s\n" % (item[1], item[0])  for item in topten])

def cutOffTopTen(iterable):
	topTen = []
	for tupl in iterable:
		if len(topTen) < 10:
			topTen.append(tupl)
			topTen.sort()
		elif topTen[9][0] > tupl[0]:
			topTen[9] = tupl
			topTen.sort()
	return iter(topTen)

def updateFunction(newValues, runningAvg):
    if runningAvg is None:
        runningAvg = (0.0, 0, 0.0)
    # calculate sum, count and average.
    prod = sum(newValues, runningAvg[0])
    count = runningAvg[1] + len(newValues)
    avg = prod / float(count)
    return (prod, count, round(avg,2))

sc = SparkContext(appName="TopTenCarriers")
sc.setLogLevel('OFF')

# Create a local StreamingContext
ssc = StreamingContext(sc, 1)
ssc.checkpoint("/checkpoints/1-2/")
lines = KafkaUtils.createDirectStream(ssc, ['test1'], {"metadata.broker.list": sys.argv[1], "auto.offset.reset":"smallest"})

# Split each line by separator
lines = lines.map(lambda tup: str(tup[1]))
rows = lines.map(lambda line: line.split(','))

# Get the carriers and delays
rows = rows.filter(lambda row: row[16] != '')
carriers = rows.map(lambda row: (row[6], float(row[16])))

# Count averages
carriers = carriers.updateStateByKey(updateFunction)

# Filter top ten
sorted = carriers.map(lambda tuple: (tuple[1][2], tuple[0]))

# We filter at each worker by partition as well, reducing shuffling time between each workers
sorted = sorted.transform(lambda rdd: rdd.mapPartitions(cutOffTopTen))

# Final sorting
sorted = sorted.transform(lambda rdd: rdd.sortByKey())
# Saving and debugging
sorted.foreachRDD(lambda rdd: printResults(rdd))
sorted.foreachRDD(lambda rdd: saveResults(rdd))

if __name__ == "__main__":
    sys.excepthook = exception_handler
    ssc.start()             # Start the computation
    ssc.awaitTermination()  # Wait for the computation to terminate
    if KeyboardInterrupt:
        print("Shutting down Spark...")
        ssc.stop(stopSparkContext=True, stopGraceFully=True)