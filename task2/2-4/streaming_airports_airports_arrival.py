#!/usr/bin/python
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from kafka import SimpleProducer, KafkaClient
import sys

def exception_handler(exception_type, exception, traceback):
    print "%s: %s" % (exception_type.__name__, exception)

def printResults(rdd):
    """
    Print partial results to screen.
    """
    print "----------------- SNAPSHOT ----------------------"
    for line in rdd.take(10):
        print line
    print "SIZE: %d" % rdd.count()

def saveResults(rdd):
	"""
	Save results as a report.
	"""
	results = rdd.collect();
	if len(results) == 0:
		return
	file = open(sys.argv[2], 'w')
	for item in results:
		file.write("\n%s -> %s: %s\n\n" % (item[0][0], item[0][1], item[1]))

def updateFunction(newValues, runningAvg):
    if runningAvg is None:
        runningAvg = (0.0, 0, 0.0)
    # calculate sum, count and average.
    prod = sum(newValues, runningAvg[0])
    count = runningAvg[1] + len(newValues)
    avg = prod / float(count)
    return (prod, count, round(avg,2))

# MAIN

sc = SparkContext(appName="AirportAirportArrival")
sc.setLogLevel('OFF')

# Create a local StreamingContext
ssc = StreamingContext(sc, 1)
ssc.checkpoint("/checkpoints/checkpoint-airport-airport-arrival/")
lines = KafkaUtils.createDirectStream(ssc, ['test1'], {"metadata.broker.list": sys.argv[1], "auto.offset.reset":"smallest"})

# Split each line by separator
lines = lines.map(lambda tup: str(tup[1]))
rows = lines.map(lambda line: line.split(','))

# Get the airports
rows = rows.filter(lambda row: row[16] != '')
airports_fromto = rows.map(lambda row: ((row[8], row[9]), float(row[16])))

# Count averages
airports_fromto = airports_fromto.updateStateByKey(updateFunction)

# Change key to just airports
airports = airports_fromto.map(lambda row: ((row[0][0], row[0][1]), row[1][2]))
#Filter and print
airports = airports.filter(lambda x: x[0] in [('"LGA"','"BOS"'),('"BOS"','"LGA"'),('"OKC"','"DFW"'),('"MSP"','"ATL"')])

airports.foreachRDD(printResults)
airports.foreachRDD(saveResults)

if __name__ == "__main__":
    sys.excepthook = exception_handler
    ssc.start()             # Start the computation
    ssc.awaitTermination()  # Wait for the computation to terminate
    if KeyboardInterrupt:
        print("Shutting down Spark...")
        ssc.stop(stopSparkContext=True, stopGraceFully=True)