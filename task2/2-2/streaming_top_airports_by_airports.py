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
    for line in rdd.collect():
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
		file.write("\n--- %s ---\n\n" % item[0])
		file.writelines(["(%s: %s)\n" % (record[0], record[1]) for record in item[1]])

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
    return (prod, count, avg)

def append(aggr, newAirportAvg):
	"""
	Add new element to aggregate. Aggregate contains top ten airports and departure delays.
	Sample: [('JFK',-0.0001), ('SFO',0.025), ('LAX',0.3)]
	"""
	aggr.append(newAirportAvg)
	aggr.sort(key=lambda element: element[1])
	return aggr[0:10]

def combine(left, right):
	"""
	Combine two aggregates. Aggregate contains top ten airports and departure delays.
	Sample: [('JFK',-0.0001), ('SFO',0.025), ('LAX',0.3)]
	"""
	for newElement in right:
		left.append(newElement)
	left.sort(key=lambda element: element[1])
	return left[0:10]

# MAIN

sc = SparkContext(appName="TopTenAirportsByAirports")
sc.setLogLevel('OFF')

# Create a local StreamingContext
ssc = StreamingContext(sc, 1)
ssc.checkpoint("/checkpoints/checkpoint-top-carriers/")
lines = KafkaUtils.createDirectStream(ssc, ['test1'], {"metadata.broker.list": sys.argv[1], "auto.offset.reset":"smallest"})

# Split each line by separator
lines = lines.map(lambda tup: str(tup[1]))
rows = lines.map(lambda line: line.split(','))

# Get the airports
rows = rows.filter(lambda row: row[12] != '')
airports_and_airports = rows.map(lambda row: ((row[8], row[9]), float(row[12])))

# Count averages
airports_and_airports = airports_and_airports.updateStateByKey(updateFunction)

# Change key to just airports
airports = airports_and_airports.map(lambda row: (row[0][0], (row[0][1], row[1][2])))
# Aggregate to just top 10 carriers
airports = airports.transform(lambda rdd: rdd.aggregateByKey([],append,combine))
#Filter and print
airports = airports.filter(lambda x: x[0] in ['"SRQ"', '"CMH"', '"JFK"', '"SEA', '"BOS"'])

airports.foreachRDD(printResults)
airports.foreachRDD(saveResults)

if __name__ == "__main__":
    sys.excepthook = exception_handler
    ssc.start()             # Start the computation
    ssc.awaitTermination()  # Wait for the computation to terminate
    if KeyboardInterrupt:
        print("Shutting down Spark...")
        ssc.stop(stopSparkContext=True, stopGraceFully=True)