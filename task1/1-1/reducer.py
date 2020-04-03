#!/usr/bin/python

from operator import itemgetter 
import sys

# keep a map of the sum of the airports
redditupvotemap = {}

for line in sys.stdin:
    line = line.strip()
    airport, count = line.split('\t', 1)
    try:
        count = int(count)
        redditupvotemap[airport] = redditupvotemap.get(airport, 0) + count
    except ValueError:
        # ignore lines where the count is not a number
        pass

# sort the reddits alphabetically;
alphabetic_redditupvotemap = sorted(redditupvotemap.items(), key=itemgetter(0))

# output to STDOUT
for airport, total_count in alphabetic_redditupvotemap:
    print ('%s\t%s'% (airport, total_count))