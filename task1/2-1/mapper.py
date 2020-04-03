#!/usr/bin/python

import sys

for line in sys.stdin: #infile:
	line = line.strip()
	columns = line.split(',') # split line into parts
	if columns[12] != '':
		origin = columns[8]
		unique_carrier = columns[6]
		dep_delay = columns[12]
		print('%s\t%s' % ((origin, unique_carrier), dep_delay))