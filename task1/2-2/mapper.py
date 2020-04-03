#!/usr/bin/python

import sys

for line in sys.stdin: #infile:
	line = line.strip()
	columns = line.split(',') # split line into parts
	if columns[12] != '':
		origin = columns[8]
		dest = columns[9]
		dep_delay = columns[12]
		print('%s\t%s' % ((origin, dest), dep_delay))