#!/usr/bin/python

import sys

#infile = sys.stdin
#next(infile) # skip first line of input file

for line in sys.stdin: #infile:
	line = line.strip()
	columns = line.split(',') # split line into parts
	if columns[16] != '':
		unique_carrier = columns[6]
		arr_delay = columns[16]
		print('%s\t%s' % (unique_carrier, arr_delay))