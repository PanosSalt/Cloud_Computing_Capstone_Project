#!/usr/bin/python

import sys

for line in sys.stdin:
	line = line.strip()
	columns = line.split(',') # split line into parts
	if columns[16] != '':
		origin = columns[8]
		dest = columns[9]
		arr_delay = columns[16]
		print('%s\t%s' % ((origin, dest), arr_delay))