#!/usr/bin/python

import sys

for line in sys.stdin:
	line = line.strip()
	columns = line.split(',') # split line into parts
	if columns[8] != '"Origin"':
		airport = columns[8]
		print ('%s\t%s' % (airport, 1))
	if columns[9] != '"Dest"':
		airport = columns[9]
		print('%s\t%s' % (airport, 1))