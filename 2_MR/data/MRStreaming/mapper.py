#!/usr/bin/env python
import sys
 
#--- get all lines from stdin ---
for line in sys.stdin:
    #--- remove leading and trailing whitespace---
    line = line.strip()

    #--- split the line into words ---
    words = line.split()

    #--- output tuples [word, 1] in tab-delimited format---
    for word in words: 
        print '%s\t%s' % (word, "1")

# Use following command to run the application
#hadoop jar /usr/lib/hadoop-mapreduce/hadoop-streaming.jar -file ./mapper.py -mapper ./mapper.py -file ./reducer.py -reducer ./reducer.py -input /user/cloudera/constitution.txt -output dft-output
