from __future__ import print_function

import sys
import math, time
from operator import add

from pyspark import SparkContext

bad_codes = ['2', '3', '6', '7']

def mapper(word):
    if word[92] in bad_codes or word[87:92] == '+9999':
        # missing or bad data
        min_temp = float('inf')
        max_temp = -float('inf')
        cur_temp = (0, 0)
    else:
        temp = float(word[87:92]) / 10
        min_temp = temp
        max_temp = temp
        cur_temp = (temp, 1)
    if word[69] in bad_codes or word[65:69] == '9999':
        # missing or bad data
        max_wind = -float('inf')
    else:
        max_wind = float(word[65:69]) / 10
    return (min_temp, max_temp, max_wind, cur_temp)

def reducer(x, y):
    return (min(x[0], y[0]), max(x[1], y[1]), max(x[2], y[2]), (x[3][0] + y[3][0], x[3][1] + y[3][1]))

if __name__ == "__main__":
    start_time = time.time()
    if len(sys.argv) < 2:
        print("Usage: %s <year 1> ... <year N>", file=sys.stderr)
        exit(-1)
    sc = SparkContext(appName="PySparkClimateAnalysis")
    for year in sys.argv[1:]:
        lines = sc.textFile('/home/DATA/NOAA_weather/%s/*' % year)
        output = lines.flatMap(lambda x: x.split('\n')) \
                    .map(mapper) \
                    .reduce(reducer)
        print('%s:' % year)
        print('\tMin Temp:\t%f' % output[0])
        print('\tMax Temp:\t%f' % output[1])
        print('\tMax Wind:\t%f' % output[2])
        print('\tAvg Temp:\t%f' % (output[3][0] / output[3][1]))

    sc.stop()
    
    print('Time Elapsed:\t%f' % (time.time() - start_time))