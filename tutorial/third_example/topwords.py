#! /usr/bin/env python -u
# coding=utf-8

__author__ = 'xl'

import sys
from operator import add

from pyspark import SparkContext
import re

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: wordcount <input> <output>")
        exit(-1)
    sc = SparkContext(appName="PythonWordCount")
    common_words = ["the", "a", "an", "and", "of", "to", "in", "am", "is", "are", "at", "not"]
    lines = sc.textFile(sys.argv[1], 1)
    counts = lines.flatMap(lambda x: re.split(r"[ \t,;\.\?!-:@\[\]\(\){}_\*/]+", x)) \
                  .filter(lambda x: x.lower() not in common_words and len(x) > 0) \
                  .map(lambda x: (x, 1)) \
                  .reduceByKey(add) \
                  .map(lambda x: (x[1],x[0])) \
                  .sortByKey(ascending=False) \
                  .take(10)
    counts = sc.parallelize(counts) \
                  .map(lambda x: (x[1],x[0]))
    counts.saveAsTextFile(sys.argv[2])
    output = counts.collect()

    for (word, count) in output:
        print("%s: %i" % (word, count))


    sc.stop()
