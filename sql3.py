#!/usr/local/bin/python3.7
from sqlfuncs.sql import *
sc = SparkContext("local[6]", "myapp3")
sc.setLogLevel("ERROR")
ssc = StreamingContext(sc, 2)
words = ssc.socketTextStream("localhost", 9999)

words1 = words.window(windowDuration=16, slideDuration=2)
words2 = words.window(windowDuration=16, slideDuration=2)
words1.foreachRDD(lambda time, x: ip_x_more_than_stddev(time, x, 1, 15))
words2.foreachRDD(lambda time, x: protocols_x_more_than_stddev(time, x, 1, 15))
ssc.start()
ssc.awaitTermination()

