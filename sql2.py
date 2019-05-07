#!/usr/local/bin/python3.7

from sqlfuncs.sql import *
sc = SparkContext("local[6]", "myapp2")
sc.setLogLevel("ERROR")
ssc = StreamingContext(sc, 4)

words = ssc.socketTextStream("localhost", 9999)

words1 = words.window(windowDuration=16, slideDuration=4)
words2 = words.window(windowDuration=16, slideDuration=4)
words1.foreachRDD(lambda time, x: top_protocol_H_T(time, x, 0.1, 15))
words2.foreachRDD(lambda time, x: top_ip_addr_H_T(time, x, 0.1, 15))
ssc.start()
ssc.awaitTermination()