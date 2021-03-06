#!/usr/local/bin/python3.7
#import findspark
#findspark.init()
from sqlfuncs.sql import *

sc = SparkContext("local[5]", "myapp1")
sc.setLogLevel("ERROR")
ssc = StreamingContext(sc, 2)
ssc.checkpoint("checkpoint_App1")

words = ssc.socketTextStream("localhost", 9999)

words1 = words.window(windowDuration=16, slideDuration=2)
words1.foreachRDD(lambda time, x: top_H_T(time, x, 0.1, 15))
ssc.start()
ssc.awaitTermination()
