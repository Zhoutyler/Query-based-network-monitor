#!/usr/local/bin/python3.7
import findspark
findspark.init()
from sqlfuncs.sql import *
sc = SparkContext("local[10]", "myapp")
sc.setLogLevel("ERROR")
ssc = StreamingContext(sc, 2)
ssc.checkpoint("checkpoint_App")

words = ssc.socketTextStream("localhost", 9999)
words1 = words.window(windowDuration=16, slideDuration=2)
words2 = words.window(windowDuration=16, slideDuration=2)
words1.foreachRDD(lambda time, x: top_k_ip(time, x, 10, 15))
words2.foreachRDD(lambda time, x: top_k_protocols(time, x, 10, 15))
ssc.start()
ssc.awaitTermination()

