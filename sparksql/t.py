from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import *
import datetime
sc = SparkContext("local[5]", "myapp")
sc.setLogLevel("ERROR")
ssc = StreamingContext(sc, 30)
ssc.checkpoint("checkpoint_App")


def getSparkSessionInstance(sparkConf):
    if ("sparkSessionSingletonInstance" not in globals()):
        globals()["sparkSessionSingletonInstance"] = SparkSession \
            .builder \
            .config(conf=sparkConf) \
            .getOrCreate()
    return globals()["sparkSessionSingletonInstance"]


# DataFrame operations inside your streaming program

words = ssc.socketTextStream("localhost", 9999)

def top_protocol_H_T(time, rdd, H, T):
    """
    :param H:
    :param T:
    :return:
    """
    print("\n")
    print("========= %s =========" % str(time))
    try:
        # Get the singleton instance of SparkSession
        spark = getSparkSessionInstance(rdd.context.getConf())

        rowRdd = rdd.map(lambda p: p.split("/"))
        
        # Convert RDD[String] to RDD[Row] to DataFrame
        rowRdd = rowRdd.map(lambda p: Row(ts=datetime.datetime.strptime(p[0], '%Y-%m-%d %H:%M:%S.%f'),
                                          protocol=p[1], portNum=p[2], src_ip=p[3], dest_ip=p[4], data_size=p[5]))
        wordsDataFrame = spark.createDataFrame(rowRdd)

        # Creates a temporary view using the DataFrame
        wordsDataFrame.createOrReplaceTempView("services")

        q = "SELECT b.protocol from " \
            "(select protocol, sum(data_size) as bw from services where unix_timestamp(current_timestamp()) - unix_timestamp(ts) < " + str(T) +" group by protocol) as b " \
            "where b.bw > (select sum(data_size) from services)/"+str(H)

        #x = "where unix_timestamp(current_timestamp()) - unix_timestamp(ts) < 20"
        # q = "select ts from services where unix_timestamp(current_timestamp()) - unix_timestamp(ts) < 5"
        # Do word count on table using SQL and print it
        wordCountsDataFrame = spark.sql(q)

        wordCountsDataFrame.show()
    except:
        pass


#  List the top-k most resource intensive protocols over the last T time units.
def top_k_protocols(time, rdd, k, T):
    """

    :param k:
    :param T:
    :return:
    """
    print("\n")
    print("========= %s =========" % str(time))
    try:
        spark = getSparkSessionInstance(rdd.context.getConf())
        rowRdd = rdd.map(lambda p: p.split("/"))
        rowRdd = rowRdd.map(lambda p: Row(ts=datetime.datetime.strptime(p[0], '%Y-%m-%d %H:%M:%S.%f'),
                                          protocol=p[1], portNum=p[2], src_ip=p[3], dest_ip=p[4], data_size=p[5]))
        wordsDataFrame = spark.createDataFrame(rowRdd)
        wordsDataFrame.createOrReplaceTempView("services")

        q = "SELECT b.protocol, b.t from " \
            "(select protocol, count(protocol) as t from services where unix_timestamp(current_timestamp()) - unix_timestamp(ts) < " + str(T) +" group by protocol) as b order by b.t desc limit " + str(k)

        wordCountsDataFrame = spark.sql(q)
        wordCountsDataFrame.show()
    except:
        pass


words.foreachRDD(lambda x: top_k_protocols(datetime.datetime.now(),x,20,5))
ssc.start()
ssc.awaitTermination()

