from pyspark.sql import *
import datetime
from pyspark import SparkContext
from pyspark.streaming import StreamingContext

# Create a local StreamingContext with two working thread and batch interval of 1 second
sc = SparkContext("local[2]", "NetworkWordCount")
ssc = StreamingContext(sc, 1)

ssc.checkpoint("checkpoint_App")
dStream = ssc.socketTextStream("localhost", 9999)
words = dStream.map(lambda line: line.split("/"))
parts = words.map(lambda p: Row(ts=datetime(p[0]), protocol=p[1], portNum=p[2], src_ip=p[3], dest_ip=p[4], data_size=p[5]))


def getSparkSessionInstance(sparkConf):
    if ("sparkSessionSingletonInstance" not in globals()):
        globals()["sparkSessionSingletonInstance"] = SparkSession \
                .builder \
                .config(conf=sparkConf) \
                .getOrCreate()
    return globals()["sparkSessionSingletonInstance"]


def process(time, rdd):
    print("========= %s =========" % str(time))
    try:
        # Get the singleton instance of SparkSession
        spark = getSparkSessionInstance(rdd.context.getConf())

        # Convert RDD[String] to RDD[Row] to DataFrame
        rowRdd = rdd.map(lambda w: Row(word=w))
        wordsDataFrame = spark.createDataFrame(rowRdd)

        # Creates a temporary view using the DataFrame
        wordsDataFrame.createOrReplaceTempView("words")

        # Do word count on table using SQL and print it
        wordCountsDataFrame = spark.sql("select word, count(*) as total from words group by word")
        wordCountsDataFrame.show()
    except:
        pass

words.foreachRDD(process)
ssc.start()
ssc.awaitTermination()


# lines.foreachRDD(lambda rdd: rdd.foreachPartition(sendPartition))


# def sendPartition(iter):
#     r = redis.Redis(host='localhost', port=6379, db=0)
#     for record in iter:
#         r.set('foo', 'bar')

#  List protocols that are consuming more than H percent of the total external bandwidth over the last T time units.
def top_protocol_H_T(H, T):
    """
    :param H:
    :param T:
    :return:
    """

    q = "SELECT b.protocols from (select protocols, sum(data_size) " \
        "as bw from record group by protocols) as b where b.bw/"\
        + H + "> (select sum(data_size) from record)"
    
    pass




#  List the top-k most resource intensive protocols over the last T time units.
def top_k_protocols(k, T):
    """

    :param k:
    :param T:
    :return:
    """
    pass


#  List all protocols that are consuming more than X times the standard deviation of
# the average traffic consumption of all protocols over the last T time units.
def protocols_x_more_than_sd(X, T):
    """

    :param X:
    :param T:
    :return:
    """
    pass


# List IP addresses that are consuming more than H percent of the total external
# bandwidth over the last T time units.
def top_ip_addr_H_T(H, T):
    """

    :param H:
    :param T:
    :return:
    """
    pass

#  List the top-k most resource intensive IP addresses over the last T time units.
def top_k_ip(T):
    """

    :param T:
    :return:
    """
    pass

#  List all IP addresses that are consuming more than X times the standard deviation
# of the average traffic consumption of all IP addresses over the last T time units.
def ip_x_more_than_sd(T):
    """

    :param T:
    :return:
    """
    pass



