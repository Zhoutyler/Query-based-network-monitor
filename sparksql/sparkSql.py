from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import *
import datetime
import redis

sc = SparkContext("local[10]", "myapp")
sc.setLogLevel("ERROR")
ssc = StreamingContext(sc, 2)
ssc.checkpoint("checkpoint_App")

words = ssc.socketTextStream("localhost", 9999)

def getSparkSessionInstance(sparkConf):
    if ("sparkSessionSingletonInstance" not in globals()):
        globals()["sparkSessionSingletonInstance"] = SparkSession \
            .builder \
            .config(conf=sparkConf) \
            .getOrCreate()
    return globals()["sparkSessionSingletonInstance"]

#  List protocols that are consuming more than H percent of the total external bandwidth over the last T time units.
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
        df = spark.createDataFrame(rowRdd)

        # Creates a temporary view using the DataFrame
        df.createOrReplaceTempView("services")

        q = "SELECT b.protocol from " \
            "(select protocol, sum(data_size) as bw from services where unix_timestamp(current_timestamp()) - unix_timestamp(ts) < " + str(T) +" group by protocol) as b " \
            "where b.bw > (select sum(data_size) from services)/"+str(H)

        #x = "where unix_timestamp(current_timestamp()) - unix_timestamp(ts) < 20"
        # q = "select ts from services where unix_timestamp(current_timestamp()) - unix_timestamp(ts) < 5"
        # Do word count on table using SQL and print it
        logsDF = spark.sql(q)

        logsDF.show()
    except:
        pass


#  List the top-k most resource intensive protocols over the last T time units.
def top_k_protocols(time, rdd, k, T):
    """

    :param k:
    :param T:
    :return:
    """
    print("\n========= %s =========" % str(time))
    try:
        spark = getSparkSessionInstance(rdd.context.getConf())
        rowRdd = rdd.map(lambda p: p.split("/"))
        rowRdd = rowRdd.map(lambda p: Row(ts=datetime.datetime.strptime(p[0], '%Y-%m-%d %H:%M:%S.%f'),
                                          protocol=p[1], portNum=p[2], src_ip=p[3], dest_ip=p[4], data_size=p[5]))
        df = spark.createDataFrame(rowRdd)
        df.createOrReplaceTempView("services")

        q = "SELECT b.protocol, b.t from " \
            "(select protocol, count(protocol) as t from services where unix_timestamp(current_timestamp()) - unix_timestamp(ts) < " + str(T) +" group by protocol) as b order by b.t desc limit " + str(k)

        logsDF = spark.sql(q)
        logsDF.show()
    except:
        pass


# List IP addresses that are consuming more than H percent of the total external
# bandwidth over the last T time units.
def top_ip_addr_H_T(time, rdd, H, T):
    """

    :param H:
    :param T:
    :return:
    """
    print("\n========= %s =========" % str(time))
    try:
        spark = getSparkSessionInstance(rdd.context.getConf())
        rowRdd = rdd.map(lambda p: p.split("/"))
        rowRdd = rowRdd.map(lambda p: Row(ts=datetime.datetime.strptime(p[0], '%Y-%m-%d %H:%M:%S.%f'),
                                          protocol=p[1], portNum=p[2], src_ip=p[3], dest_ip=p[4], data_size=p[5]))
        df = spark.createDataFrame(rowRdd)
        df.createOrReplaceTempView("services")

        q = "SELECT b.src_ip from " \
            "(select src_ip, sum(data_size) as bw from services where unix_timestamp(current_timestamp()) - unix_timestamp(ts) < " + str(T) +" group by src_ip) as b " \
            "where b.bw > (select sum(data_size) from services)/"+str(H)

        logsDF = spark.sql(q)
        logsDF.show()
    except:
        pass

#  List the top-k most resource intensive IP addresses over the last T time units.
def top_k_ip(time, rdd, k, T):
    """

    :param T:
    :return:
    """
    print("\n========= %s =========" % str(time))
    try:
        spark = getSparkSessionInstance(rdd.context.getConf())
        rowRdd = rdd.map(lambda p: p.split("/"))
        rowRdd = rowRdd.map(lambda p: Row(ts=datetime.datetime.strptime(p[0], '%Y-%m-%d %H:%M:%S.%f'),
                                          protocol=p[1], portNum=p[2], src_ip=p[3], dest_ip=p[4], data_size=p[5]))
        df = spark.createDataFrame(rowRdd)
        df.createOrReplaceTempView("services")

        q = "SELECT b.src_ip, b.t from " \
            "(select src_ip, count(src_ip) as t from services where unix_timestamp(current_timestamp()) - unix_timestamp(ts) < " + str(T) +" group by src_ip) as b order by b.t desc limit " + str(k)

        logsDF = spark.sql(q)
        logsDF.show()
    except:
        pass


#  List all protocols that are consuming more than X times the standard deviation of
# the average traffic consumption of all protocols over the last T time units.
def protocols_x_more_than_stddev(time, rdd, X, T):
    """

    :param X:
    :param T:
    :return:
    """
    print("\n========= %s =========" % str(time))
    try:
        r = redis.StrictRedis(
            host='localhost',
            port=6379,
            charset="utf-8", decode_responses=True)
        spark = getSparkSessionInstance(rdd.context.getConf())
        rowRdd = rdd.map(lambda p: p.split("/"))
        rowRdd = rowRdd.map(lambda p: Row(ts=datetime.datetime.strptime(p[0], '%Y-%m-%d %H:%M:%S.%f'),
                                          protocol=p[1], portNum=p[2], src_ip=p[3], dest_ip=p[4], data_size=p[5]))
        df = spark.createDataFrame(rowRdd)
        df.createOrReplaceTempView("services")

        # q = "select CAST(data_size as DOUBLE) from services"

        q = "SELECT b.protocol from " \
            "(select protocol, sum(data_size) as bw from services where unix_timestamp(current_timestamp()) - unix_timestamp(ts) < " + str(T) + " group by protocol) as b " \
            "where b.bw > (select avg(data_size) from services) + (select stddev(data_size) from services)*"+str(X)

        # q = "select stddev(data_size) from services"
        logsDF = spark.sql(q)
        ll = [r["protocol"] for r in logsDF.collect()]
        dt = datetime.datetime.now()
        t = dt.strftime("%s")
        lt = [t, "5", str(X), str(T)]
        lt = "_".join(lt)
        print(ll)
        print(lt)
        r.rpush(lt, *ll)
    except Exception as err:
        print(err)

#  List all IP addresses that are consuming more than X times the standard deviation
# of the average traffic consumption of all IP addresses over the last T time units.
def ip_x_more_than_stddev(time, rdd, X, T):
    """

    :param T:
    :return:
    """
    print("\n========= %s =========" % str(time))
    try:
        r = redis.StrictRedis(
            host='localhost',
            port=6379,
            charset="utf-8", decode_responses=True)

        spark = getSparkSessionInstance(rdd.context.getConf())
        rowRdd = rdd.map(lambda p: p.split("/"))
        rowRdd = rowRdd.map(lambda p: Row(ts=datetime.datetime.strptime(p[0], '%Y-%m-%d %H:%M:%S.%f'),
                                          protocol=p[1], portNum=p[2], src_ip=p[3], dest_ip=p[4], data_size=p[5]))
        df = spark.createDataFrame(rowRdd)
        df.createOrReplaceTempView("services")

        q = "SELECT b.src_ip from " \
            "(select src_ip, sum(data_size) as bw from services where unix_timestamp(current_timestamp()) - unix_timestamp(ts) < " + str(T) + " group by src_ip) as b " \
            "where b.bw > (select avg(data_size) from services) + (select stddev(data_size) from services)*" + str(X)
        # q = "select stddev(data_size) from services"
        logsDF = spark.sql(q)
        # logsDF.show()
        ll = [r["src_ip"] for r in logsDF.collect()]
        dt = datetime.datetime.now()
        t = dt.strftime("%s")
        lt = [t, "6", str(X), str(T)]
        lt = "_".join(lt)
        print(lt)
        print(ll)
        r.rpush(lt, *ll)

    except Exception as err:
        print(err)

words1 = words.window(windowDuration=16, slideDuration=2)
words2 = words.window(windowDuration=16, slideDuration=2)
words1.foreachRDD(lambda time, x: top_k_ip(time, x, 10, 15))
words2.foreachRDD(lambda time, x: top_k_protocols(time, x, 10, 15))
ssc.start()
ssc.awaitTermination()

