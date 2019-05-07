from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import *
import datetime
import time
import redis

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
    print("\n========= %s =========" % str(time))
    try:
        r = redis.StrictRedis(
            host='localhost',
            port=6379,
            charset="utf-8", decode_responses=True)
        spark = getSparkSessionInstance(rdd.context.getConf())
        
        rowRdd = rdd.map(lambda p: p.split("/"))
        unix_cur_time = int(time.timestamp())
        rowRdd = rowRdd.filter(lambda p: unix_cur_time - int(datetime.datetime.strptime(p[0], '%Y-%m-%d %H:%M:%S.%f').timestamp()) <= T) \
            .map(lambda p: Row(protocol=p[1], data_size=p[5]))
        
        df = spark.createDataFrame(rowRdd)
        df.createOrReplaceTempView("services")
        q = '''
        SELECT b.protocol, b.bw FROM 
                 (SELECT protocol, sum(data_size) as bw FROM services GROUP BY protocol) as b 
                 WHERE b.bw > (
                     SELECT sum(data_size) FROM services) * %f''' % H
        logsDF = spark.sql(q)
        
        ll = [r["protocol"] for r in logsDF.collect()]
        dt = datetime.datetime.now()
        t = dt.strftime("%s")
        lt = [t, "1", str(H), str(T)]
        lt = "_".join(lt)
        print(ll)
        print(lt)
        r.rpush(lt, *ll)
    except Exception as e:
        print (e)

    
#  List the top-k most resource intensive protocols over the last T time units.
def top_k_protocols(time, rdd, k, T):
    """

    :param k:
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
        unix_cur_time = int(time.timestamp())
        rowRdd = rowRdd.filter(lambda p: unix_cur_time - int(datetime.datetime.strptime(p[0], '%Y-%m-%d %H:%M:%S.%f').timestamp()) <= T) \
            .map(lambda p: Row(protocol=p[1]))
        
        df = spark.createDataFrame(rowRdd)
        df.createOrReplaceTempView("services")
        q = '''
        SELECT b.protocol, b.t FROM 
                 (SELECT protocol, count(protocol) as t FROM services GROUP BY protocol) as b 
                 ORDER BY b.t DESC LIMIT %d''' % k
        logsDF = spark.sql(q)
        # ll = [r["protocol"] for r in logsDF.collect()]
        d = {r["protocol"]: r["t"] for r in logsDF.collect()}
        dt = datetime.datetime.now()
        t = dt.strftime("%s")
        lt = [t, "2", str(k), str(T)]
        lt = "_".join(lt)
        print(d)
        print(lt)
        r.hmset(lt, d)
    except Exception as err:
        print(err)
        


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
        r = redis.StrictRedis(
            host='localhost',
            port=6379,
            charset="utf-8", decode_responses=True)
        spark = getSparkSessionInstance(rdd.context.getConf())
        
        rowRdd = rdd.map(lambda p: p.split("/"))
        unix_cur_time = int(time.timestamp())
        rowRdd = rowRdd.filter(lambda p: unix_cur_time - int(datetime.datetime.strptime(p[0], '%Y-%m-%d %H:%M:%S.%f').timestamp()) <= T) \
            .map(lambda p: Row(src_ip=p[3], data_size=p[5]))
        
        df = spark.createDataFrame(rowRdd)
        df.createOrReplaceTempView("services")
        q = '''
        SELECT b.src_ip, b.bw FROM 
                 (SELECT src_ip, sum(data_size) as bw FROM services GROUP BY src_ip) as b 
                 WHERE b.bw > (
                     SELECT sum(data_size) FROM services) * %d''' % H
        logsDF = spark.sql(q)
        
        ll = [r["src_ip"] for r in logsDF.collect()]
        dt = datetime.datetime.now()
        t = dt.strftime("%s")
        lt = [t, "4", str(H), str(T)]
        lt = "_".join(lt)
        print(ll)
        print(lt)
        r.rpush(lt, *ll)
    except Exception as e:
        print (e)

#  List the top-k most resource intensive IP addresses over the last T time units.
def top_k_ip(time, rdd, k, T):
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
        unix_cur_time = int(time.timestamp())
        rowRdd = rowRdd.filter(lambda p: unix_cur_time - int(datetime.datetime.strptime(p[0], '%Y-%m-%d %H:%M:%S.%f').timestamp()) <= T) \
            .map(lambda p: Row(src_ip=p[3]))
        
        df = spark.createDataFrame(rowRdd)
        df.createOrReplaceTempView("services")
        q = '''
        SELECT b.src_ip, b.t FROM 
                 (SELECT src_ip, count(src_ip) as t FROM services GROUP BY src_ip) as b 
                 ORDER BY b.t DESC LIMIT %d''' % k
        logsDF = spark.sql(q)
        d = {r["src_ip"]: r["t"] for r in logsDF.collect()}

        dt = datetime.datetime.now()
        t = dt.strftime("%s")
        lt = [t, "5", str(k), str(T)]
        lt = "_".join(lt)
        print(d)
        print(lt)
        r.hmset(lt, d)
    except Exception as err:
        print(err)


#  List all protocols that are consuming more than X times the standard deviation of
# the average traffic consumption of all protocols over the last T time units.
def protocols_x_more_than_stddev(time, rdd, X, T):
    print("\n========= %s =========" % str(time))
    try:
        r = redis.StrictRedis(
            host='localhost',
            port=6379,
            charset="utf-8", decode_responses=True)
        spark = getSparkSessionInstance(rdd.context.getConf())
        rowRdd = rdd.map(lambda p: p.split("/"))
        rowRdd = rowRdd.map(lambda p: Row(ts=datetime.datetime.strptime(p[0], '%Y-%m-%d %H:%M:%S.%f'),
                                          protocol=p[1], data_size=p[5]))
        df = spark.createDataFrame(rowRdd)
        df.createOrReplaceTempView("services")

        # q = "select CAST(data_size as DOUBLE) from services"

        q = "SELECT b.protocol, b.bw from " \
            "(select protocol, sum(data_size) as bw from services where unix_timestamp(current_timestamp()) - unix_timestamp(ts) < " + str(T) + " group by protocol) as b " \
            "where b.bw > (select avg(data_size) from services) + (select stddev(data_size) from services)*"+str(X)

        # q = "select stddev(data_size) from services"
        logsDF = spark.sql(q)
        d = {r["protocol"]: r["bw"] for r in logsDF.collect()}
        dt = datetime.datetime.now()
        t = dt.strftime("%s")
        lt = [t, "3", str(X), str(T)]
        lt = "_".join(lt)
        print(d)
        print(lt)
        r.hmset(lt, d)
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
                                          src_ip=p[3], data_size=p[5]))
        df = spark.createDataFrame(rowRdd)
        df.createOrReplaceTempView("services")

        q = "SELECT b.src_ip, b.bw from " \
            "(select src_ip, sum(data_size) as bw from services where unix_timestamp(current_timestamp()) - unix_timestamp(ts) < " + str(T) + " group by src_ip) as b " \
            "where b.bw > (select avg(data_size) from services) + (select stddev(data_size) from services)*" + str(X)
        # q = "select stddev(data_size) from services"
        logsDF = spark.sql(q)
        # logsDF.show()
        d = {r["src_ip"]: r["bw"] for r in logsDF.collect()}
        dt = datetime.datetime.now()
        t = dt.strftime("%s")
        lt = [t, "6", str(X), str(T)]
        lt = "_".join(lt)
        print(lt)
        print(d)
        r.hmget(lt, d)

    except Exception as err:
        print(err)