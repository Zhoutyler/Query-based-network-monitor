import findspark
findspark.init()

from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
'''
spark = SparkSession \
    .builder \
    .appName("Python Spark SQL basic example") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()
'''



#  List protocols that are consuming more than H percent of the total external bandwidth over the last T time units.
def top_protocol_H_T(H, T, lines):
    """
    :param H:
    :param T:
    :return:
    """



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
def top_k_ip(k, T):
    """

    :param T:
    :return:
    """
    # map into (ip, datasize)
    ips = lines.map(map_ip_datasize)
    # ips = lines.map(lambda lines: (lines.split('/')[0].split(':', 1)[1], 1))
    # reduce by ip
    ipcounts = ips.reduceByKey(lambda x, y : x + y)
    #sorted_ips = ipcounts.transform(lambda ipcounts: ipcounts.sortBy(lambda ipcounts: int(ipcounts[1]), ascending=False))
    

    #sorted_ips.pprint()
    top_k_ips = ipcounts.transform(lambda ipcounts: ipcounts.filter(lambda ipcounts: ipcounts in ipcounts.take(5)))
    # top_k_ips = ipcounts.filter(lambda ipcounts: ipcounts in ipcounts.take(5))
    top_k_ips.pprint()
    
#  List all IP addresses that are consuming more than X times the standard deviation
# of the average traffic consumption of all IP addresses over the last T time units.
def ip_x_more_than_sd(T):
    """

    :param T:
    :return:
    """
    pass

def parse_data(data):
    '''
        Args:
        data: record line like "timestamp:2019-04-24 00:20:10.310626/http/9361/src_ip:117.170.164.15/dest_ip:127.0.0.1/data_size:1066"
        
        Returns:
        [timestamp, protocol, port, src_ip, dst_ip, data_size]
    '''
    fields = data.split('/')
    timestamp = fields[0].split(':', 1)[1]
    protocol = fields[1]
    port = fields[2]
    src_ip = fields[3].split(':', 1)[1]
    dst_ip = fields[4].split(':', 1)[1]
    data_size = fields[5].split(':', 1)[1]
    return [timestamp, protocol, port, src_ip, dst_ip, data_size]

def map_ip_datasize(data):
    fields = data.split('/')
    return (fields[3].split(':', 1)[1], fields[5].split(':', 1)[1])

if __name__ == "__main__":
    sc = SparkContext("local[2]", "NetworkWordCount")
    ssc = StreamingContext(sc, 1)
    lines = ssc.socketTextStream("localhost", 9009)
    # top_protocol_H_T(0, 0, lines)
    top_k_ip(1, 0)
    ssc.start()
    ssc.awaitTermination()
