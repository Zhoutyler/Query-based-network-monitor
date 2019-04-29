from socket import *
import datetime
import random
import time


def ip_addr_generator():
    x = [str(random.randrange(1, 255)), str(random.randrange(0, 255)),
         str(random.randrange(0, 255)), str(random.randrange(0, 255))]
    return ".".join(x)


def port_num_generator():
    return str(random.randrange(1, 10000))


def data_size_generator():
    return str(random.randrange(1, 10000))


def generate():
    services = ["http", "tcp", "smtp", "pop3", "imap", "mysql", "ftp", "dns", "udp", "bolt"]
    msg1 = ["timestamp:"+str(datetime.datetime.now())
        , services[random.randrange(0, len(services))]
        , port_num_generator()
        , "src_ip:" + ip_addr_generator()
        , "dest_ip:127.0.0.1"
        , "data_size:" + data_size_generator()]

    return "/".join(msg1)


servername = 'localhost'
serverPort = 9009
sock = socket(AF_INET, SOCK_STREAM)
print ("Running at: %s" % serverPort)
sock.bind((servername, serverPort))
sock.listen()

con, con_addr = sock.accept()
print ("accept: %s:%s" % (con_addr[0], con_addr[1]))
while True:
    msg = generate()
    print("sending: ", msg)

    con.send((msg + '\n').encode())
    time.sleep(0.1)
    '''
    reply = sock.recv(1024)
    print('From Server:', reply)
    '''


