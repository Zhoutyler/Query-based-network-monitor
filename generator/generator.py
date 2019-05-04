from socket import *
import datetime
import random
import time
import json

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
    if random.randrange(0,5) == 1:
        msg1 = [str(datetime.datetime.now())
            , services[1]
            , "3000"
            , "0.0.0.1"
            , "127.0.0.1"
            , "1000"]
    else:
        msg1 = [str(datetime.datetime.now())
            , services[random.randrange(0, len(services))]
            , port_num_generator()
            , ip_addr_generator()
            , "127.0.0.1"
            , data_size_generator()]

    return "/".join(msg1)


servername = 'localhost'
serverPort = 9999
sock = socket(AF_INET, SOCK_STREAM)
sock.bind(("localhost", 9999))
sock.listen(1)

while True:
    print("start")
    conn, addr = sock.accept()
    while True:
        time.sleep(0.2)
        msg = generate() + "\n"
        print("sending: ", msg)
        conn.send(msg)



