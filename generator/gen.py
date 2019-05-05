import datetime
import random
import time
import json

ip = ["132.22.11.33", "33.99.11.22", "255.77.99.11", "127.0.0.2"]

def ip_addr_generator():
    if random.randrange(0,2) == 1:
        return ip[random.randrange(0, 4)]
    else:
        x = [str(random.randrange(1, 255)), str(random.randrange(0, 255)),
        str(random.randrange(0, 255)), str(random.randrange(0, 255))]
        return ".".join(x)


def port_num_generator():
    return str(random.randrange(1, 10000))


def data_size_generator():
    return str(random.randrange(1, 10000))

msg0 = ["udp/8855/184.84.159.134/127.0.0.1/307"
        ,"imap/1874/55.26.107.24/127.0.0.1/1572"
        ,"http/3607/81.106.0.240/127.0.0.1/5968"
        ,"mysql/9691/124.160.230.223/127.0.0.1/3893"]

def generate():
    if random.randrange(0,2) == 1:
        services = ["http", "tcp", "smtp", "pop3", "imap", "mysql", "ftp", "dns", "udp", "bolt"]
        msg1 = [services[random.randrange(0, len(services))]
            , port_num_generator()
            , ip_addr_generator()
            , "127.0.0.1"
            , data_size_generator()]

        return "/".join(msg1) + "\n"
    else:
        return msg0[random.randrange(0, 4)] + "\n"

with open("logs.txt", "w") as f:
    cnt = 0
    while cnt <= 10000:
        log = generate()
        f.write(log)
        cnt += 1
