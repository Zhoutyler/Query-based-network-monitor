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
    msg1 = [services[random.randrange(0, len(services))]
        , port_num_generator()
        , ip_addr_generator()
        , "127.0.0.1"
        , data_size_generator()]

    return "/".join(msg1) + "\n"

with open("logs.txt", "w") as f:
    cnt = 0
    while cnt <= 10000:
        log = generate()
        f.write(log)
        cnt += 1
