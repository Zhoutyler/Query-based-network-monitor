#!/usr/local/bin/python2
from socket import *
import datetime
import random
import time
import json
import threading

def worker(conn):
    with open("logs.txt", "r") as f:
        while True:
            log = str(datetime.datetime.now()) + "/"+ f.readline()
            time.sleep(1) 
            print(log)
            conn.send(log)

servername = 'localhost'
serverPort = 9999
sock = socket(AF_INET, SOCK_STREAM)
sock.bind(("localhost", 9999))
sock.listen(2)

print("start")
while True:
    conn, addr = sock.accept()
    thd = threading.Thread(target=worker, args=(conn,))
    thd.start()
