from flask import Flask
from flask import request
from flask import Response
import redis

import datetime
app = Flask(__name__)


@app.route('/')
def hello_world():
    return 'Hello World!'


if __name__ == '__main__':
    app.run()


r = redis.StrictRedis(
    host='localhost',
    port=6379,
    charset="utf-8", decode_responses=True)


def get_res(ts, kind, H, T):
    key = "_".join([ts, kind, H, T])
    res = r.get(key)
    return res


@app.route('/<query_id>/<H>/<T>/<X>/<k>', methods=['GET'])
def handle_request(query_id, H, T, X, k):
    resp = Response("Internal server error", status=500, mimetype="text/plain")
    '''
    query_id:
    # 1 -> top_protocol_H_T
    # 2 -> top_k_protocols_T
    # 3 -> protocols_x_more_than_stddev
    # 4 -> top_ip_addr_H_T
    # 5 -> top_k_ip_H
    # 6 -> ip_x_more_than_stddev
    '''
    res = None
    ts = datetime.datetime.now()
    try:
        if query_id == "1":
            res = get_res(ts, query_id, H, T)
        elif query_id == "2":
            res = get_res(ts, query_id, k, T)
        elif query_id == "3":
            res = get_res(ts, query_id, X, T)
        elif query_id == "4":
            res = get_res(ts, query_id, H, T)
        elif query_id == "5":
            res = get_res(ts, query_id, k, T)
        elif query_id == "6":
            res = get_res(ts, query_id, X, T)
        resp = Response(res, status=200, mimetype='application/json')

    except Exception as err:
        print(err)

    return resp

