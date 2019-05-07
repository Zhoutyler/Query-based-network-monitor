from flask import Flask
from flask_cors import CORS
from flask import Response
import redis
import json
import datetime
app = Flask(__name__)
cors = CORS(app, resources={r"/api/*": {"origins": "*"}})

@app.route('/')
def hello_world():
    return 'Hello World!'

r = redis.StrictRedis(
    host='localhost',
    port=6379,
    charset="utf-8", decode_responses=True)


def get_res(ts, kind, H, T):
    t = ts.strftime("%s")

    '''
    # FOR TESTING #
    if kind == "2":
        t = "1557156790"
    elif kind == "5":
        t = "1557186931"
    elif kind == "1":
        t = "1557121283"
    elif kind == "4":
        t = "1557121292"
    elif kind == "3":
        t = "1557189350"
    elif kind == "6":
        t = "aaaaaaaaaa"
    ###############
    '''
    key = "_".join([t, kind, H, T])
    print (key)
    res = r.hgetall(key)
    print (res)
    return res


@app.route('/api/<query_id>/<H>/<T>', methods=['GET'])
def handle_request(query_id, H, T):
    resp = Response("Internal server error", status=500, mimetype="text/plain")
    '''
    query_id:
    # 1 -> top_protocol_H_T
    # 2 -> top_k_protocols_T
    # 3 -> protocols_x_more_than_stddev
    # 4 -> top_ip_addr_H_T
    # 5 -> top_k_ip_T
    # 6 -> ip_x_more_than_stddev
    '''

    res = None
    ts = datetime.datetime.now()
    try:
        if query_id == "1":
            res = get_res(ts, query_id, H, T)
        elif query_id == "2":
            res = get_res(ts, query_id, H, T)
        elif query_id == "3":
            res = get_res(ts, query_id, H, T)
        elif query_id == "4":
            res = get_res(ts, query_id, H, T)
        elif query_id == "5":
            res = get_res(ts, query_id, H, T)
        elif query_id == "6":
            res = get_res(ts, query_id, H, T)
        print(res)
        resp = Response(json.dumps(res), status=200, mimetype='application/json')

    except Exception as err:
        print(err)

    return resp

if __name__ == '__main__':
    app.run()

