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
    key = "_".join([str(ts), kind, H, T])
    res = r.hgetall(key)
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
    ts = datetime.datetime.now()
    t = int(ts.strftime("%s"))
    try:
        while True:
            res = get_res(t, query_id, H, T)
            if len(res) > 0:
                break
            else:
                t -= 1
        resp = Response(json.dumps(res), status=200, mimetype='application/json')
    except Exception as err:
        print(err)

    return resp

if __name__ == '__main__':
    app.run()

