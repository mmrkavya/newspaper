import json
from flask import Flask, request
from predict import predict
app = Flask(__name__)
@app.route('/predict')
def index():
    url = request.args.get("url")
    try:
        a=predict(url)
        print(a)
        return a
    except Exception as e:
        print(e)
        return {'err':e}

app.run(host='0.0.0.0', port='8999')