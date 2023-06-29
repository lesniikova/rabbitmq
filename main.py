from bson import ObjectId
from flask import Flask, jsonify
import pika
import datetime
from flask_cors import CORS
from pymongo import MongoClient
import json

app = Flask(__name__)
CORS(app)

client = MongoClient('mongodb://mongo:27017/logs')
db = client['logs']
logs_collection = db['logs']


@app.route('/logs', methods=['POST'])
def fetch_logs():
    amqp_url = 'amqp://student:student123@studentdocker.informatika.uni-mb.si:5672/'
    exchange_name = 'UPP-2'
    queue_name = 'UPP-2'

    connection = pika.BlockingConnection(pika.URLParameters(amqp_url))
    channel = connection.channel()

    channel.exchange_declare(exchange=exchange_name, exchange_type='direct')
    channel.queue_declare(queue=queue_name)
    channel.queue_bind(exchange=exchange_name, queue=queue_name, routing_key=queue_name)

    method_frame, header_frame, body = channel.basic_get(queue=queue_name)

    while method_frame:
        logs_collection.insert_one({
            "datetime": datetime.datetime.now(),
            "log": body.decode('utf-8')
        })
        channel.basic_ack(delivery_tag=method_frame.delivery_tag)
        method_frame, header_frame, body = channel.basic_get(queue=queue_name)

    connection.close()
    return "Logs fetched successfully", 200

@app.route('/logs/<date_from>/<date_to>', methods=['GET'])
def get_logs(date_from, date_to):
    date_from_obj = datetime.datetime.strptime(date_from, "%Y-%m-%d")
    date_to_obj = datetime.datetime.strptime(date_to, "%Y-%m-%d")

    logs = logs_collection.find({"datetime": {"$gte": date_from_obj, "$lte": date_to_obj}})
    logs_filtered = [
        {
            'message': log['log']
        }
        for log in logs
    ]
    return jsonify(logs_filtered), 200


@app.route('/logs', methods=['DELETE'])
def delete_logs():
    logs_collection.delete_many({})
    return "Logs deleted successfully", 200

if __name__ == '__main__':
    app.run(debug=True, host="0.0.0.0", port=9000)
