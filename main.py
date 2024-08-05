import json
import logging

from flask import Flask, request, jsonify
import os
import time

from kafka import KafkaProducer

app = Flask(__name__)

logging.basicConfig(level=logging.INFO, format='[%(asctime)s] %(levelname)s: %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
logger = logging.getLogger(__name__)


class ResponseWrapper:
    def __init__(self, response):
        self.response = response
        self.headers = dict(response.headers)
        self.status_code = response.status_code
        self.body = b""

    def write(self, data):
        self.body += data
        return data


def get_kafka_producer():
    kafka_url = os.getenv('KAFKA_URL')
    topic = os.getenv('KAFKA_TOPIC')
    producer = KafkaProducer(
        bootstrap_servers=[kafka_url],
        value_serializer=lambda x: json.dumps(x).encode('utf-8')
    )
    return producer, topic


producer, topic = get_kafka_producer()


# Middleware to log request and response details
@app.before_request
def log_request():
    logger.info(f"Request : {request.method} {request.path}")
    request.start_time = time.time()
    request.headers = dict(request.headers)
    request.body = request.get_data(as_text=True)
    logger.info(f"Request before done")


@app.after_request
def log_response(response):
    logger.info(f"Response after started")
    response_wrapper = ResponseWrapper(response)
    response.data = response_wrapper.write(response.data)
    response_body = response.data.decode('utf-8')

    log_data = {
        "akto_account_id": os.getenv("AKTO_ACCOUNT_ID"),  # replace with actual account ID
        "path": request.path,
        "requestHeaders": json.dumps(request.headers),
        "responseHeaders": json.dumps(response_wrapper.headers),
        "method": request.method,
        "requestPayload": request.body,
        "responsePayload": response_body,
        "ip": request.remote_addr,
        "time": int(time.time()),
        "statusCode": response_wrapper.status_code,
        "type": request.environ.get('SERVER_PROTOCOL'),
        "status": "null",
        "contentType": response_wrapper.headers.get('Content-Type')
    }

    try:
        producer.send(topic, value=log_data)
    except Exception as e:
        logger.error(f"Failed to send data to Kafka. Error: {e}")
    logger.info("logged data:" + json.dumps(log_data, indent=4))
    return response


# GET endpoint
@app.route('/get', methods=['GET'])
def get_endpoint():
    response_data = {
        "message": "This is a GET request",
        "status": "success"
    }
    return jsonify(response_data)


# POST endpoint
@app.route('/post', methods=['POST'])
def post_endpoint():
    data = request.json
    return jsonify(data)


# File upload endpoint
@app.route('/upload', methods=['POST'])
def upload_file():
    if 'file' not in request.files:
        return "No file part", 400

    file = request.files['file']
    if file.filename == '':
        return "No selected file", 400

    if file:
        filename = file.filename
        file.save(os.path.join('uploads', filename))
        return f"File {filename} uploaded successfully", 200


if __name__ == '__main__':
    # Ensure the upload directory exists
    os.makedirs('uploads', exist_ok=True)
    app.run(debug=True)
