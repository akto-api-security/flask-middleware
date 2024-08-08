import json
import logging
import os
import time
from flask import request, Response
from kafka import KafkaProducer

# Set up logging
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


class StreamWrapper:
    def __init__(self, response):
        self.response = response
        self.headers = dict(response.headers)
        self.status_code = response.status_code
        self.body = b""

    def write(self, data):
        self.body += data
        return data

    def stream(self):
        for chunk in self.response.response:
            self.write(chunk)
            yield chunk


def get_kafka_producer():
    kafka_url = os.getenv('KAFKA_URL')
    topic = os.getenv('KAFKA_TOPIC')
    # Uncomment the following lines to initialize the producer
    producer = KafkaProducer(
        bootstrap_servers=[kafka_url],
        value_serializer=lambda x: json.dumps(x).encode('utf-8')
    )
    return producer, topic


producer, topic = get_kafka_producer()
MAX_PAYLOAD_SIZE = int(os.getenv('MAX_PAYLOAD_SIZE', '100000'))


def log_request():
    logger.info(f"Request : {request.method} {request.path} {request.mimetype}")
    request.start_time = time.time()
    request.headers = dict(request.headers)

    content_length = request.content_length
    if content_length and content_length > MAX_PAYLOAD_SIZE:
        logger.warning("Request payload too large, skipping logging of payload.")
        request.body = "{}"
    else:
        if request.mimetype == 'application/json':
            request.body = request.get_data()
        elif request.mimetype == 'application/x-www-form-urlencoded':
            request.body = request.form.to_dict()
        elif request.mimetype == 'multipart/form-data':
            request.body = request.files
        else:
            request.body = request.get_data()

    logger.info(f"Request before done: {request.body}")


def log_response(response):
    logger.info(f"Response after started: {request.mimetype}")

    response_body = response.get_data()
    if len(response_body) > MAX_PAYLOAD_SIZE:
        logger.warning("Response payload too large, skipping logging of payload.")
        response_body = b"{}"

    # Ensure all parts of log_data are JSON serializable
    def make_serializable(data):
        if isinstance(data, bytes):
            return data.decode('utf-8')
        if isinstance(data, (dict, list)):
            return json.dumps(data)
        return str(data)

    log_data = {
        "akto_account_id": os.getenv("AKTO_ACCOUNT_ID"),  # replace with actual account ID
        "path": request.path,
        "requestHeaders": json.dumps(request.headers),
        "responseHeaders": json.dumps(dict(response.headers)),
        "method": request.method,
        "requestPayload": make_serializable(request.body),
        "responsePayload": make_serializable(response_body),
        "ip": request.remote_addr,
        "time": int(time.time()),
        "statusCode": response.status_code,
        "type": request.environ.get('SERVER_PROTOCOL'),
        "status": "null",
        "contentType": response.headers.get('Content-Type')
    }

    try:
        # Producer code commented out for the example
        producer.send(topic, value=log_data)
        pass
    except Exception as e:
        logger.error(f"Failed to send data to Kafka. Error: {e}")

    logger.info("logged data:" + json.dumps(log_data, indent=4))
    return response


def setup_middleware(app):
    app.before_request(log_request)
    app.after_request(log_response)
