import json
import logging
import os
import time
from io import BytesIO

from flask import request, g
from kafka import KafkaProducer

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
    producer = KafkaProducer(
        bootstrap_servers=[kafka_url],
        value_serializer=lambda x: json.dumps(x).encode('utf-8')
    )
    return producer, topic


producer, topic = get_kafka_producer()
MAX_PAYLOAD_SIZE = int(os.getenv('MAX_PAYLOAD_SIZE', '100000'))


def log_request():
    g.request_payload_too_big = request.content_length and request.content_length > MAX_PAYLOAD_SIZE
    logger.info(f"Request payload too big: {g.request_payload_too_big}")
    g.req_payload = '{}'
    if not g.request_payload_too_big:
        g.req_payload = request.get_data(as_text=True)
        request.environ['wsgi.input'] = BytesIO(g.req_payload.encode('utf-8'))


def log_response(response):
    if response.content_type and 'json' in response.content_type:
        if g.request_payload_too_big:
            logger.warning("Request payload too large, skipping payload logging.")
            return response

        resp_payload = response.get_data(as_text=True)
        log_data = {
            "akto_account_id": os.getenv("AKTO_ACCOUNT_ID"),
            "path": request.path,
            "requestHeaders": json.dumps(dict(request.headers)),
            "responseHeaders": json.dumps(dict(response.headers)),
            "method": request.method,
            "requestPayload": g.req_payload,
            "responsePayload": resp_payload,
            "ip": request.remote_addr,
            "time": int(time.time()),
            "statusCode": response.status_code,
            "type": request.environ.get('SERVER_PROTOCOL'),
            "status": "null",
            "contentType": response.headers.get('Content-Type')
        }

        try:
            producer.send(topic, value=log_data)
            logger.info("Data sent to Kafka.")
        except Exception as e:
            logger.error(f"Kafka send error: {e}")
    else:
        logger.warning("Non-JSON response, skipping logging.")
    return response


def setup_middleware(app):
    app.before_request(log_request)
    app.after_request(log_response)
