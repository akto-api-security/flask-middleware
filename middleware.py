import json
import logging
import os
import time
from io import BytesIO

from flask import request, g
from kafka import KafkaProducer

logging.basicConfig(level=logging.INFO, format='[%(asctime)s] %(levelname)s: %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
logger = logging.getLogger(__name__)

producer = KafkaProducer(
    bootstrap_servers=[os.getenv('KAFKA_URL')],
    value_serializer=lambda x: json.dumps(x).encode('utf-8'),
    batch_size=int(os.getenv('KAFKA_BATCH_SIZE', '999900')),
    linger_ms=int(os.getenv('KAFKA_BATCH_TIMEOUT', '10000'))
)
topic = os.getenv('KAFKA_TOPIC', 'akto.api.logs')
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
            logger.warning("Request payload too big, skipping logging.")
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
