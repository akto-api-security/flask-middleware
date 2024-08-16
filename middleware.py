import json
import logging
import os
import time
from io import BytesIO

from flask import request, g
from confluent_kafka import Producer

friendlyHttpStatus = {
        '200': 'OK',
        '201': 'Created',
        '202': 'Accepted',
        '203': 'Non-Authoritative Information',
        '204': 'No Content',
        '205': 'Reset Content',
        '206': 'Partial Content',
        '300': 'Multiple Choices',
        '301': 'Moved Permanently',
        '302': 'Found',
        '303': 'See Other',
        '304': 'Not Modified',
        '305': 'Use Proxy',
        '306': 'Unused',
        '307': 'Temporary Redirect',
        '400': 'Bad Request',
        '401': 'Unauthorized',
        '402': 'Payment Required',
        '403': 'Forbidden',
        '404': 'Not Found',
        '405': 'Method Not Allowed',
        '406': 'Not Acceptable',
        '407': 'Proxy Authentication Required',
        '408': 'Request Timeout',
        '409': 'Conflict',
        '410': 'Gone',
        '411': 'Length Required',
        '412': 'Precondition Required',
        '413': 'Request Entry Too Large',
        '414': 'Request-URI Too Long',
        '415': 'Unsupported Media Type',
        '416': 'Requested Range Not Satisfiable',
        '417': 'Expectation Failed',
        '418': 'I\'m a teapot',
        '429': 'Too Many Requests',
        '500': 'Internal Server Error',
        '501': 'Not Implemented',
        '502': 'Bad Gateway',
        '503': 'Service Unavailable',
        '504': 'Gateway Timeout',
        '505': 'HTTP Version Not Supported',
    }

logging.basicConfig(level=logging.INFO, format='[%(asctime)s] %(levelname)s: %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
logger = logging.getLogger(__name__)

conf = {
    'bootstrap.servers': os.getenv('KAFKA_URL'),
    'batch.num.messages': int(os.getenv('KAFKA_BATCH_SIZE', '100')),
    'linger.ms': int(os.getenv('KAFKA_BATCH_TIMEOUT', '10000')),
    'request.timeout.ms':int(os.getenv('KAFKA_BATCH_TIMEOUT', '10000')),
    'compression.type':'zstd',
    'acks':0,
}
producer = Producer(**conf)
topic = os.getenv('KAFKA_TOPIC', 'akto.api.logs')
MAX_PAYLOAD_SIZE = int(os.getenv('MAX_PAYLOAD_SIZE', '100000'))

def log_request():
    g.request_payload_too_big = request.content_length and request.content_length > MAX_PAYLOAD_SIZE
    g.req_payload = '{}'
    if not g.request_payload_too_big:
        g.req_payload = request.get_data(as_text=True)
        request.environ['wsgi.input'] = BytesIO(g.req_payload.encode('utf-8'))

def log_response(response):
    try:
        if response.content_type and 'json' in response.content_type:
            if g.request_payload_too_big:
                logger.info("Request payload too big, skipping logging.")
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
                "time": str(int(time.time())),
                "statusCode": str(response.status_code),
                "type": request.environ.get('SERVER_PROTOCOL'),
                "status": friendlyHttpStatus[str(response.status_code)],
                "contentType": response.headers.get('Content-Type')
            }

            try:
                producer.produce(topic, value=json.dumps(log_data).encode('utf-8'))
                producer.poll(0)
            except Exception as e:
                logger.error(f"Kafka send error: {e}")
    except Exception as f:
        logger.error(f"Error in middleware : {f}")
    finally:
        return response

def setup_middleware(app):
    app.before_request(log_request)
    app.after_request(log_response)