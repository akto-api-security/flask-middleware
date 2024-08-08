### Steps to add flask middleware 

1. Copy and paste the middleware.py file in your project directory.
2. In your application, where you are initializing flask app, import the middleware.py file and add the following line of code.
```python
from middleware import setup_middleware

app = Flask(__name__)
setup_middleware(app)
```
3. Add the following environment variables for the middleware to start working.
```bash
KAFKA_URL=your_kafka_url:9092 # Get the service/load balancer dns of akto-runtime
AKTO_ACCOUNT_ID=your_account_id # For on-prem setup, this will be 1000000, for cloud setup, grab the account id from Settings > About
MAX_PAYLOAD_SIZE=100000 # This is an optional parameter, default value is 100000. Request payloads greater than this size will be ignored
```
4. Add `kafka-python` dependency in your requirements.txt file.
5. Now you can start your application and the middleware will start working.

You can refer the sample_app.py to see how the middleware is integrated with a flask application.


