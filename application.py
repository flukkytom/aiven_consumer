from flask import Flask
import threading
import consumer

app = Flask(__name__)

def run_consumer():
    consumer.consume_messages()

if __name__ == '__main__':
    # Start the consumer thread
    threading.Thread(target=run_consumer).start()
    # Run the Flask app
    app.run(host='0.0.0.0', port=5002)
