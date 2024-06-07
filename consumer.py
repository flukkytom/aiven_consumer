from kafka import KafkaConsumer
import json

# Define the topics
TOPICS = ["cus-transaction", "canada-transaction", "usa-transaction"]
#TOPIC_NAME = "cus-transaction" #Single Topic
CERTS_PATH = "./certs"

consumer = KafkaConsumer(
    *TOPICS, #Change of using just one Topic
    bootstrap_servers=f"kafka-transfer-olubillion.g.aivencloud.com:14283",
    client_id="CLIENT_TRANSFERS",
    group_id="AIVEN_MONEY_100",
    security_protocol="SSL",
    ssl_cafile=f"{CERTS_PATH}/ca.pem",
    ssl_certfile=f"{CERTS_PATH}/service.cert",
    ssl_keyfile=f"{CERTS_PATH}/service.key",
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

#Single Topic
#for message in consumer:
    #print("Got message: ", message.value)

#Multiple Topic display
for message in consumer:
    topic = message.topic  # Get the topic name from which the message was consumed
    msg = message.value    # Get the message content   
    print(f"Got message from {topic}: {msg}")