# Simulating a Pub/Sub system

def publish_message(topic, message):
    print(f"Publishing to topic: {topic}")
    print(f"Message: {message}")

def subscriber(message):
    print("Subscriber received message")
    print("Processing message...")
    print("Loading data into BigQuery raw table")

# Simulate producer
order_event = {
    "order_id": 5,
    "user_id": 104,
    "amount": 299.00
}

publish_message("orders-topic", order_event)

# Simulate subscriber receiving the message
subscriber(order_event)
