from confluent_kafka import Consumer, KafkaException, KafkaError

# Configuration settings for the consumer
conf = {
    'bootstrap.servers': '192.168.1.42:29092',  # Kafka server address
    'group.id': 'my-consumer-group',  # Consumer group id
    'auto.offset.reset': 'earliest'  # Start from the earliest offset
}

# Create Kafka consumer instance
consumer = Consumer(conf)


# Function to process the consumed message
def process_message(message):
    print(f"Consumed message: {message.value().decode('utf-8')}")


# Subscribe to the topic
topic = 'first_topic'  # Replace with your Kafka topic name
consumer.subscribe([topic])

try:
    # Poll for messages
    while True:
        msg = consumer.poll(timeout=1.0)  # Adjust timeout as needed
        if msg is None:
            continue
        elif msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                print(f"End of partition reached: {msg.topic()} [{msg.partition()}] @ offset {msg.offset()}")
            else:
                raise KafkaException(msg.error())
        else:
            # Process the message if no error
            process_message(msg)
except KeyboardInterrupt:
    print("Consumer interrupted")
finally:
    # Close the consumer connection when done
    consumer.close()
