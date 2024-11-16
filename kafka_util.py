import logging as LOG
import os

from kafka import KafkaProducer, KafkaConsumer
from kafka.errors import KafkaError

# ============== KAFKA ====================
KAFKA_BOOTSTRAP_HOST = os.getenv('KAFKA_STANDALONE_BOOTSTRAP_HOST', '192.168.1.42')
KAFKA_BOOTSTRAP_PORT = os.getenv('KAFKA_STANDALONE_BOOTSTRAP_PORT', '9092')
KAFKA_BOOTSTRAP_URL = f"{KAFKA_BOOTSTRAP_HOST}:{KAFKA_BOOTSTRAP_PORT}"


class KafkaUtil:
    @staticmethod
    def produce_kafka_topic(topic, data: str):
        LOG.info(f"Producing Kafka topic {topic}")

        producer = KafkaProducer(bootstrap_servers=[KAFKA_BOOTSTRAP_URL])

        # Asynchronous by default
        future = producer.send(topic, data.encode('utf-8'))

        # Block for 'synchronous' sends
        try:
            record_metadata = future.get(timeout=10)
            LOG.info("Producing Kafka topic finished")
            return True
        except KafkaError:
            # Decide what to do if produce request failed...
            LOG.error(f"Failed to produce Kafka topic {topic}")
            return False

        # Successful result returns assigned partition and offset
        # print(record_metadata.topic)
        # print(record_metadata.partition)
        # print(record_metadata.offset)

        # # produce keyed messages to enable hashed partitioning
        # producer.send('my-topic', key=b'foo', value=b'bar')
        #
        # # encode objects via msgpack
        # producer = KafkaProducer(value_serializer=msgpack.dumps)
        # producer.send('msgpack-topic', {'key': 'value'})
        #
        # # produce json messages
        # producer = KafkaProducer(value_serializer=lambda m: json.dumps(m).encode('ascii'))
        # producer.send('json-topic', {'key': 'value'})

        # produce asynchronously
        # for _ in range(100):
        #     producer.send('my-topic', b'msg')
        #
        # def on_send_success(record_metadata):
        #     print(record_metadata.topic)
        #     print(record_metadata.partition)
        #     print(record_metadata.offset)
        #
        # def on_send_error(excp):
        #     log.error('I am an errback', exc_info=excp)
        # handle exception

        # # produce asynchronously with callbacks
        # producer.send('my-topic', b'raw_bytes').add_callback(on_send_success).add_errback(on_send_error)
        #
        # # block until all async messages are sent
        # producer.flush()
        #
        # # configure multiple retries
        # producer = KafkaProducer(retries=5)

    @staticmethod
    def consume_kafka_topic(topic):
        consumer = KafkaConsumer(bootstrap_servers=[KAFKA_BOOTSTRAP_URL])
        consumer.subscribe([topic])
        print("Consumer: {}".format(consumer))
        for msg in consumer:
            LOG.info(f"Consumed Kafka msg: {msg}")
        print("DONE")