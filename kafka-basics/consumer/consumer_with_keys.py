from confluent_kafka import Consumer

config = {
    "bootstrap.servers": "localhost:9092",
    "group.id": "kafka-python-consumer",
    "auto.offset.reset": "earliest",  # to start consuming from the earliest message, which was produced while consumer was NOT built.
}

# create a consumer - connect it to my local Kafka
c = Consumer(config)


topic = "confluent_kafka_python"
c.subscribe([topic])
while True:
    msg = c.poll(0.1)

    if msg is None:
        continue

    if msg.error():
        print(msg.error())
        continue

    print(msg.value().decode(), msg.partition(), msg.offset(), msg.key(), msg.value())
