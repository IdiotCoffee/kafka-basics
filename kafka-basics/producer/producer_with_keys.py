from confluent_kafka import Producer

config = {
    "bootstrap.servers": "localhost:9092",
    # "batch.size": 400,
    # "partitioner": "random",
}

# create a producer - connect it to my local Kafka
p = Producer(config)


# this is known as a CALLBACK function - it is run upon completion of the produce() function.
def producerKafka(err, msg):
    if err is not None:
        print("message delivery failed: ", err)
    else:
        print(
            "Message delivered to {} [{}] - key: {}, value: {}".format(
                msg.topic(), msg.partition(), msg.key(), msg.value()
            )
        )


for j in range(2):
    for i in range(20):
        topic = "confluent_kafka_python"
        key = f"id_{i}"
        value = f"this_is_value_{i}"
        # trigger any available producerKafka callbacks, from previous produce() runs - for backlog
        p.poll(0)

        # connect to a topic, convert the data into utf-8 string, use producerKafka as a callback and produce to the kafka topic.
        p.produce(topic, key=key, value=value.encode("utf-8"), callback=producerKafka)

# wait for outstanding messages to be delivered, and callbacks to producerKafka to be triggered.
p.flush()
