from confluent_kafka import Producer

config = {
    "bootstrap.servers": "localhost:9092",
    "batch.size": 400,
    "partitioner": "random",
}

# create a producer - connect it to my local Kafka
p = Producer(config)


# this is known as a CALLBACK function - it is run upon completion of the produce() function.
def producerKafka(err, msg):
    if err is not None:
        print("message delivery failed: ", err)
    else:
        print("Message delivered to {} [{}]".format(msg.topic(), msg.partition()))


# sample data:
data = [1, 2, 3, 4, 5, 6, 7, 8, 9, "hello", "from", "python"]

for j in range(10):
    for i in range(30):
        # trigger any available producerKafka callbacks, from previous produce() runs - for backlog
        p.poll(0)

        # connect to a topic, convert the data into utf-8 string, use producerKafka as a callback and produce to the kafka topic.
        p.produce(
            "confluent_kafka_python", str(i).encode("utf-8"), callback=producerKafka
        )

# wait for outstanding messages to be delivered, and callbacks to producerKafka to be triggered.
p.flush()
