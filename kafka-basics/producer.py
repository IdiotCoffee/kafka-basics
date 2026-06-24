from confluent_kafka import Producer

# create a producer - connect it to my local Kafka
p = Producer({"bootstrap.servers": "localhost:9092"})


# producer function - this produces data into Kafka
def producerKafka(err, msg):
    if err is not None:
        print("message delivery failed: ", err)
    else:
        print("Message delivered to {} [{}]".format(msg.topic(), msg.partition()))


# sample data:
data = [1, 2, 3, 4, 5, 6, 7, 8, 9, "hello", "from", "python"]


for d in data:
    # trigger any available producerKafka callbacks, from previous produce() runs - for backlog
    p.poll(0)

    # connect to a topic, convert the data into utf-8 string, use producerKafka as a callback and produce to the kafka topic.
    p.produce("first_topic_ishaan", str(d).encode("utf-8"), callback=producerKafka)

# wait for outstanding messages to be delivered, and callbacks to producerKafka to be triggered.
p.flush()
