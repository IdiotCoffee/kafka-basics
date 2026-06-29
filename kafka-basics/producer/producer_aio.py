import asyncio

from confluent_kafka.aio import AIOProducer

data = ["async", "python", "producer", "using", "asyncio"]


async def main():
    p = AIOProducer({"bootstrap.servers": "localhost:9092"})
    try:
        # produce() returns a Future; first await the coroutine to get the Future,
        # then await the Future to get the delivered Message.
        for d in data:
            delivery_future = await p.produce(
                "first_topic_ishaan", value=str(d).encode("utf-8")
            )
            delivered_msg = await delivery_future
        # Optionally flush any remaining buffered messages before shutdown
        await p.flush()
    finally:
        await p.close()


asyncio.run(main())
