# Please complete the TODO items in the code

from dataclasses import dataclass, field
import json
import random
from datetime import datetime
import asyncio

from confluent_kafka import Consumer, Producer
from confluent_kafka.admin import AdminClient, NewTopic
from faker import Faker


faker = Faker()

BROKER_URL = "PLAINTEXT://localhost:9092"
TOPIC_NAME = "org.udacity.exercise4.purchases"


def produce(topic_name):
    """Produces data synchronously into the Kafka Topic"""
    #
    # TODO: Configure the Producer to:
    #       1. Have a Client ID
    #       2. Have a batch size of 100
    #       3. A Linger Milliseconds of 1 second
    #       4. LZ4 Compression
    #
    #       See: https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md
    #
    p = Producer(
        {
            "bootstrap.servers": BROKER_URL,
            "linger.ms": 500,
            "batch.num.messages": 100,
            # "queue.buffering.max.messages": 10000000,
            # "compression.type": "lz4"
        }
    )
    start_time = datetime.utcnow()
    curr_iteration = 0

    while True:
        p.produce(topic_name, f'iteration: {curr_iteration}')
        if curr_iteration % 1e6 == 0.:
            elapsed = (datetime.utcnow() - start_time).seconds
            print(f"Messages sent: {curr_iteration} | Total elapsed seconds: {elapsed}")
        curr_iteration += 1
        p.poll(0)


def main():
    """Checks for topic and creates the topic if it does not exist"""
    client = AdminClient({'bootstrap.servers': BROKER_URL})
    try:
        asyncio.run(produce_consume("com.udacity.lesson2.sample4.purchases"))
    except KeyboardInterrupt as e:
        print("shutting down")


async def produce_consume(topic_name):
    """Runs Producer and Consumer tasks"""
    t1 = asyncio.create_task(produce(topic_name))
    t2 = asyncio.create_task(consume(topic_name))
    await t1
    await t2


async def consume(topic_name):
    """Consumes data from Kafka Topic"""
    c = Consumer({
        "bootstrap.servers": BROKER_URL,
        "group.id": "0",
    })
    c.subscribe([topic_name])
    while True:
        message = c.poll(0)
        if message is None:
            print("no message received by consumer")
        elif message.error() is not None:
            print(f'error from consumer {message.error()}')
        else:
            print(f'consumed message {message.key()}: {message.value()}')
        await asyncio.sleep(0.1)


def create_topic(client):
    """Creates the topic with the given topic name"""
    client = AdminClient({"bootstrap.servers": BROKER_URL})
    futures = client.create_topics(
        [NewTopic(topic=TOPIC_NAME, num_partitions=5, replication_factor=1)]
    )
    for _, future in futures.items():
        try:
            future.result()
        except Exception as e:
            pass


@dataclass
class Purchase:
    username: str = field(default_factory=faker.user_name)
    currency: str = field(default_factory=faker.currency_code)
    amount: int = field(default_factory=lambda: random.randint(100, 200000))

    def serialize(self):
        """Serializes the object in JSON string format"""
        return json.dumps(
            {
                "username": self.username,
                "currency": self.currency,
                "amount": self.amount,
            }
        )


if __name__ == "__main__":
    main()
