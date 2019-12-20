# Please complete the TODO items in this code

import asyncio
from dataclasses import dataclass, field
import json
import random

from confluent_kafka import Consumer, Producer
from confluent_kafka.admin import AdminClient, NewTopic
from faker import Faker


faker = Faker()

BROKER_URL = "PLAINTEXT://localhost:9092"


async def produce(topic_name):
    """Produces data into the Kafka Topic"""
    p = Producer({"bootstrap.servers": BROKER_URL})
    while True:
        p.produce(topic_name, Purchase().serialize())
        await asyncio.sleep(1.0)


async def consume(topic_name):
    """Consumes data from the Kafka Topic"""
    c = Consumer({"bootstrap.servers": BROKER_URL, "group.id": "0"})
    c.subscribe([topic_name])
    while True:
        message = c.poll(1.0)
        if message is None:
            print("no message received by consumer")
        elif message.error() is not None:
            print(f"error from consumer {message.error()}")
        else:
            purchase_json = json.loads(message.value())
            try:
                print(
                    Purchase(
                        username=purchase_json["username"],
                        currency=purchase_json["currency"],
                        amount=purchase_json["amount"],
                    )
                )
            except KeyError as e:
                print(f"Failed to unpack message {e}")
        await asyncio.sleep(1.0)


def main():
    """Checks for topic and creates the topic if it does not exist"""
    client = AdminClient({"bootstrap.servers": BROKER_URL})

    try:
        asyncio.run(produce_consume("com.udacity.lesson3.exercise1.clicks"))
    except KeyboardInterrupt as e:
        print("shutting down")


async def produce_consume(topic_name):
    """Runs the Producer and Consumer tasks"""
    t1 = asyncio.create_task(produce(topic_name))
    t2 = asyncio.create_task(consume(topic_name))
    await t1
    await t2


@dataclass
class Purchase:
    username: str = field(default_factory=faker.user_name)
    currency: str = field(default_factory=faker.currency_code)
    amount: int = field(default_factory=lambda: random.randint(100, 200000))

    num_calls = 0

    def serialize(self):
        return json.dumps(
            {
                "username": self.username,
                "currency": self.currency,
                random.choice(["amount", "amount_pennies"]): self.amount,
            }
        )

    @classmethod
    def deserialize(cls, json_data):
        purchase_json = json.loads(json_data)
        return Purchase(
            username=purchase_json["username"],
            currency=purchase_json["currency"],
            amount=purchase_json["amount"],
        )


if __name__ == "__main__":
    main()
