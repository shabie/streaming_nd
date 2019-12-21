# Please complete the TODO items in the code

import asyncio
from dataclasses import asdict, dataclass, field
import io
import json
import random

from confluent_kafka import Producer
from faker import Faker
from fastavro import parse_schema, writer

faker = Faker()

BROKER_URL = "PLAINTEXT://localhost:9092"


@dataclass
class Purchase:
    username: str = field(default_factory=faker.user_name)
    currency: str = field(default_factory=faker.currency_code)
    amount: int = field(default_factory=lambda: random.randint(100, 200000))

    # parse_schema: Returns a parsed avro schema
    schema = parse_schema({
        "type": "record",
        "name": "purchase",
        "namespace": "com.udacity.lesson3.sample2",
        "fields": [
            {"name": "username", "type": "string"},
            {"name": "currency", "type": "string"},
            {"name": "amount", "type": "int"}
        ]
    })

    def serialize(self):
        """Serializes the ClickEvent for sending to Kafka"""
        #
        # TODO: Rewrite the serializer to send data in Avro format
        #       See: https://fastavro.readthedocs.io/en/latest/schema.html?highlight=parse_schema#fastavro-schema
        #
        # HINT: Python dataclasses provide an `asdict` method that can quickly transform this
        #       instance into a dictionary!
        #       See: https://docs.python.org/3/library/dataclasses.html#dataclasses.asdict
        #
        # HINT: Use BytesIO for your output buffer. Once you have an output buffer instance, call
        #       `getvalue() to retrieve the data inside the buffer.
        #       See: https://docs.python.org/3/library/io.html?highlight=bytesio#io.BytesIO
        #
        # HINT: This exercise will not print to the console. Use the `kafka-console-consumer` to view the messages.
        #
        out = io.BytesIO()
        # writer writes it to a file-like object. We're not interested in writing to disk but in writing it out over the
        # network. So we keep it in memory using the io.BytesIO object.
        writer(out,
               Purchase.schema,  # tells writer which avro schema to use
               [asdict(self)],  # list of objects to be serialized into avro format
               )
        return out.getvalue()


async def produce(topic_name):
    """Produces data into the Kafka Topic"""
    p = Producer({"bootstrap.servers": BROKER_URL})
    while True:
        p.produce(topic_name, Purchase().serialize())
        await asyncio.sleep(1.0)


def main():
    """Checks for topic and creates the topic if it does not exist"""
    try:
        asyncio.run(produce_consume("com.udacity.lesson3.exercise2.clicks"))
    except KeyboardInterrupt as e:
        print("shutting down")


async def produce_consume(topic_name):
    """Runs the Producer and Consumer tasks"""
    await asyncio.create_task(produce(topic_name))


if __name__ == "__main__":
    main()
