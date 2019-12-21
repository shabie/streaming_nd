# Please complete the TODO items in the code

import asyncio
from dataclasses import asdict, dataclass, field
from io import BytesIO
import json
import random

from confluent_kafka import Producer
from faker import Faker
from fastavro import parse_schema, writer


faker = Faker()

BROKER_URL = "PLAINTEXT://localhost:9092"


@dataclass
class ClickEvent:
    email: str = field(default_factory=faker.email)
    timestamp: str = field(default_factory=faker.iso8601)
    uri: str = field(default_factory=faker.uri)
    number: int = field(default_factory=lambda: random.randint(0, 999))

    #
    # TODO: Define an Avro Schema for this ClickEvent
    #       See: https://avro.apache.org/docs/1.8.2/spec.html#schema_record
    #       See: https://fastavro.readthedocs.io/en/latest/schema.html?highlight=parse_schema#fastavro-schema
    #
    # Note: This will not produce any output, but you can use `kafka-console-consumer` to check that messages are being produced.
    #
    # schema = parse_schema(...)

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
        return json.dumps(
            {"uri": self.uri, "timestamp": self.timestamp, "email": self.email}
        )


async def produce(topic_name):
    """Produces data into the Kafka Topic"""
    p = Producer({"bootstrap.servers": BROKER_URL})
    while True:
        p.produce(topic_name, ClickEvent().serialize())
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
