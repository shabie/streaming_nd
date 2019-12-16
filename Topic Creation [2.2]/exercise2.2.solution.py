import asyncio

from confluent_kafka import Consumer, Producer
from confluent_kafka.admin import AdminClient, NewTopic


BROKER_URL = "PLAINTEXT://localhost:9092"


def topic_exists(client, topic_name):
    """Checks if the given topic exists"""
    topic_metadata = client.list_topics(timeout=5)
    return topic_name in set(t.topic for t in iter(topic_metadata.topics.values()))


def create_topic(client, topic_name):
    """Creates the topic with the given topic name"""
    futures = client.create_topics(
        [
            NewTopic(
                topic=topic_name,
                num_partitions=10,
                replication_factor=1,
                config={
                    "cleanup.policy": "delete",
                    "compression.type": "lz4",
                    "delete.retention.ms": "2000",
                    "file.delete.delay.ms": "2000",
                },
            )
        ]
    )

    for topic, future in futures.items():
        try:
            future.result()
            print("topic created")
        except Exception as e:
            print(f"failed to create topic {topic_name}: {e}")


def main():
    """Checks for topic and creates the topic if it does not exist"""
    client = AdminClient({"bootstrap.servers": BROKER_URL})

    topic_name = "org.udacity.lesson2.views"
    exists = topic_exists(client, topic_name)
    print(f"Topic {topic_name} exists: {exists}")

    if exists is False:
        create_topic(client, topic_name)

    try:
        asyncio.run(produce_consume(topic_name))
    except KeyboardInterrupt as e:
        print("shutting down")


async def produce_consume(topic_name):
    """Runs the Producer and Consumer tasks"""
    t1 = asyncio.create_task(produce(topic_name))
    t2 = asyncio.create_task(consume(topic_name))
    await t1
    await t2


async def produce(topic_name):
    """Produces data into the Kafka Topic"""
    p = Producer({"bootstrap.servers": BROKER_URL})

    curr_iteration = 0
    while True:
        p.produce(topic_name, f"iteration {curr_iteration}".encode("utf-8"))
        curr_iteration += 1
        await asyncio.sleep(0.5)


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
            print(f"consumed message {message.key()}: {message.value()}")
        await asyncio.sleep(2.5)


if __name__ == "__main__":
    main()

