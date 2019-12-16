# Please complete the TODO items in this code

import asyncio

from confluent_kafka import Consumer, Producer
from confluent_kafka.admin import AdminClient, NewTopic


BROKER_URL = "PLAINTEXT://localhost:9092"


def topic_exists(client, topic_name):
    """Checks if the given topic exists"""
    # TODO: Check to see if the given topic exists
    #       See: https://docs.confluent.io/current/clients/confluent-kafka-python/#confluent_kafka.Consumer.list_topics
    return True


def create_topic(client, topic_name):
    """Creates the topic with the given topic name"""
    # TODO: Create the topic. Make sure to set the topic name, the number of partitions, the
    # replication factor. Additionally, set the config to have a cleanup policy of delete, a
    # compression type of lz4, delete retention milliseconds of 2 seconds, and a file delete delay
    # milliseconds of 2 second.
    #
    # See: https://docs.confluent.io/current/clients/confluent-kafka-python/#confluent_kafka.admin.NewTopic
    # See: https://docs.confluent.io/current/installation/configuration/topic-configs.html
    futures = client.create_topics(
        [
            # TODO
            # NewTopic(
            #    ...
            # )
        ]
    )

    for topic, future in futures.items():
        try:
            future.result()
            print("topic created")
        except Exception as e:
            print(f"failed to create topic {topic_name}: {e}")
            raise


def main():
    """Checks for topic and creates the topic if it does not exist"""
    client = AdminClient({"bootstrap.servers": BROKER_URL})

    #
    # TODO: Decide on a topic name
    #
    topic_name = ""
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
