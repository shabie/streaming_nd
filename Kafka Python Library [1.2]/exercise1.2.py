# PLEASE COMPLETE THE TODO ITEMS IN THIS PYTHON CODE

import asyncio

from confluent_kafka import Consumer, Producer
from confluent_kafka.admin import AdminClient, NewTopic


BROKER_URL = "PLAINTEXT://localhost:9092"
TOPIC_NAME = "my-first-python-topic"


async def produce(topic_name):
    """Produces data into the Kafka Topic"""
    # TODO: Configure the producer with `bootstrap.servers`
    #       See: https://docs.confluent.io/current/clients/confluent-kafka-python/#producer
    # p = Producer(...)

    curr_iteration = 0
    while True:
        # TODO: Produce a message to the topic
        #       See: https://docs.confluent.io/current/clients/confluent-kafka-python/#confluent_kafka.Producer.produce
        # p.produce(...)

        curr_iteration += 1
        await asyncio.sleep(1)


async def consume(topic_name):
    """Consumes data from the Kafka Topic"""
    # TODO: Configure the consumer with `bootstrap.servers` and `group.id`
    #       See: https://docs.confluent.io/current/clients/confluent-kafka-python/#consumer
    # c = Consumer(...)

    # TODO: Subscribe to the topic
    #       See: https://docs.confluent.io/current/clients/confluent-kafka-python/#confluent_kafka.Consumer.subscribe
    # c.subscribe(...)

    while True:
        # TODO: Poll for a message
        #       See: https://docs.confluent.io/current/clients/confluent-kafka-python/#confluent_kafka.Consumer.poll
        # message = ???

        # TODO: Handle the message. Remember that you should:
        #   1. Check if the message is `None`
        #   2. Check if the message has an error: https://docs.confluent.io/current/clients/confluent-kafka-python/#confluent_kafka.Message.error
        #   3. If 1 and 2 were false, print the message key and value
        #       Key: https://docs.confluent.io/current/clients/confluent-kafka-python/#confluent_kafka.Message.key
        #       Value: https://docs.confluent.io/current/clients/confluent-kafka-python/#confluent_kafka.Message.value
        #
        # if
        # elif
        # else
        #

        await asyncio.sleep(1)


async def produce_consume():
    """Runs the Producer and Consumer tasks"""
    t1 = asyncio.create_task(produce(TOPIC_NAME))
    t2 = asyncio.create_task(consume(TOPIC_NAME))
    await t1
    await t2


def main():
    """Runs the exercise"""
    # TODO: Configure the AdminClient with `bootstrap.servers`
    #       See: https://docs.confluent.io/current/clients/confluent-kafka-python/#confluent_kafka.admin.AdminClient
    # client = AdminClient(...)
    # TODO: Create a NewTopic object. Don't forget to set partitions and replication factor to 1!
    #       See: https://docs.confluent.io/current/clients/confluent-kafka-python/#confluent_kafka.admin.NewTopic
    # topic = NewTopic(...)

    # TODO: Using `client`, create the topic
    #       See: https://docs.confluent.io/current/clients/confluent-kafka-python/#confluent_kafka.admin.AdminClient.create_topics
    # client.create_topics(...)

    try:
        asyncio.run(produce_consume())
    except KeyboardInterrupt as e:
        print("shutting down")
    finally:
        # TODO: Using `client`, delete the topic
        #       See: https://docs.confluent.io/current/clients/confluent-kafka-python/#confluent_kafka.admin.AdminClient.delete_topics
        # client.delete_topics(...)
        pass


if __name__ == "__main__":
    main()