import json

import requests


REST_PROXY_URL = "http://localhost:8082"


def get_topics():
    """Gets topics from REST Proxy"""
    # TODO: See: https://docs.confluent.io/current/kafka-rest/api.html#get--topics
    resp = requests.get(f"{REST_PROXY_URL}/topics")  # TODO

    try:
        resp.raise_for_status()
    except:
        print("Failed to get topics {json.dumps(resp.json(), indent=2)})")
        return []

    print("Fetched topics from Kafka:")
    print(json.dumps(resp.json(), indent=2))
    return resp.json()


def get_topic(topic_name):
    """Get specific details on a topic"""
    # TODO: See: https://docs.confluent.io/current/kafka-rest/api.html#get--topics
    resp = requests.get(f"{REST_PROXY_URL}/topics/{topic_name}")  # TODO

    try:
        resp.raise_for_status()
    except:
        print("Failed to get topics {json.dumps(resp.json(), indent=2)})")

    print("Fetched topics from Kafka:")
    print(json.dumps(resp.json(), indent=2))


def get_brokers():
    """Gets broker information"""
    # TODO See: https://docs.confluent.io/current/kafka-rest/api.html#get--brokers
    resp = requests.get(f"{REST_PROXY_URL}/brokers")  # TODO

    try:
        resp.raise_for_status()
    except:
        print("Failed to get brokers {json.dumps(resp.json(), indent=2)})")

    print("Fetched brokers from Kafka:")
    print(json.dumps(resp.json(), indent=2))


def get_partitions(topic_name):
    """Prints partition information for a topic"""
    # TODO: Using the above endpoints as an example, list
    #       partitions for a given topic name using the API
    #
    #       See: https://docs.confluent.io/current/kafka-rest/api.html#get--topics-(string-topic_name)-partitions
    resp = requests.get(f"{REST_PROXY_URL}/topics/{topic_name}/partitions")  # TODO

    try:
        resp.raise_for_status()
    except:
        print("Failed to get partitions {json.dumps(resp.json(), indent=2)})")

    print("Fetched partitions from Kafka:")
    print(json.dumps(resp.json(), indent=2))


if __name__ == "__main__":
    topics = get_topics()
    get_topic(topics[0])
    get_brokers()
    get_partitions(topics[-1])
