import asyncio
import json

import requests


KAFKA_CONNECT_URL = "http://localhost:8083/connectors"
CONNECTOR_NAME = "solution3"


def configure_connector():
    """Calls Kafka Connect to create the Connector"""
    print("creating or updating kafka connect connector...")

    rest_method = requests.post
    resp = requests.get(f"{KAFKA_CONNECT_URL}/{CONNECTOR_NAME}")
    if resp.status_code == 200:
        return

    #
    # TODO: Complete the Kafka Connect Config below for a JDBC source connector.
    #       You should whitelist the `clicks` table, use incrementing mode and the
    #       incrementing column name should be id.
    #
    #       See: https://docs.confluent.io/current/connect/references/restapi.html
    #       See: https://docs.confluent.io/current/connect/kafka-connect-jdbc/source-connector/source_config_options.html
    #
    resp = rest_method(
        KAFKA_CONNECT_URL,
        headers={"Content-Type": "application/json"},
        data=json.dumps(
            {
                "name": "clicks-jdbc",  # TODO
                "config": {
                    "connector.class": "io.confluent.connect.jdbc.JdbcSourceConnector",  # TODO
                    "topic.prefix": "solution3.",  # TODO
                    "mode": "incrementing",  # TODO
                    "incrementing.column.name": "id",  # TODO
                    "table.whitelist": "clicks",  # TODO
                    "tasks.max": 1,
                    "connection.url": "jdbc:postgresql://localhost:5432/classroom",
                    "connection.user": "root",
                    "key.converter": "org.apache.kafka.connect.json.JsonConverter",
                    "key.converter.schemas.enable": "false",
                    "value.converter": "org.apache.kafka.connect.json.JsonConverter",
                    "value.converter.schemas.enable": "false",
                },
            }
        ),
    )

    # Ensure a healthy response was given
    try:
        resp.raise_for_status()
    except:
        print(f"failed creating connector: {json.dumps(resp.json(), indent=2)}")
        exit(1)
    print("connector created successfully.")
    print("Use kafka-console-consumer and kafka-topics to see data!")


if __name__ == "__main__":
    configure_connector()
