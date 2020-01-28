"""Configures a Kafka Connector for Postgres Station data"""
import json
import logging

import requests

logger = logging.getLogger(__name__)

KAFKA_CONNECT_URL = "http://localhost:8083/connectors"
CONNECTOR_NAME = "stations"


def configure_connector():
    """Starts and configures the Kafka Connect connector"""
    logging.debug("creating or updating kafka connect connector...")

    resp = requests.get(f"{KAFKA_CONNECT_URL}/{CONNECTOR_NAME}")
    if resp.status_code == 200:
        logging.debug("connector already created skipping recreation")
        return

    # TODO: Complete the Kafka Connect Config below.

    # Directions: Use the JDBC Source Connector to connect to Postgres. Load the
    # `stations` table using incrementing mode, with `stop_id` as the
    # incrementing column name. Make sure to think about what an appropriate
    # topic prefix would be, and how frequently Kafka Connect should run this
    # connector (hint: not very often!)

    connection_class = "io.confluent.connect.jdbc.JdbcSourceConnector"
    json_converter = "org.apache.kafka.connect.json.JsonConverter"

    resp = requests.post(
        KAFKA_CONNECT_URL,
        headers={"Content-Type": "application/json"},
        data=json.dumps(
            {
                "name": CONNECTOR_NAME,
                "config": {
                    "connector.class": connection_class,
                    "key.converter": json_converter,
                    "key.converter.schemas.enable": "false",
                    "value.converter": json_converter,
                    "value.converter.schemas.enable": "false",
                    "batch.max.rows": "500",
                    # TODO
                    "connection.url": "jdbc:postgresql://postgres:5432/cta",
                    # TODO
                    "connection.user": "cta_admin",
                    # TODO
                    "connection.password": "chicago",
                    # TODO
                    "table.whitelist": "stations",
                    # TODO
                    "mode": "incrementing",
                    # TODO
                    "incrementing.column.name": "stop_id",
                    # TODO
                    "topic.prefix": "org.chicago.cta.",
                    # TODO
                    "poll.interval.ms": "60000",
                },
            },
        ),
    )

    # Ensure a healthy response was given
    resp.raise_for_status()
    logging.debug("connector created successfully")


if __name__ == "__main__":
    configure_connector()
