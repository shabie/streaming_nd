"""Creates a turnstile data producer"""
import logging
from pathlib import Path

from confluent_kafka import avro

from models.producer import Producer
from models.turnstile_hardware import TurnstileHardware


logger = logging.getLogger(__name__)


class Turnstile(Producer):
    key_schema = avro.load(f"{Path(__file__).parents[0]}/schemas/turnstile_key.json")

    #
    # TODO: Define this value schema in `schemas/turnstile_value.json, then
    #       uncomment the below
    #
    value_schema = avro.load(
       f"{Path(__file__).parents[0]}/schemas/turnstile_value.json"
    )

    def __init__(self, station):
        """Create the Turnstile"""

        #
        # TODO: Complete the below by deciding on a topic name, number of
        #       partitions, and number of replicas
        #

        # TODO: Include/fill the following in the call to super.__init__():
        #       value_schema=Station.value_schema,
        #       num_partitions=???,
        #       num_replicas=???,

        super().__init__(
            # TODO: Come up with a better topic name
            topic_name="org.chicago.cta.station.turnstile.v1",
            key_schema=Turnstile.key_schema,
            value_schema=Turnstile.value_schema,
            num_partitions=3,
            num_replicas=1,
        )
        self.station = station
        self.turnstile_hardware = TurnstileHardware(station)

    def run(self, timestamp, time_step):
        """Simulates riders entering through the turnstile."""
        num_entries = self.turnstile_hardware.get_entries(timestamp, time_step)
        #
        #
        # TODO: Complete this function by emitting a message to the turnstile
        #       topic for the number of entries that were calculated
        #
        msg = f"{num_entries} riders have entered station {self.station.name}" \
              f" at {timestamp.isoformat()}"
        logger.debug(msg)

        for _ in range(num_entries):
            try:
                k = {"timestamp": self.time_millis()}
                v = {
                    "station_id": self.station.station_id,
                    "station_name": self.station.name,
                    "line": self.station.color.name,
                }
                self.producer.produce(topic=self.topic_name,
                                      key=k,
                                      value=v,
                                      )
            except Exception as e:
                logger.fatal(e)
                raise
