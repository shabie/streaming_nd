"""Methods pertaining to loading and configuring CTA "L" station data."""
import logging
from pathlib import Path

from confluent_kafka import avro

from models import Turnstile
from models.producer import Producer


logger = logging.getLogger(__name__)


class Station(Producer):
    """Defines a single station"""

    this_fp = Path(__file__)
    key_schema = avro.load(f"{this_fp.parents[0]}/schemas/arrival_key.json")

    #
    # TODO: Define this value schema in `schemas/arrival_value.json, then
    #       uncomment the line below
    #
    value_schema = avro.load(f"{this_fp.parents[0]}/schemas/arrival_value.json")

    def __init__(self,
                 station_id,
                 name,
                 color,
                 direction_a=None,
                 direction_b=None,
                 ):
        self.name = name
        station_name = (
            self.name.lower()
            .replace("/", "_and_")
            .replace(" ", "_")
            .replace("-", "_")
            .replace("'", "")
        )

        #
        # TODO: Complete the below by deciding on a topic name, number of
        #       partitions, and number of replicas
        #

        # TODO: Come up with a better topic name
        topic_name = f"org.chicago.cta.station.arrivals.v1"

        # TODO: Include/fill the following in the call to super.__init__():
        #       value_schema=Station.value_schema,
        #       num_partitions=???,
        #       num_replicas=???,

        # call the super to instantiate super's vars also incl. self.producer
        super().__init__(
            topic_name,
            key_schema=Station.key_schema,
            value_schema=Station.value_schema,
            num_partitions=3,
            num_replicas=1,
        )

        self.station_id = int(station_id)
        self.color = color
        self.dir_a = direction_a
        self.dir_b = direction_b
        self.a_train = None
        self.b_train = None
        self.turnstile = Turnstile(self)

    def run(self, train, direction, prev_station_id, prev_direction):
        """Simulates train arrivals at this station"""
        #
        #
        # TODO: Complete this function by producing an arrival message to Kafka
        #
        #

        # schemas have already been set in instance creation hence commented out
        try:
            self.producer.produce(
               topic=self.topic_name,
               key={"timestamp": self.time_millis()},
               # key_schema=Station.key_schema,
               # value_schema=Station.value_schema,
               value={
                   "station_id": self.station_id,
                   "train_id": train.train_id,
                   "direction": direction,
                   "line": self.color.name,
                   "train_status": train.status.name,
                   "prev_station_id": prev_station_id,
                   "prev_direction": prev_direction,
               },
            )
        except Exception as e:
            logger.fatal(e)
            raise

    def __str__(self):
        return "Station | {:^5} | {:<30} | Direction A: | {:^5} | departing" \
               " to {:<30} | Direction B: | {:^5} | departing to {:<30} | " \
            .format(
            self.station_id,
            self.name,
            self.a_train.train_id if self.a_train is not None else "---",
            self.dir_a.name if self.dir_a is not None else "---",
            self.b_train.train_id if self.b_train is not None else "---",
            self.dir_b.name if self.dir_b is not None else "---",
        )

    def __repr__(self):
        return str(self)

    def arrive_a(self, train, prev_station_id, prev_direction):
        """Denotes a train arrival at this station in the 'a' direction"""
        self.a_train = train
        self.run(train, "a", prev_station_id, prev_direction)

    def arrive_b(self, train, prev_station_id, prev_direction):
        """Denotes a train arrival at this station in the 'b' direction"""
        self.b_train = train
        self.run(train, "b", prev_station_id, prev_direction)

    def close(self):
        """Prepares the producer for exit by cleaning up the producer"""
        self.turnstile.close()
        super(Station, self).close()
