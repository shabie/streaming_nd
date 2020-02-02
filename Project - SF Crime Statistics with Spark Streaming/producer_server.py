from kafka import KafkaProducer
import json
import time


class ProducerServer(KafkaProducer):

    def __init__(self, input_file, topic, **kwargs):
        print(kwargs)
        super().__init__(**kwargs)
        self.input_file = input_file
        self.topic = topic

    #TODO we're generating a dummy data
    def generate_data(self):
        """
        Read input JSON file from disk and produce individual serialized rows to Kafka
        """
        with open(self.input_file, "r") as f:

            # read JSON data from input file
            data = json.loads(f.read())

            for idx, row in enumerate(data):

                # serialize Python dict to string
                msg = self.serialize_json(row)
                self.send(self.topic, msg)
                time.sleep(1)

    @staticmethod
    def serialize_json(json_data):
        """
        Serialize Python dict to JSON-formatted, UTF-8 encoded string
        """
        return json.dumps(json_data).encode("utf-8")