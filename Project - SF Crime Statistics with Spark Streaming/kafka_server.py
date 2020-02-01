import producer_server


def run_kafka_server():
	# TODO get the json file path
    input_file = ""

    # TODO fill in blanks
    producer = producer_server.ProducerServer(
        input_file=input_file,
        topic="",
        bootstrap_servers="",
        client_id=""
    )

    return producer


def feed():
    producer = run_kafka_server()
    producer.generate_data()


if __name__ == "__main__":
    feed()
