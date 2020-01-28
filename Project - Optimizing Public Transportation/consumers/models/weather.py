"""Contains functionality related to Weather"""
import logging


logger = logging.getLogger(__name__)


class Weather:
    """Defines the Weather model"""

    def __init__(self):
        """Creates the weather model"""
        self.temperature = 70.0
        self.status = "sunny"

    def process_message(self, message):
        """Handles incoming weather data"""

        # TODO: Process incoming weather messages. Set temperature and status.

        logger.debug("Processing incoming weather message")
        value = message.value()
        self.temperature = value["temperature"]
        self.status = value["status"]
        msg = f"weather is now {self.temperature} and " \
              f"{self.status.replace('_', ' ')}"
        logger.debug(msg)
