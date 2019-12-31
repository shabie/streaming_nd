# Please complete the TODO items in the code

from dataclasses import asdict, dataclass
import json
import random

import faust


@dataclass
class ClickEvent(faust.Record):
    email: str
    timestamp: str
    uri: str
    number: int
    score: int = 0


#
# TODO: Define a scoring function for incoming ClickEvents.
#       It doens't matter _how_ you score the incoming records, just perform
#       some modification of the `ClickEvent.score` field and return the value
#
#def add_score(...):


app = faust.App("exercise5", broker="kafka://localhost:9092")
clickevents_topic = app.topic("com.udacity.streams.clickevents", value_type=ClickEvent)
scored_topic = app.topic(
    "com.udacity.streams.clickevents.scored",
    key_type=str,
    value_type=ClickEvent,
)

@app.agent(clickevents_topic)
async def clickevent(clickevents):
    #
    # TODO: Add the `add_score` processor to the incoming clickevents
    #       See: https://faust.readthedocs.io/en/latest/reference/faust.streams.html?highlight=add_processor#faust.streams.Stream.add_processor
    #

    async for ce in clickevents:
        await scored_topic.send(key=ce.uri, value=ce)

if __name__ == "__main__":
    app.main()
