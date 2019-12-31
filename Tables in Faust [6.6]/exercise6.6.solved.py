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


app = faust.App("exercise6", broker="kafka://localhost:9092")
clickevents_topic = app.topic("com.udacity.streams.clickevents", value_type=ClickEvent)

#
# TODO: Define a uri summary table
#       See: https://faust.readthedocs.io/en/latest/userguide/tables.html#basics
#
uri_summary_table = app.Table("count_summary", default=int)


@app.agent(clickevents_topic)
async def clickevent(clickevents):
    #
    # TODO: Group By URI
    #       See: https://faust.readthedocs.io/en/latest/userguide/streams.html#group-by-repartition-the-stream
    #
    async for ce in clickevents.group_by(ClickEvent.uri):
        #
        # TODO: Use the URI as key, and add the number for each click event. Print the updated
        #       entry for each key so you can see how the table is changing.
        #       See: https://faust.readthedocs.io/en/latest/userguide/tables.html#basics
        #
        uri_summary_table[ce.uri] += ce.number
        print(f'{ce.uri}: {uri_summary_table[ce.uri]}')


if __name__ == "__main__":
    app.main()
