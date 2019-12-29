# Please complete the TODO items in the code

from dataclasses import asdict, dataclass
import json

import faust


#
# TODO: Define a ClickEvent Record Class with an email (str), timestamp (str), uri(str),
#       and number (int)
#
#       See: https://docs.python.org/3/library/dataclasses.html
#       See: https://faust.readthedocs.io/en/latest/userguide/models.html#model-types
#
class ClickEvent:
    pass

app = faust.App("exercise2", broker="kafka://localhost:9092")

#
# TODO: Provide the key (uri) and value type to the clickevent
#
clickevents_topic = app.topic(
    "com.udacity.streams.clickevents",
    #key_type=???,
    #value_type=???,
)

@app.agent(clickevents_topic)
async def clickevent(clickevents):
    async for ce in clickevents:
        print(json.dumps(asdict(ce), indent=2))


if __name__ == "__main__":
    app.main()
