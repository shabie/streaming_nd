import faust

#
# TODO: Create the faust app with a name and broker
#
app = faust.App("hello-world-faust", broker="localhost:9092")

#
# TODO: Connect Faust to a topic
#
topic = app.topic("com.udacity.streams.clickevents")

#
# TODO: Provide an app agent to execute this function on topic event retrieval
#
@app.agent(topic)
async def clickevent(clickevents):
    async for ce in clickevents:
        print(ce)


if __name__ == "__main__":
    app.main()
