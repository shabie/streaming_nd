# Please complete the TODO items below

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import pathlib


def run_spark_streaming():
    """
    In this lesson, we're going to import a static json file first to explore the data
    Then load that static data as streaming data
    and show in our terminal
    :return:
    """
    spark = SparkSession.builder \
            .master("local") \
            .appName("spark streaming example") \
            .getOrCreate()


    # read the data file
    file_path = './resources/lesson1/json/data.txt'
    df = spark.read.json(file_path)

    df.show(10, False)

    # TODO based on the df.show, build schema
    jsonSchema = StructType([
        StructField('status', StringType(), True),
        StructField('timestamp', TimestampType(), True),
    ])

    # TODO create streamning input dataframe that reads the same dataset, with jsonSchema you created
    streamingInputDF = (
        spark
        .readStream
        .schema(jsonSchema)  # Set the schema of the JSON data
        .option('maxFilesPerTrigger', 1)  # Treat a sequence of files as a stream by picking one file at a time
        .json(file_path)
    )

    # This will be given
    streamingCountsDF = (
        streamingInputDF
            .groupBy(
            streamingInputDF.status,
            window(streamingInputDF.timestamp, "1 hour"))
            .count()
    )

    streamingCountsDF.isStreaming

    # TODO Write query output to console
    query = (
        streamingCountsDF
        .writeStream
        .format('console')
        .queryName('counts')
        .outputMode('complete')
        .start()

    )

if __name__ == "__main__":
    run_spark_streaming()