import logging
from pyspark.sql import SparkSession


def run_spark_job(spark):
    #TODO read format as Kafka and add various configurations
    df = spark \
        .readStream \
        .load()

    # Show schema for the incoming resources for checks
    df.printSchema()

    agg_df = df.count()

    # TODO complete this
    # play around with processingTime to see how the progress report changes
    query =


if __name__ == "__main__":
    logger = logging.getLogger(__name__)

    spark = SparkSession \
        .builder \
        .master("local[*]") \
        .appName("StructuredStreamingSetup") \
        .getOrCreate()

    logger.info("Spark started")

    run_spark_job(spark)

    spark.stop()
