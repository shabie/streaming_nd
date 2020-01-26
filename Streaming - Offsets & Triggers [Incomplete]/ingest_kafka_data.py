import logging
from pyspark.sql import SparkSession


def run_spark_job(spark):
    #TODO set this entry point so that you can start ingesting kafka data
    df = spark \
        .readStream \

    # Show schema for the incoming resources for checks
    df.printSchema()


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
