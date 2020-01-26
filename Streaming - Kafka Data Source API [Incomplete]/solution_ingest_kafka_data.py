import logging
from pyspark.sql import SparkSession

def run_spark_job(spark):

    df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:<your port>") \
        .option("subscribe", "<your topic name>") \
        .option("startingOffsets", "earliest") \
        .option("maxOffsetsPerTrigger", 10) \
        .option("stopGracefullyOnShutdown", "true") \
        .load()

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
