# Please complete the TODO items below
from pyspark.sql import SparkSession

from pyspark.sql.functions import rand, expr


def join_exercise():

    spark = SparkSession.builder \
            .master("local[2]") \
            .appName("join and watermark exercise") \
            .getOrCreate()
    #setting shuffle partition to 1
    spark.conf.set("spark.sql.shuffle.partitions", "1")
    
#     Rate source (for testing) - Generates data at the specified number of rows per second, 
#     each output row contains a timestamp and value. Where *timestamp* is a Timestamp type 
#     containing the time of message dispatch, and *value* is of Long type containing the 
#     message count, starting from 0 as the first row. 
#     This source is intended for testing and benchmarking.

    #TODO create a streaming dataframe using format('rate')
    #TODO select expressions value and timestamp
    left = spark.readStream \
        .format('rate') \
        .option('rowsPerSecond', '5') \
        .option('numPartitions', '1').load() \
        .selectExpr('value AS row_id', 'timestamp AS left_timestamp')

    # spark.writeStream.outputMode("append").format("console").start().awaitTermination() 
    #TODO enable writeStream line above to just run it on console and test

    #TODO create a streaming dataframe that we'll join on
    right = spark.readStream \
        .format("rate")\
        .option("rowsPerSecond", "5")\
        .option("numPartitions", "1").load()\
        .where((rand() * 100).cast("integer") < 10) \
        .selectExpr("(value - 50) AS row_id ", "timestamp AS right_timestamp") \
        .where("row_id > 0")

    #TODO join using row_id
    join_query = left.join(right, "row_id")

    join_query.writeStream.outputMode("append").format("console").start().awaitTermination()


if __name__ == "__main__":
    join_exercise()
