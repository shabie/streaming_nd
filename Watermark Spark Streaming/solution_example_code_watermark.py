
from pyspark.sql import SparkSession
from pyspark.sql.functions import rand, expr

def join_exercise():
    spark = SparkSession.builder \
        .master("local[2]") \
        .appName("join and watermark exercise") \
        .getOrCreate()
    # setting shuffle partition to 1
    spark.conf.set("spark.sql.shuffle.partitions", "1")

    # TODO create a streaming dataframe using format('rate')
    # TODO select expressions value and timestamp
    left = spark.readStream \
        .format("rate") \
        .option("rowsPerSecond", "5") \
        .option("numPartitions", "1").load() \
        .selectExpr("value AS row_id", "timestamp AS left_timestamp")
    #   .writeStream.outputMode("append").format("console").start().awaitTermination()
    # enable writeStream line above to just run it on console and test

    # TODO create a streaming dataframe that we'll join on
    right = spark.readStream \
        .format("rate") \
        .option("rowsPerSecond", "5") \
        .option("numPartitions", "1").load() \
        .where((rand() * 100).cast("integer") < 10) \
        .selectExpr("(value - 50) AS row_id ", "timestamp AS right_timestamp") \
        .where("row_id > 0")

    # TODO join using row_id
    join_query = left.join(right, "row_id")

    # join_query.writeStream.outputMode("append").format("console").start().awaitTermination()

    # TODO use watermark of 10 seconds
    left_with_watermark = left \
        .selectExpr("row_id AS left_row_id", "left_timestamp") \
        .withWatermark("left_timestamp", "10 seconds ")

    # TODO use watermark of 20 seconds
    right_with_watermark = right \
        .selectExpr("row_id AS right_row_id", "right_timestamp") \
        .withWatermark("right_timestamp", "20 seconds")

    query = left_with_watermark.join(
        right_with_watermark,
        expr("""
        right_row_id = left_row_id AND
        right_timestamp >= left_timestamp AND
        right_timestamp <= left_timestamp + interval 1 minutes
        """), "left_outer")

    query.writeStream.outputMode("append").format("console").start().awaitTermination()

    # try with left outer
    print(query)


if __name__ == "__main__":
    join_exercise()
