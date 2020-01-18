
from pyspark.sql import SparkSession
import pyspark.sql.functions as psf
import pathlib


def explore_data():
    # TODO build a spark session (SparkSession is already imported for you!)
    spark = SparkSession.builder.appName("cities").getOrCreate()

    # TODO set correct path for file_path using Pathlib
    # TODO use a correct operator to load a csv file
    file_path = './resources/lesson1/csv/cities.csv'
    df = spark.read.csv(file_path, header=True)

    # view schema
    df.printSchema()

    # TODO create another dataframe, drop null columns for start_year
    # TODO select start_year and country only and get distinct values
    # TODO sort by start_year ascending

    distinct_df = df.na.drop(subset=['start_year']) \
        .select("start_year", "country") \
        .distinct() \
        .sort(psf.col("start_year").asc())

    # show distinct values
    distinct_df.show()

    # which country had the metro system the earliest?


if __name__ == "__main__":
    explore_data()