import pathlib

from pyspark.sql import SparkSession
import pyspark.sql.functions as psf

CITIES_CSV = './resources/lesson1/csv/cities.csv'
LINES_CSV = './resources/lesson1/csv/lines.csv'
TRACKS_CSV = './resources/lesson1/csv/tracks.csv'

def transformation_exercise():
    """
    Do an exploration on World Transit System
    Q1: How many tracks are still in operation?
    Q2: What are the names of tracks that are still in operation?
    :return:
    """
    spark = SparkSession.builder \
        .master("local") \
        .appName("transformation exercise") \
        .getOrCreate()

    # TODO import all the necessary files - the dataframe name should match the CSV files,
    # like cities_df should be corresponding to CITIES_CSV, and so on.
    cities_df = spark.read.csv(CITIES_CSV, header=True)
    lines_df = spark.read.csv(LINES_CSV, header=True)
    tracks_df = spark.read.csv(TRACKS_CSV, header=True)

    # TODO the names of the columns are confusing (two id columns, but they're not matching)
    # TODO how do we solve this problem?
    lines_df = lines_df.withColumnRenamed('name', 'city_name').select(['city_id', 'city_name'])
    left_df = cities_df.join(lines_df, cities_df.id == lines_df.city_id, "inner")

    # TODO how do you know which track is still operating?
    tracks_df.select(psf.max('closure')).distinct().show()

    # TODO filter on only the operating tracks
    filtered_df = tracks_df.filter("closure like '999999%' ")

    joined_df = left_df.join(filtered_df, left_df.city_id == filtered_df.city_id, "inner")

    # TODO Q1 and Q2 answers
    joined_df.select('city_name').distinct().count()
    joined_df.select('city_name').distinct().show(50)

if __name__ == "__main__":
    transformation_exercise()
