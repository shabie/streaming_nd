
from pyspark.sql import SparkSession, Row
from pyspark.sql.types import *


def create_spark_session():
    """
    Create SparkSession
    :return:
    """

    spark = SparkSession.builder \
        .master("local[2]") \
        .appName("Schema example") \
        .getOrCreate()

    # TODO build schema for below rows
    schema = StructType([StructField('name', StringType()),
                         StructField('age', IntegerType()),
                         StructField('address', StringType()),
                         StructField('phone_number', StringType())])

    rows = [Row(name='Jake', age=33, address='123 Main Street', phone_number='111-222-3333'),
            Row(name='John', age=48, address='872 Pike Street', phone_number='8972341253')]

    # TODO create dataframe using the rows and schema
    df = spark.createDataFrame(rows, schema)

    # check if schema is created correctly
    df.printSchema()


if __name__ == "__main__":
    create_spark_session()