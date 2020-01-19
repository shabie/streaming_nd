from pyspark import SparkConf, SparkContext


def action_exercise():
    """
    simple action exercise
    :return:
    """
    conf = SparkConf().setMaster("local[2]").setAppName("RDD Example")
    sc = SparkContext(conf=conf)

    df = sc.parallelize([1, 2, 3, 4, 5, 6, 7, 8, 9, 10])

    df.reduce(lambda a, b: a + b)


if __name__ == "__main__":
    action_exercise()