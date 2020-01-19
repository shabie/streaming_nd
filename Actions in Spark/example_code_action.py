from pyspark import SparkConf, SparkContext


def action_exercise():
    """
    simple action exercise
    :return:
    """
    # TODO create spark conf
    conf = SparkConf().setMaster('local[2]').setAppName('RDD Example')

    # TODO create a spark context
    sc = SparkContext(conf=conf)

    # TODO create a simple rdd, parallelize a list of numbers
    rdd = sc.parallelize([1, 2, 3, 4, 5, 6, 7, 8, 9, 10])
    
    print(type(rdd))

    # TODO create a lambda function to get sum of all numbers in the list
    print(rdd.reduce(lambda x, y: x + y))


if __name__ == "__main__":
    action_exercise()
