"""All task implementations go in sub-packages to this package"""


def get_spark_for_test():
    """
    a quick wrapper around spark object instantiation
    the below spark configuration is for purposes of running the test suite only!
    """

    from spaceprod.utils.space_context.spark import spark

    return spark


class SparkTests(object):
    _spark = None

    # ensures it is a singleton
    def __new__(cls, *args, **kwargs):
        if not cls._spark:
            cls._spark = get_spark_for_test()

        return cls._spark


spark_tests = SparkTests()
