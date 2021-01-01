import logging

from pyspark.sql import SparkSession


class SparkSessionManager:
    def __init__(self):
        self.spark = None

    @staticmethod
    def suppress_py4j_logging():
        logger = logging.getLogger('py4j')
        logger.setLevel(logging.WARN)

    @staticmethod
    def create_testing_pyspark_session():
        return (SparkSession.builder
                .master('local')
                .appName('pyspark-test')
                .enableHiveSupport()
                .getOrCreate())

    @staticmethod
    def start():
        SparkSessionManager.suppress_py4j_logging()
        spark = SparkSessionManager.create_testing_pyspark_session()
        return spark

    @staticmethod
    def stop():
        spark = SparkSessionManager.create_testing_pyspark_session()
        spark.stop()


if __name__ == '__main__':
    SparkSessionManager.stop()
