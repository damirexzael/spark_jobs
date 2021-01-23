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
    import sys
    if sys.argv[1] == 'start':
        print('starting Spark session')
        SparkSessionManager.start()
    elif sys.argv[1] == 'stop':
        print('stopping  Spark session')
        SparkSessionManager.stop()
    else:
        raise ValueError("Arg not start or end, call this file as 'python spark_session_manager.py start'")
