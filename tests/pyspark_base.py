import sys
import unittest
from unittest.mock import MagicMock

from tests.spark_session_manager import SparkSessionManager


class PySparkTest(unittest.TestCase):
    @property
    def job_name(self):
        return

    @property
    def list_tables(self):
        return []

    tables = dict()

    @classmethod
    def setUpClass(cls):
        cls.spark = SparkSessionManager.start()

    def setUp(self) -> None:
        base_jobs = MagicMock()
        glue_context = MagicMock()
        self.args = {
            's3_path': '/tmp/read_files'
        }
        base_jobs.get_values.return_value = [self.args, self.spark, glue_context]
        sys.modules['jobs.base_jobs'] = base_jobs

        self.add_temporal_tables()

    def add_temporal_tables(self):
        for table_name in self.list_tables:
            self.tables[table_name] = self.spark.read.csv(
                f'tests/resources/{self.job_name}/{table_name}.csv', quote="'", header=True
            )
            self.tables[table_name].createOrReplaceTempView(table_name)

    def tearDown(self) -> None:
        for key, value in self.tables.items():
            self.spark.catalog.dropTempView(key)

    @staticmethod
    def df_to_dict(df):
        return [x.asDict() for x in df.collect()]
