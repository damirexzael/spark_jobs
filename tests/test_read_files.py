import shutil

from tests.pyspark_base import PySparkTest


class TestReadFiles(PySparkTest):
    job_name = 'read_files'
    list_tables = ['Users']

    def test_read_files(self):
        from jobs import read_files

        parquet_file = self.spark.read.parquet(self.args['s3_path'])
        self.assertEqual(parquet_file.show(), self.tables['Users'].show())

    def tearDown(self) -> None:
        super().setUp()

        shutil.rmtree(self.args['s3_path'])
