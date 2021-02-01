from unittest.mock import MagicMock

from tests.pyspark_base import PySparkTest

from jobs.python_libs import utils


class TestUtils(PySparkTest):
    def setUp(self) -> None:
        # read_sql
        self.users = self.spark.read.csv('tests/resources/utils/Users.csv', quote="'", header=True)
        self.users.createOrReplaceTempView('Users')
        self.expected_result_users = [{'name': 'nombre1', 'phone': '981232'}, {'name': 'nombre2', 'phone': '311233123'}]

    def test_read_sql(self):
        query = """
            SELECT * FROM Users
        """

        df = utils.read_sql(self.spark, query)
        test_results = [x.asDict() for x in df.collect()]
        self.assertEqual(test_results, self.expected_result_users)

    def test_df_to_parquet(self):
        df = MagicMock()
        df.count.side_effect = [2, 0]

        utils.df_to_parquet(df, '/tmp')

        utils.df_to_parquet(df, '/temporal')

        self.assertEqual(df.count.call_count, 2)
        self.assertEqual(df.write.parquet.call_count, 1)

        df.write.parquet.assert_called_once_with('/tmp', mode='overwrite')

    def test_parse_to_string(self):
        from pyspark.sql.functions import lit

        table = self.users
        table = table.withColumn('new_column', lit(10))
        result = utils.parse_to_string(table)

        self.assertEqual(result.dtypes, [('name', 'string'), ('phone', 'string'), ('new_column', 'string')])

    def tearDown(self) -> None:
        # read_sql
        self.spark.catalog.dropTempView("Users")
