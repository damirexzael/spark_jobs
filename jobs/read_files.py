"""
Script path: s3://621254586315.mach-big-data-edl/glue/read_files.py
IAM role: StackSet-airflow-prerequisites-RoleAirflowProduct-12CGR0NSB7T8V
Type: Spark
Glue version: Spark 2.4, Python 3
ETL language: Python
Temporary directory: s3://621254586315.mach-big-data-edl/glue/tmp
Python library path:
s3://621254586315.mach-big-data-edl/glue/base_jobs.py
Number of workers: 2
Job parameters:
--s3_path s3://621254586315.mach-big-data-edl/glue/save_data
--extra-py-files s3://621254586315.mach-big-data-edl/glue/extra_py_files/python_libs.zip,s3://621254586315.mach-big-data-edl/glue/extra_py_files/jsonschema-3.2.0-py2.py3-none-any.whl,s3://621254586315.mach-big-data-edl/glue/extra_py_files/attrs-20.3.0-py2.py3-none-any.whl,s3://621254586315.mach-big-data-edl/glue/extra_py_files/pyrsistent.zip,s3://621254586315.mach-big-data-edl/glue/extra_py_files/importlib_metadata-3.3.0-py3-none-any.whl,s3://621254586315.mach-big-data-edl/glue/extra_py_files/zipp-3.4.0-py3-none-any.whl,s3://621254586315.mach-big-data-edl/glue/extra_py_files/typing_extensions-3.7.4.3-py3-none-any.whl
--extra-jars s3://621254586315.mach-big-data-edl/glue/extra_jars/json-serde.jar
--enable-glue-datacatalog
--encryption-type sse-s3
"""
from base_jobs import get_values
from utils import read_sql, df_to_parquet


args, spark_context, glue_context = get_values(['s3_path'])

query = """
    select * from data_warehouse.users
    limit 10
"""
df = read_sql(spark_context, query)

df_to_parquet(df, args['s3_path'])
