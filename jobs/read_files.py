from .base_jobs import get_values
from .utils import read_sql, df_to_parquet


args, spark_context, glue_context = get_values(['s3_path'])

query = """
    select * from Users
"""
df = read_sql(spark_context, query)

df_to_parquet(df, args['s3_path'])
