from pyspark.sql.functions import col


def read_sql(spark, query):
    return spark.sql(query)


def df_to_parquet(df, path):
    if df.count() > 0:
        df.write.parquet(path, mode='overwrite')


def parse_to_string(df):
    return df.select([col(c).cast("string") for c in df.columns])
