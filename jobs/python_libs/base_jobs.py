import sys

from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions


def get_values(args_list):
    args = getResolvedOptions(
        sys.argv, args_list
    )

    print(args)

    sc = SparkContext.getOrCreate()
    glue_context = GlueContext(sc)
    spark_context = glue_context.spark_session
    return args, spark_context, glue_context
