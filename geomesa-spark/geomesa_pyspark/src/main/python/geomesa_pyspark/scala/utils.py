"""
Utility functions for creating Pyspark UDFs (from Scala UDFs) that run on the JVM.
"""
from functools import partial
from pyspark.sql.column import Column, _to_java_column, _to_seq

def scala_udf(sc, udf, *cols):
    """Create Column for applying the Scala UDF."""
    return Column(udf.apply(_to_seq(sc, cols, _to_java_column)))

def build_scala_udf(sc, udf):
    """Build Scala UDF for PySpark."""
    return partial(scala_udf, sc, udf())
