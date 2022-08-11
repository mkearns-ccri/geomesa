from functools import partial

from pyspark import SparkContext
from pyspark.sql.column import Column, _to_java_column, _to_seq


def scala_udf(sc, udf, *cols):
    """Create Column for applying the Scala UDF."""
    return Column(udf.apply(_to_seq(sc, cols, _to_java_column)))

def build_scala_udf(sc, udf):
    """Build Scala UDF for PySpark."""
    return partial(scala_udf, sc, udf())


spark_context = SparkContext.getOrCreate()
geomesa_functions = spark_context._jvm.org.locationtech.geomesa.spark.jts.udf.GeomesaPysparkFunctions

st_area = build_scala_udf(spark_context, geomesa_functions.st_area)
st_distance = build_scala_udf(spark_context, geomesa_functions.st_distance)


class GeomesaUserDefinedFunctions:

    def __init__(self, sc):
        self._spark_context = sc
        self._geomesa_functions = sc._jvm.org.locationtech.geomesa.spark.jts.udf.GeomesaPysparkFunctions

    def st_boundary(self, col):
        return build_scala_udf(self._spark_context, self._geomesa_functions.st_boundary)(col)

    def st_coordDim(self, col):
        return build_scala_udf(self._spark_context, self._geomesa_functions.st_coordDim)(col)

    def st_area(self, col):
        return build_scala_udf(self._spark_context, self._geomesa_functions.st_area)(col)

    def st_distance(self, col1, col2):
        return build_scala_udf(self._spark_context, self._geomesa_functions.st_distance)(col1, col2)

