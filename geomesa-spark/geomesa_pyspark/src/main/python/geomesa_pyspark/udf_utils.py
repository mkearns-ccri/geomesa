# from functools import partial
#
# from pyspark.sql.functions import col
# from pyspark.sql.column import Column, _to_java_column, _to_seq
#
#
# def scala_udf(sc, udf, col):
#     """"""
#     return Column(udf.apply(_to_seq(sc, [col], _to_java_column)))
#
# def build_scala_udf(sc, udf):
#     """"""
#     return partial(scala_udf, sc, udf())
#
#
# class GeomesaUserDefinedFunctions:
#
#     def __init__(self, sc):
#         self._sc = sc
#         self._geomesa_functions = sc._jvm.org.locationtech.geomesa.spark.jts.udf.GeomesaPysparkFunctions
#
#     def _geomesa_udf(self, udf, col):
#         """"""
#         return Column(udf.apply(_to_seq(self._sc, [col], _to_java_column)))
#
#     def _build_geomesa_udf(self, udf):
#         """"""
#         return partial(self._geomesa_udf, udf())
#
#     def st_area(self, col):
#         return build_geomesa_udf(self._sc, self._geomesa_functions.st_area)(col)

