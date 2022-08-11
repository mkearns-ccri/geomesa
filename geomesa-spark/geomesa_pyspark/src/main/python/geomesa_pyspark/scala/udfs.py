"""
Build PySpark UDFs from the Geomesa UDFs (scala UDFs).
"""
from pyspark import SparkContext
from geomesa_pyspark.scala.udf import build_scala_udf

spark_context = SparkContext.getOrCreate()
geomesa_functions = spark_context._jvm.org.locationtech.geomesa.spark.jts.udf.GeomesaPysparkFunctions

# Geometric Accessor Functions
st_boundary = build_scala_udf(spark_context, geomesa_functions.st_boundary)
st_coordDim = build_scala_udf(spark_context, geomesa_functions.st_coordDim)
st_dimension = build_scala_udf(spark_context, geomesa_functions.st_dimension)
st_envelope = build_scala_udf(spark_context, geomesa_functions.st_envelope)
st_exteriorRing = build_scala_udf(spark_context, geomesa_functions.st_exteriorRing)
st_geometryN = build_scala_udf(spark_context, geomesa_functions.st_geometryN)
st_geometryType = build_scala_udf(spark_context, geomesa_functions.st_geometryType)
st_interiorRingN = build_scala_udf(spark_context, geomesa_functions.st_interiorRingN)
st_isClosed = build_scala_udf(spark_context, geomesa_functions.st_isClosed)
st_isCollection = build_scala_udf(spark_context, geomesa_functions.st_isCollection)
st_isEmpty = build_scala_udf(spark_context, geomesa_functions.st_isEmpty)
st_isRing = build_scala_udf(spark_context, geomesa_functions.st_isRing)
st_isSimple = build_scala_udf(spark_context, geomesa_functions.st_isSimple)
st_isValid = build_scala_udf(spark_context, geomesa_functions.st_isValid)
st_numGeometries = build_scala_udf(spark_context, geomesa_functions.st_numGeometries)
st_numPoints = build_scala_udf(spark_context, geomesa_functions.st_numPoints)
st_pointN = build_scala_udf(spark_context, geomesa_functions.st_pointN)
st_x = build_scala_udf(spark_context, geomesa_functions.st_x)
st_y = build_scala_udf(spark_context, geomesa_functions.st_y)

# Geometric Cast Functions
st_castToPoint = build_scala_udf(spark_context, geomesa_functions.st_castToPoint)
st_castToPolygon = build_scala_udf(spark_context, geomesa_functions.st_castToPolygon)
st_castToLineString = build_scala_udf(spark_context, geomesa_functions.st_castToLineString)
st_castToGeometry = build_scala_udf(spark_context, geomesa_functions.st_castToGeometry)
st_byteArray = build_scala_udf(spark_context, geomesa_functions.st_byteArray)

# Geometric Constructor Functions
st_geomFromGeoHash = build_scala_udf(spark_context, geomesa_functions.st_geomFromGeoHash)
st_box2DFromGeoHash = build_scala_udf(spark_context, geomesa_functions.st_box2DFromGeoHash)
st_geomFromText = build_scala_udf(spark_context, geomesa_functions.st_geomFromText)
st_geometryFromText = build_scala_udf(spark_context, geomesa_functions.st_geometryFromText)
st_geomFromWKT = build_scala_udf(spark_context, geomesa_functions.st_geomFromWKT)
st_geomFromWKB = build_scala_udf(spark_context, geomesa_functions.st_geomFromWKB)
st_lineFromText = build_scala_udf(spark_context, geomesa_functions.st_lineFromText)
st_makeBox2D = build_scala_udf(spark_context, geomesa_functions.st_makeBox2D)
st_makeBBOX = build_scala_udf(spark_context, geomesa_functions.st_makeBBOX)
st_makePolygon = build_scala_udf(spark_context, geomesa_functions.st_makePolygon)
st_makePoint = build_scala_udf(spark_context, geomesa_functions.st_makePoint)
st_makeLine = build_scala_udf(spark_context, geomesa_functions.st_makeLine)
st_makePointM = build_scala_udf(spark_context, geomesa_functions.st_makePointM)
st_mLineFromText = build_scala_udf(spark_context, geomesa_functions.st_mLineFromText)
st_mPointFromText = build_scala_udf(spark_context, geomesa_functions.st_mPointFromText)
st_mPolyFromText = build_scala_udf(spark_context, geomesa_functions.st_mPolyFromText)
st_point = build_scala_udf(spark_context, geomesa_functions.st_point)
st_pointFromGeoHash = build_scala_udf(spark_context, geomesa_functions.st_pointFromGeoHash)
st_pointFromText = build_scala_udf(spark_context, geomesa_functions.st_pointFromText)
st_pointFromWKB = build_scala_udf(spark_context, geomesa_functions.st_pointFromWKB)
st_polygon = build_scala_udf(spark_context, geomesa_functions.st_polygon)
st_polygonFromText = build_scala_udf(spark_context, geomesa_functions.st_polygonFromText)

# Geometric Output Functions
st_asBinary = build_scala_udf(spark_context, geomesa_functions.st_asBinary)
st_asGeoJSON = build_scala_udf(spark_context, geomesa_functions.st_asGeoJSON)
st_asLatLonText = build_scala_udf(spark_context, geomesa_functions.st_asLatLonText)
st_asText = build_scala_udf(spark_context, geomesa_functions.st_asText)
st_geoHash = build_scala_udf(spark_context, geomesa_functions.st_geoHash)

# Geometric Processing Functions
st_antimeridianSafeGeom = build_scala_udf(spark_context, geomesa_functions.st_)
st_bufferPoint = build_scala_udf(spark_context, geomesa_functions.st_)

# Spatial Relation Functions
st_translate = build_scala_udf(spark_context, geomesa_functions.st_translate)
st_contains = build_scala_udf(spark_context, geomesa_functions.st_contains)
st_covers = build_scala_udf(spark_context, geomesa_functions.st_covers)
st_crosses = build_scala_udf(spark_context, geomesa_functions.st_crosses)
st_disjoint = build_scala_udf(spark_context, geomesa_functions.st_disjoint)
st_equals = build_scala_udf(spark_context, geomesa_functions.st_equals)
st_intersects = build_scala_udf(spark_context, geomesa_functions.st_intersects)
st_overlaps = build_scala_udf(spark_context, geomesa_functions.st_overlaps)
st_touches = build_scala_udf(spark_context, geomesa_functions.st_touches)
st_within = build_scala_udf(spark_context, geomesa_functions.st_within)
st_relate = build_scala_udf(spark_context, geomesa_functions.st_relate)
st_relateBool = build_scala_udf(spark_context, geomesa_functions.st_relateBool)
st_area = build_scala_udf(spark_context, geomesa_functions.st_area)
st_centroid = build_scala_udf(spark_context, geomesa_functions.st_centroid)
st_closestPoint = build_scala_udf(spark_context, geomesa_functions.st_closestPoint)
st_distance = build_scala_udf(spark_context, geomesa_functions.st_distance)
st_distanceSphere = build_scala_udf(spark_context, geomesa_functions.st_distanceSphere)
st_length = build_scala_udf(spark_context, geomesa_functions.st_length)
st_aggregateDistanceSphere = build_scala_udf(spark_context, geomesa_functions.st_aggregateDistanceSphere)
st_lengthSphere = build_scala_udf(spark_context, geomesa_functions.st_lengthSphere)
