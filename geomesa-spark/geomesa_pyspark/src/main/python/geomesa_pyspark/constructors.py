"""
SparkSQL Functions: Geometry Constructors.
"""

import shapely as shp

from pyspark.sql.functions import udf
from geomesa_pyspark.types import *


def _st_box2DFromGeoHash(geo_hash, precision):
    """Shapely BBOX 2D from GeoHash."""
    pass

def _st_geomFromGeoHash(geo_hash, precision):
    """Shapely Geometry from GeoHash."""
    pass

def _st_geomFromGeoJSON(geo_json):
    """Shapely Geometry from GeoJSON."""
    return shp.geometry.shape(geo_json)

def _st_geomFromWKB(wkb):
    """Shapely Geometry from Well-Known Binary."""
    return shp.wkb.loads(wkb)

def _st_geomFromWKT(wkt):
    """Shapely Geometry from Well-Known Text."""
    return shp.wkt.loads(wkt)

def _st_geomFromText(text):
    """Shapely Geometry from Text. Alias for st_geomFromWKT."""
    return shp.wkt.loads(text)

def _st_geometryFromText(text):
    """Shapely Geometry from Text. Alias for st_geomFromWKT."""
    return shp.wkt.loads(text)

def _st_lineFromText(text):
    """Shapely LineString from Text."""
    return shp.wkt.loads(text)

def _st_mLineFromText(text):
    """Shapely MultiLineString from Text."""
    return shp.wkt.loads(text)

def _st_mPointFromText(text):
    """Shapely MultiPoint from Text."""
    return shp.wkt.loads(text)

def _st_mPolyFromText(text):
    """Shapely MultiPolygon from Text."""
    return shp.wkt.loads(text)

def _st_makeBBOX(lower_x, lower_y, upper_x, upper_y):
    """Shapely BBOX from lower/upper (x, y) Coordinates."""
    return shp.geometry.box(lower_x, lower_y, upper_x, upper_y)

def _st_makeBox2D(lower_left, upper_right):
    """Shapely BBOX from lower/upper Points."""
    return shp.geometry.box(lower_left.x, lower_left.y, upper_right.x, upper_right.y)

def _st_makeLine(points):
    """Shapely LineString from sequence of Points."""
    return shp.geometry.LineString(points)

def _st_makePoint(x, y):
    """Shapely Point from (x, y) Coordinates."""
    return shp.geometry.Point(x, y)

def _st_makePointM(x, y, m):
    """Shapely Point from (x, y, m) Coordinates."""
    point = shp.geometry.Point(x, y)
    point.m = m
    return point

def _st_makePolygon(shell):
    """Shapely Polygon from a closed LineString."""
    return shp.geometry.Polygon(shell)

def _st_point(x, y):
    """Shapely Point from (x, y) Coordinates."""
    return shp.geometry.Point(x, y)

def _st_pointFromGeoHash(geo_hash):
    pass

def _st_pointFromText(text):
    """Shapely Point from Text."""
    return shp.wkt.loads(text)

def _st_pointFromWKB(wkb):
    """Shapely Point from Well-Known Binary."""
    return shp.wkb.loads(wkb)

def _st_polygon(shell):
    """Shapely Polygon from a closed LineString."""
    return shp.geometry.Polygon(shell)

def _st_polygonFromText(text):
    """Shapely Polygon from Text."""
    return shp.wkt.loads(text)


st_box2DFromGeoHash = udf(_st_box2DFromGeoHash, GeometryUDT())
st_geomFromGeoHash = udf(_st_geomFromGeoHash, GeometryUDT())
st_geomFromGeoJSON = udf(_st_geomFromGeoJSON, GeometryUDT())
st_geomFromText = udf(_st_geomFromText, GeometryUDT())
st_geomFromWKB = udf(_st_geomFromWKB, GeometryUDT())
st_geomFromWKT = udf(_st_geomFromWKT, GeometryUDT())
st_geometryFromText = udf(_st_geometryFromText, GeometryUDT())
st_lineFromText = udf(_st_lineFromText, LineStringUDT())
st_mLineFromText = udf(_st_mLineFromText, MultiLineStringUDT())
st_mPointFromText = udf(_st_mPointFromText, MultiPointUDT())
st_mPolyFromText = udf(_st_mPolyFromText, MultiPolygonUDT())
st_makeBBOX = udf(_st_makeBBOX, GeometryUDT())
st_makeBox2D = udf(_st_makeBox2D, GeometryUDT())
st_makeLine = udf(_st_makeLine, LineStringUDT())
st_makePoint = udf(_st_makePoint, PointUDT())
st_makePointM = udf(_st_makePointM, PointUDT())
st_makePolygon = udf(_st_makePolygon, PolygonUDT())
st_point = udf(_st_point, PointUDT())
st_pointFromGeoHash = udf(_st_pointFromGeoHash, PointUDT())
st_pointFromText = udf(_st_pointFromText, PointUDT())
st_pointFromWKB = udf(_st_pointFromWKB, PointUDT())
st_polygon = udf(_st_polygon, PolygonUDT())
st_polygonFromText = udf(_st_polygonFromText, PolygonUDT())
