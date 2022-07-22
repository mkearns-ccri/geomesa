"""
SparkSQL Functions: Utility Functions.
"""
import shapely as shp

from shapely.geometry import Point, MultiPoint, LineString, MultiLineString, Polygon, MultiPolygon, LinearRing, GeometryCollection

from pyspark.sql.types import StringType, BinaryType
from pyspark.sql.functions import udf
from geomesa_pyspark.types import *


def _st_castToLineString(g):
    """Casts Geometry g to a LineString."""
    pass

def _st_castToPoint(g):
    """Casts Geometry g to a Point."""
    pass

def _st_castToPolygon(g):
    """Casts Geometry g to a Polygon."""
    pass

def _st_castToGeometry(g):
    """Casts Geometry subclass g to a Geometry. This can be necessary e.g. 
    when storing the output of st_makePoint as a Geometry in a case class."""
    pass

def _st_byteArray(s):
    """Encodes string s into an array of bytes using the UTF-8 charset."""
    pass

def _st_translate(geom, deltaX, deltaY):
    """Returns the Geometry produced when geom is translated by deltaX and deltaY."""
    pass

def _st_asBinary(geom):
    """Returns Geometry geom in WKB representation."""
    pass

def _st_asGeoJSON(geom):
    """Returns Geometry geom in GeoJSON representation."""
    pass

def _st_asLatLonText(p):
    """Returns a String describing the latitude and longitude of Point p in degrees, 
    minutes, and seconds. (This presumes that the units of the coordinates of p are 
    latitude and longitude.)"""
    pass

def _st_asText(geom):
    """Returns Geometry geom in WKT representation."""
    pass

def _st_geoHash(geom, prec):
    """Returns the Geohash (in base-32 representation) of an interior point of 
    Geometry geom. See Geohash for more information on Geohashes."""
    pass

def _st_antimeridianSafeGeom(geom):
    """If geom spans the antimeridian, attempt to convert the geometry into an 
    equivalent form that is “antimeridian-safe” (i.e. the output geometry is 
    covered by BOX(-180 -90, 180 90)). In certain circumstances, this method 
    may fail, in which case the input geometry will be returned and an error 
    will be logged."""
    pass

def _st_bufferPoint(p, buffer):
    """Returns a Geometry covering all points within a given radius of Point p, 
    where radius is given in meters."""
    pass

def _st_convexHull(geom):
    """Aggregate function. The convex hull of a geometry represents the minimum 
    convex geometry that encloses all geometries geom in the aggregated rows."""
    pass

def _st_idlSafeGeom(geom):
    """Alias of st_antimeridianSafeGeom."""
    pass


st_castToLineString = udf(_st_castToLineString, LineStringUDT())
st_castToPoint = udf(_st_castToPoint, PointUDT())
st_castToPolygon = udf(_st_castToPolygon, PolygonUDT())
st_castToGeometry = udf(_st_castToGeometry, GeometryUDT())
st_byteArray = udf(_st_byteArray, BinaryType())
st_translate = udf(_st_translate, GeometryUDT())
st_asBinary = udf(_st_asBinary, BinaryType())
st_asGeoJSON = udf(_st_asGeoJSON, StringType())
st_asLatLonText = udf(_st_asLatLonText, StringType())
st_asText = udf(_st_asText, StringType())
st_geoHash = udf(_st_geoHash, StringType())
st_antimeridianSafeGeom = udf(_st_antimeridianSafeGeom, GeometryUDT())
st_bufferPoint = udf(_st_bufferPoint, GeometryUDT())
st_convexHull = udf(_st_convexHull, GeometryUDT())
st_idlSafeGeom = udf(_st_idlSafeGeom, GeometryUDT())
