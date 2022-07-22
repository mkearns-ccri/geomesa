"""
SparkSQL Functions: Spatial Relationships.
"""
import shapely as shp
import pyproj

from shapely.ops import nearest_points, transform
from shapely.geometry import Point, MultiPoint, LineString, MultiLineString, Polygon, MultiPolygon, LinearRing, GeometryCollection
from pyspark.sql.types import DoubleType, BooleanType, StringType
from pyspark.sql.functions import udf
from geomesa_pyspark.types import *


def _st_area(g):
    """If Geometry g is areal, returns the area of its surface 
    in square units of the coordinate reference system (for example, 
    degrees^2 for EPSG:4326). Returns 0.0 for non-areal geometries 
    (e.g. Points, non-closed LineStrings, etc.)."""
    return g.area

def _st_centroid(g):
    """Returns the geometric center of a geometry."""
    return g.centroid

def _st_closestPoint(a, b):
    """Returns the Point on a that is closest to b. 
    This is the first point of the shortest line."""
    return nearest_points(a, b)[0]

def _st_contains(a, b):
    """Returns true if and only if no points of b lie in the exterior of a, 
    and at least one point of the interior of b lies in the interior of a."""
    return a.contains(b)

def _st_covers(a, b):
    """Returns true if no point in Geometry b is outside Geometry a."""
    return a.covers(b)

def _st_crosses(a, b):
    """Returns true if the supplied geometries have some, 
    but not all, interior points in common."""
    return a.crosses(b)

def _st_difference(a, b):
    """Returns the difference of the input geometries."""
    return a.difference(b)

def _st_disjoint(a, b):
    """Returns true if the geometries do not “spatially intersect”; i.e., 
    they do not share any space together. Equivalent to NOT st_intersects(a, b)."""
    return a.disjoint(b)

def _st_distance(a, b):
    """Returns the 2D Cartesian distance between the two geometries in units 
    of the coordinate reference system (e.g. degrees for EPSG:4236)."""
    return a.distance(b)

def _st_distanceSphere(a, b):
    """Approximates the minimum distance between two longitude/latitude geometries 
    assuming a spherical earth."""
    pass

def _st_distanceSpheroid(a, b):
    """Returns the minimum distance between two longitude/latitude geometries 
    assuming the WGS84 spheroid."""
    pass

def _st_equals(a, b):
    """Returns true if the given Geometries represent the same logical Geometry. 
    Directionality is ignored."""
    return a.equals(b)

def _st_intersection(a, b):
    """Returns the intersection of the input geometries."""
    return a.intersection(b)

def _st_intersects(a, b):
    """Returns true if the geometries spatially intersect in 2D (i.e. share 
    any portion of space). Equivalent to NOT st_disjoint(a, b)."""
    return a.intersects(b)

def _st_length(geom):
    """Returns the 2D path length of linear geometries, or perimeter of areal 
    geometries, in units of the the coordinate reference system (e.g. degrees 
    for EPSG:4236). Returns 0.0 for other geometry types (e.g. Point)."""
    return geom.length

def _st_lengthSphere(line):
    """Approximates the 2D path length of a LineString geometry using a spherical 
    earth model. The returned length is in units of meters. The approximation is 
    within 0.3% of st_lengthSpheroid and is computationally more efficient."""
    pass

def _st_lengthSpheroid(line):
    """Calculates the 2D path length of a LineString geometry defined with longitude/latitude 
    coordinates on the WGS84 spheroid. The returned length is in units of meters."""
    pass

def _st_overlaps(a, b):
    """Returns true if the geometries have some but not all points in common, are of 
    the same dimension, and the intersection of the interiors of the two geometries 
    has the same dimension as the geometries themselves."""
    return a.overlaps(b)

def _st_relate(a, b):
    """Returns the DE-9IM 3x3 interaction matrix pattern describing the dimensionality of the 
    intersections between the interior, boundary and exterior of the two geometries."""
    return a.relate(b)

def _st_relateBool(a, b, mask):
    """Returns true if the DE-9IM interaction matrix mask mask matches the interaction matrix 
    pattern obtained from st_relate(a, b)."""
    return a.relate_pattern(b, mask)

def _st_touches(a, b):
    """Returns true if the geometries have at least one point in common, but their interiors 
    do not intersect."""
    return a.touches(b)

def _st_transform(a, fromCRS, toCRS):
    """Returns a new geometry with its coordinates transformed to a different coordinate 
    reference system (for example from EPSG:4326 to EPSG:27700)."""
    transformer = pyproj.Transformer.from_proj(
        pyproj.Proj(fromCRS), 
        pyproj.Proj(toCRS), 
        always_xy=True)
    return transform(transformer.transform, a)

def _st_within(a, b):
    """Returns true if geometry a is completely inside geometry b."""
    return a.within(b)


st_area = udf(_st_area, DoubleType())
st_centroid = udf(_st_centroid, PointUDT())
st_closestPoint = udf(_st_closestPoint, PointUDT())
st_contains = udf(_st_contains, BooleanType())
st_covers = udf(_st_covers, BooleanType())
st_crosses = udf(_st_crosses, BooleanType())
st_difference = udf(_st_difference, GeometryUDT())
st_disjoint = udf(_st_disjoint, BooleanType())
st_distance = udf(_st_distance, DoubleType())
st_distanceSphere = udf(_st_distanceSphere, DoubleType())
st_distanceSpheroid = udf(_st_distanceSpheroid, DoubleType())
st_equals = udf(_st_equals, BooleanType())
st_intersection = udf(_st_intersection, GeometryUDT())
st_intersects = udf(_st_intersects, BooleanType())
st_length = udf(_st_length, DoubleType())
st_lengthSphere = udf(_st_lengthSphere, DoubleType())
st_lengthSpheroid = udf(_st_lengthSpheroid, DoubleType())
st_overlaps = udf(_st_overlaps, BooleanType())
st_relate = udf(_st_relate, StringType())
st_relateBool = udf(_st_relateBool, BooleanType())
st_touches = udf(_st_touches, BooleanType())
st_transform = udf(_st_transform, GeometryUDT())
st_within = udf(_st_within, BooleanType())
