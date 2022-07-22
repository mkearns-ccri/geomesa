"""
SparkSQL Functions: Geometry Accessors.
"""
import shapely as shp

from shapely.geometry import Point, MultiPoint, LineString, MultiLineString, Polygon, MultiPolygon, LinearRing, GeometryCollection

from pyspark.sql.types import BooleanType, IntegerType, FloatType
from pyspark.sql.functions import udf
from geomesa_pyspark.types import *


dimension_map = {
    Point: 0,
    MultiPoint: 0,
    LineString: 1,
    MultiLineString: 1,
    Polygon: 2,
    MultiPolygon: 2,
    LinearRing: 2
}


def _st_boundary(geom):
    """The boundary of the geometry. Returns an empty GeometryCollection if dim = 0."""
    return geom.boundary

def _st_coordDim(geom):
    """The dimension of the coordinates for this geometry as an integer."""
    vertex = geom.boundary.coords[0]
    return 2 if len(vertex) < 3 else 3
    
def _st_dimension(geom):
    """The dimension of the geometry as an integer."""
    def _get_shape_dimension(geom):
        dim = dimension_map[type(geom)]
        return dim + 1 if geom.is_closed else dim
    if isinstance(geom, GeometryCollection):
        dims = [_get_shape_dimension(g) for g in geom]
        return max(dims)
    else:
        return _get_shape_dimension(geom)

def _st_envelope(geom):
    """The smallest bounding box containing this geometry as a Shapely Polygon."""
    return geom.envelope

def _st_exteriorRing(geom):
    """Returns a LineString representing the exterior ring of the geometry."""
    return LineString(geom.exterior)

def _st_geometryN(geom, n):
    """Returns the n-th Geometry (1-based index) of geom if the Geometry is a GeometryCollection, or geom if it is not."""
    if isinstance(geom, GeometryCollection):
        return geom[n-1] if 0 < n <= len(geom) else GeometryCollection()
    else:
        return geom

def _st_interiorRingN(geom, n):
    """Returns the n-th interior LineString (1-based index) ring of the Polygon geom. 
    Returns null if the geometry is not a Polygon or the given n is out of range."""
    if isinstance(geom, Polygon):
        interiors = [ring for ring in geom.interiors]
        return LineString(interiors[n-1].coords) if 0 < n <= len(interiors) else None

def _st_isClosed(geom):
    """Returns true if geom is a LineString or MultiLineString and its start and end points are coincident. 
    Returns true for all other Geometry types."""
    if isinstance(geom, LineString):
        return geom.is_closed
    elif isinstance(geom, MultiLineString):
        return all([line.is_closed for line in geom])
    else:
        return True

def _st_isCollection(geom):
    """Returns true if geom is a GeometryCollection."""
    return isinstance(geom, GeometryCollection)

def _st_isEmpty(geom):
    """Returns true if geom is empty."""
    return geom.is_empty

def _st_isRing(geom):
    """Returns true if geom is a LineString or a MultiLineString and is both closed and simple."""
    return geom.is_ring

def _st_isSimple(geom):
    """Returns true if geom has no anomalous geometric points, such as self intersection or self tangency."""
    return geom.is_simple

def _st_isValid(geom):
    """Returns true if the Geometry is topologically valid according to the OGC SFS specification."""
    return geom.is_valid

def _st_numGeometries(geom):
    """If geom is a GeometryCollection, returns the number of geometries. 
    For single geometries, returns 1."""
    return len(geom) if isinstance(geom, GeometryCollection) else 1

def _st_numPoints(geom):
    """Returns the number of vertices in Geometry geom."""
    if isinstance(geom, Point):             return 1
    elif isinstance(geom, MultiPoint):      return len(geom)
    elif isinstance(geom, LineString):      return len(geom.coords)
    elif isinstance(geom, MultiLineString): return sum([len(line.coords) for line in geom])
    elif isinstance(geom, Polygon):         return len(geom.exterior.coords) + sum([len(hole.coords) for hole in geom.interiors])
    else:                                   return sum([_st_numPoints(shape) for shape in geom])

def _st_pointN(geom, n):
    """If geom is a LineString, returns the n-th vertex (1-based index) of geom as a Point. 
    Negative values are counted backwards from the end of the LineString. 
    Returns null if geom is not a LineString."""
    if isinstance(geom, LineString):
        vertices = MultiPoint(geom.coords)
        n = n - 1 if n > 0 else n
        count = len(vertices)
        return vertices[n] if -count <= n < count else None

def _st_x(geom):
    """If geom is a Point, return the X coordinate of that point."""
    if isinstance(geom, Point):
        return geom.x

def _st_y(geom):
    """If geom is a Point, return the Y coordinate of that point."""
    if isinstance(geom, Point):
        return geom.y


st_boundary = udf(_st_boundary, GeometryUDT())
st_coordDim = udf(_st_coordDim, IntegerType())
st_dimension = udf(_st_dimension, IntegerType())
st_envelope = udf(_st_envelope, GeometryUDT())
st_exteriorRing = udf(_st_exteriorRing, LineStringUDT())
st_geometryN = udf(_st_geometryN, IntegerType())
st_interiorRingN = udf(_st_interiorRingN, IntegerType())
st_isClosed = udf(_st_isClosed, BooleanType())
st_isCollection = udf(_st_isCollection, BooleanType())
st_isEmpty = udf(_st_isEmpty, BooleanType())
st_isRing = udf(_st_isRing, BooleanType())
st_isSimple = udf(_st_isSimple, BooleanType())
st_isValid = udf(_st_isValid, BooleanType())
st_numGeometries = udf(_st_numGeometries, IntegerType())
st_numPoints = udf(_st_numPoints, IntegerType())
st_pointN = udf(_st_pointN, PointUDT())
st_x = udf(_st_x, FloatType())
st_y = udf(_st_y, FloatType())
