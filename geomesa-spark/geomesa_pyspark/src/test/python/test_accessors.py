"""
Test Geometry Accessors.
"""
import shapely as shp

from geomesa_pyspark.accessors import *
from shapely.geometry import Point, MultiPoint, LineString, MultiLineString, Polygon, MultiPolygon, LinearRing, GeometryCollection
from unittest import TestCase, main


class GeometryAccessorTest(TestCase):
    
    def setUp(self):
        
        self.point = Point(7, 13)
        self.polygon = Polygon([[0, 0], [0, 5], [5, 5], [5, 0], [0, 0]])
        self.line = LineString([[0, 0], [0, 5], [5, 5], [5, 0], [0, 0]])
        self.collection = GeometryCollection([self.point, self.line, self.polygon])
        
        self.outer_shell = [[0, 0], [0, 5], [5, 5], [5, 0], [0, 0]]
        self.first_hole = [[1, 1], [1, 2], [2, 2], [1, 1]]
        self.second_hole = [[3, 3], [3, 4], [4, 4], [3, 3]]
        self.poly_with_holes = Polygon(self.outer_shell, [self.first_hole, self.second_hole])

    def test_st_boundary(self):
        
        point_boundary = _st_boundary(self.point)
        polygon_boundary = _st_boundary(self.polygon)
        
        self.assertTrue(point_boundary.equals(GeometryCollection()))
        self.assertTrue(isinstance(point_boundary, GeometryCollection))
        
        self.assertTrue(polygon_boundary.equals(self.line))
        self.assertTrue(isinstance(polygon_boundary, LineString))

    def test_st_coordDim(self):
        
        poly_2D = Polygon([[0, 0], [0, 5], [5, 5], [0, 0]])
        poly_3D = Polygon([[0, 0, 1], [0, 5, 1], [5, 5, 1], [0, 0, 1]])
        
        self.assertTrue(_st_coordDim(poly_2D) == 2)
        self.assertTrue(_st_coordDim(poly_3D) == 3)

    def test_st_dimension(self):
        
        open_line = LineString([[0, 0], [0, 5], [5, 5], [5, 0]])
        closed_line = LineString([[0, 0], [0, 5], [5, 5], [5, 0], [0, 0]])
        collection = GeometryCollection([self.point, self.line, self.polygon])
        
        self.assertTrue(_st_dimension(self.point) == 0)
        self.assertTrue(_st_dimension(open_line) == 1)
        self.assertTrue(_st_dimension(closed_line) == 2)
        self.assertTrue(_st_dimension(self.polygon) == 2)
        self.assertTrue(_st_dimension(collection) == 2)

    def test_st_envelope(self):
        
        self.assertTrue(isinstance(_st_envelope(self.polygon), Polygon))
        self.assertTrue(_st_envelope(self.polygon).equals(shp.geometry.box(0, 0, 5, 5)))

    def test_st_exteriorRing(self):
        
        self.assertTrue(isinstance(_st_exteriorRing(self.polygon), LineString))
        self.assertTrue(_st_exteriorRing(self.polygon).equals(self.line))

    def test_st_geometryN(self):
        
        self.assertTrue(_st_geometryN(self.collection, 1).equals(self.point))
        self.assertTrue(_st_geometryN(self.collection, 2).equals(self.line))
        self.assertTrue(_st_geometryN(self.collection, 3).equals(self.polygon))
        self.assertTrue(_st_geometryN(self.collection, 4).equals(GeometryCollection()))
        self.assertTrue(_st_geometryN(self.point, 1).equals(self.point))
        self.assertTrue(_st_geometryN(self.line, 3).equals(self.line))

    def test_st_interiorRingN(self):
        
        self.assertTrue(_st_interiorRingN(self.poly_with_holes, 1).equals(LineString(self.first_hole)))
        self.assertTrue(_st_interiorRingN(self.poly_with_holes, 2).equals(LineString(self.second_hole)))
        self.assertTrue(_st_interiorRingN(self.poly_with_holes, 3) is None)
        self.assertTrue(_st_interiorRingN(self.polygon, 3) is None)
        self.assertTrue(_st_interiorRingN(self.point, 3) is None)

    def test_st_isClosed(self):
        
        open_line = LineString([[0, 0], [0, 1], [1, 1]])
        closed_line = LineString([[0, 0], [0, 1], [1, 1], [0, 0]])
        open_multiline = MultiLineString([[[0, 0], [0, 1], [1, 1]]])
        closed_multiline = MultiLineString([[[0, 0], [0, 1], [1, 1], [0, 0]]])
        
        self.assertFalse(_st_isClosed(open_line))
        self.assertTrue(_st_isClosed(closed_line))
        self.assertFalse(_st_isClosed(open_multiline))
        self.assertTrue(_st_isClosed(closed_multiline))

    def test_st_isCollection(self):
        
        self.assertTrue(_st_isCollection(self.collection))
        self.assertFalse(_st_isCollection(self.point))

    def test_st_isEmpty(self):
        
        self.assertTrue(_st_isEmpty(GeometryCollection()))
        self.assertFalse(_st_isEmpty(self.collection))

    def test_st_isRing(self):
        
        self.assertTrue(_st_isRing(self.line))
        self.assertFalse(_st_isRing(self.point))

    def test_st_isSimple(self):
        
        simple_line = LineString([[0, 0], [0, 1], [1, 1]])
        complex_line = LineString([[0, 0], [0, 1], [1, 1], [0, 0.5], [-0.5, 0.5]])
        
        self.assertTrue(_st_isSimple(simple_line))
        self.assertFalse(_st_isSimple(complex_line))

    def test_st_isValid(self):
        
        valid_poly = Polygon([[0, 0], [0, 1], [1, 1], [0, 0]])
        invalid_poly = Polygon([[0, 0], [0, 1], [1, 1], [0, 0.5], [-0.5, 0.5], [0, 0]])
        
        self.assertTrue(_st_isValid(valid_poly))
        self.assertFalse(_st_isValid(invalid_poly))

    def test_st_numGeometries(self):
        
        collection_1 = GeometryCollection([self.point])
        collection_2 = GeometryCollection([self.point, self.line])
        collection_3 = GeometryCollection([self.point, self.line, self.polygon])
        
        self.assertEqual(_st_numGeometries(collection_1), 1)
        self.assertEqual(_st_numGeometries(collection_2), 2)
        self.assertEqual(_st_numGeometries(collection_3), 3)
        self.assertEqual(_st_numGeometries(self.point), 1)
        self.assertEqual(_st_numGeometries(self.polygon), 1)

    def test_st_numPoints(self):
        
        multipoint = MultiPoint([[0, 0], [0, 1]])
        multiline = MultiLineString([[[0, 0], [0, 1], [1, 1]]])
        multipoly = MultiPolygon([self.polygon, self.poly_with_holes])
        
        self.assertEqual(_st_numPoints(self.point), 1)
        self.assertEqual(_st_numPoints(multipoint), 2)
        self.assertEqual(_st_numPoints(self.line), 5)
        self.assertEqual(_st_numPoints(multiline), 3)
        self.assertEqual(_st_numPoints(self.polygon), 5)
        self.assertEqual(_st_numPoints(self.poly_with_holes), 13)
        self.assertEqual(_st_numPoints(multipoly), 18)
        self.assertEqual(_st_numPoints(self.collection), 11)

    def test_st_pointN(self):
        
        self.assertTrue(_st_pointN(self.line, 1).equals(Point(0, 0)))
        self.assertTrue(_st_pointN(self.line, -2).equals(Point(5, 0)))
        self.assertTrue(_st_pointN(self.point, 3) is None)
        self.assertTrue(_st_pointN(self.line, 6) is None)

    def test_st_x(self):
        
        self.assertTrue(_st_x(self.point) == self.point.x)
        self.assertTrue(_st_x(self.polygon) == None)

    def test_st_y(self):
        
        self.assertTrue(_st_y(self.point) == self.point.y)
        self.assertTrue(_st_y(self.polygon) == None)


if __name__ == '__main__':
    main()
