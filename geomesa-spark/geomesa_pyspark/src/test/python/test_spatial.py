"""
Test Spatial Relationships.
"""

import shapely as shp

from geomesa_pyspark.spatial import *
from shapely.geometry import Point, MultiPoint, LineString, MultiLineString, Polygon, MultiPolygon, LinearRing, GeometryCollection
from unittest import TestCase, main


class SpatialRelationshipsTest(TestCase):
    
    def setUp(self):
        
        self.point = Point(7, 13)
        self.polygon = Polygon([[0, 0], [0, 5], [5, 5], [5, 0], [0, 0]])
        self.line = LineString([[0, 0], [0, 5], [5, 5], [5, 0], [0, 0]])
        self.collection = GeometryCollection([self.point, self.line, self.polygon])
        
        self.inside_geom = Point(3, 3)
        self.cross_geom = Polygon([[3, 3], [3, 4], [7, 4], [7, 3], [3, 3]])
        self.outside_geom = LineString([[-1, 0], [-1, 1], [-2, 1], [-2, 0]])
        
        self.outer_shell = [[0, 0], [0, 5], [5, 5], [5, 0], [0, 0]]
        self.first_hole = [[1, 1], [1, 2], [2, 2], [1, 1]]
        self.second_hole = [[3, 3], [3, 4], [4, 4], [3, 3]]
        self.poly_with_holes = Polygon(self.outer_shell, [self.first_hole, self.second_hole])

    def test_st_area(self):
        
        self.assertEqual(_st_area(self.point), 0)
        self.assertEqual(_st_area(self.line), 0)
        self.assertEqual(_st_area(self.polygon), 25)

    def test_st_centroid(self):
        
        self.assertTrue(_st_centroid(self.point).equals(self.point))
        self.assertTrue(_st_centroid(self.polygon).equals(Point(2.5, 2.5)))

    def test_st_closestPoint(self):
        
        polygon = Polygon([[-5, 0], [-5, 1], [-6, 1], [-6, 0], [-5, 0]])
        
        self.assertTrue(_st_closestPoint(polygon, self.polygon).equals(Point(-5, 0)))
        self.assertTrue(_st_closestPoint(self.polygon, polygon).equals(Point(0, 0)))

    def test_st_contains(self):
        
        self.assertTrue(_st_contains(self.polygon, self.inside_geom))
        self.assertFalse(_st_contains(self.polygon, self.outside_geom))

    def test_st_covers(self):
        
        self.assertTrue(_st_covers(self.polygon, self.inside_geom))
        self.assertFalse(_st_covers(self.polygon, self.outside_geom))
        self.assertFalse(_st_covers(self.polygon, self.cross_geom))

    def test_st_crosses(self):
        
        cross_geom = LineString([[-1, -1], [0, 0], [1, 1], [5, 5], [7, 7]])
        
        self.assertTrue(_st_crosses(self.polygon, cross_geom))
        self.assertFalse(_st_crosses(self.polygon, self.outside_geom))
        self.assertFalse(_st_crosses(self.polygon, self.inside_geom))

    def test_st_difference(self):
        
        holes = MultiPolygon([Polygon(self.first_hole), Polygon(self.second_hole)])
        
        self.assertTrue(_st_difference(self.point, self.point).equals(GeometryCollection()))
        self.assertTrue(_st_difference(self.polygon, self.poly_with_holes).equals(holes))

    def test_st_disjoint(self):
        
        self.assertTrue(_st_disjoint(self.polygon, self.outside_geom))
        self.assertFalse(_st_disjoint(self.polygon, self.inside_geom))

    def test_st_distance(self):
        
        first_point = Point(0, 0)
        second_point = Point(5, 5)
        
        self.assertEqual(_st_distance(first_point, second_point), 50 ** (1/2))

    def test_st_distanceSphere(self):
        pass

    def test_st_distanceSpheroid(self):
        pass

    def test_st_equals(self):
        
        self.assertTrue(_st_equals(self.point, self.point))
        self.assertFalse(_st_equals(self.point, self.line))

    def test_st_intersection(self):
        
        self.assertTrue(_st_intersection(self.polygon, self.poly_with_holes).equals(self.poly_with_holes))

    def test_st_intersects(self):
        
        self.assertTrue(_st_intersects(self.polygon, self.cross_geom))
        self.assertFalse(_st_intersects(self.polygon, self.outside_geom))

    def test_st_length(self):
        
        self.assertEqual(_st_length(self.point), 0)
        self.assertEqual(_st_length(self.line), 20)

    def test_st_lengthSphere(self):
        pass

    def test_st_lengthSpheroid(self):
        pass

    def test_st_overlaps(self):
        
        self.assertTrue(_st_overlaps(self.cross_geom, self.polygon))
        self.assertFalse(_st_overlaps(self.inside_geom, self.polygon))
        self.assertFalse(_st_overlaps(self.outside_geom, self.polygon))

    def test_st_relate(self):
        
        self.assertEqual(_st_relate(self.point, self.point), '0FFFFFFF2')
        self.assertEqual(_st_relate(self.point, self.polygon), 'FF0FFF212')
        self.assertEqual(_st_relate(self.polygon, self.polygon), '2FFF1FFF2')

    def test_st_relateBool(self):
        
        point_pattern = '0FFFFFFF2'
        polygon_pattern = '2FFF1FFF2'
        
        self.assertTrue(_st_relateBool(self.point, self.point, point_pattern))
        self.assertTrue(_st_relateBool(self.polygon, self.polygon, polygon_pattern))
        self.assertFalse(_st_relateBool(self.point, self.point, polygon_pattern))
        self.assertFalse(_st_relateBool(self.polygon, self.polygon, point_pattern))

    def test_st_touches(self):
        
        touch_point = Point(5, 5)
        touch_poly = Polygon([[5, 5], [5, 6], [6, 6], [5, 5]])
        
        self.assertTrue(_st_touches(self.polygon, touch_point))
        self.assertTrue(_st_touches(self.polygon, touch_poly))
        self.assertFalse(_st_touches(self.polygon, self.outside_geom))
        self.assertFalse(_st_touches(self.polygon, self.inside_geom))
        self.assertFalse(_st_touches(self.polygon, self.cross_geom))

    def test_st_transform(self):
        
        point_epsg4326 = Point(10, 10)
        point_epsg3857 = Point(1113194.907932736, 1118889.97485796)
        
        self.assertEqual(round(_st_transform(point_epsg4326, 'epsg:4326', 'epsg:3857').x, 2), round(point_epsg3857.x, 2))
        self.assertEqual(round(_st_transform(point_epsg3857, 'epsg:3857', 'epsg:4326').x, 2), round(point_epsg4326.x, 2))

    def test_st_within(self):
        
        self.assertTrue(_st_within(self.inside_geom, self.polygon))
        self.assertFalse(_st_within(self.outside_geom, self.polygon))
     
        
main(argv=[''], verbosity=2, exit=False)