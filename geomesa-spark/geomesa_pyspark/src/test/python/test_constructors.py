"""
Test Geometry Constructors.
"""
import shapely as shp

from shapely import wkt
from geomesa_pyspark.constructors import *
from unittest import TestCase, main


class GeometryConstructorTest(TestCase):
    
    def setUp(self):
        
        self.point_geojson = {'type': 'Point', 'coordinates': [7, 13]}
        self.polygon_geojson = {'type': 'Polygon', 'coordinates': [[[0, 0], [0, 1], [1, 1], [0, 0]]]}
        self.line_geojson = {'type': 'LineString', 'coordinates': [[0, 0], [0, 1], [1, 1], [0, 0]]}
        self.mline_geojson = {'type': 'MultiLineString', 'coordinates': [[[0, 0], [0, 1], [1, 1], [0, 0]]]}
        self.mpoint_geojson = {'type': 'MultiPoint', 'coordinates': [[0, 0], [0, 1], [1, 1]]}
        self.mpolygon_geojson = {'type': 'MultiPolygon', 'coordinates': [[[[0, 0], [0, 1], [1, 1], [0, 0]], [[2, 2], [2, 3], [3, 2], [2, 2]]]]}
        self.bbox_geojson = {'type': 'Polygon', 'coordinates': [[[0, 0], [0, 5], [5, 5], [5, 0], [0, 0]]]}
        
        self.point_wkb = b'\x01\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\x1c@\x00\x00\x00\x00\x00\x00*@'
        self.polygon_wkb = b'\x01\x03\x00\x00\x00\x01\x00\x00\x00\x04\x00'  \
                           b'\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00'  \
                           b'\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00'  \
                           b'\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\xf0?' \
                           b'\x00\x00\x00\x00\x00\x00\xf0?\x00\x00\x00\x00' \
                           b'\x00\x00\xf0?\x00\x00\x00\x00\x00\x00\x00\x00' \
                           b'\x00\x00\x00\x00\x00\x00\x00\x00'

        self.point_wkt = 'POINT (7 13)'
        self.polygon_wkt = 'POLYGON ((0 0, 0 1, 1 1, 0 0))'
        self.line_wkt = 'LINESTRING (0 0, 0 1, 1 1, 0 0)'
        self.mline_wkt = 'MULTILINESTRING ((0 0, 0 1, 1 1, 0 0))'
        self.mpoint_wkt = 'MULTIPOINT (0 0, 0 1, 1 1)'
        self.mpolygon_wkt = 'MULTIPOLYGON(((0 0, 0 1, 1 1, 0 0), (2 2, 2 3, 3 2, 2 2)))'
    
    def test_st_box2DFromGeoHash(self):
        pass

    def test_st_geomFromGeoHash(self):
        pass

    def test_st_geomFromGeoJSON(self):

        point_from_geojson = _st_geomFromGeoJSON(self.point_geojson)
        polygon_from_geojson = _st_geomFromGeoJSON(self.polygon_geojson)

        point_from_wkt = shp.wkt.loads(self.point_wkt)
        polygon_from_wkt = shp.wkt.loads(self.polygon_wkt)

        self.assertTrue(point_from_wkt.equals(point_from_geojson))
        self.assertTrue(polygon_from_wkt.equals(polygon_from_geojson))
        
        self.assertTrue(isinstance(point_from_geojson, shp.geometry.point.Point))
        self.assertTrue(isinstance(polygon_from_geojson, shp.geometry.polygon.Polygon))

    def test_st_geomFromText(self):
        
        point_from_text = _st_geomFromText(self.point_wkt)
        polygon_from_text = _st_geomFromText(self.polygon_wkt)
        
        point_from_geojson = shp.geometry.shape(self.point_geojson)
        polygon_from_geojson = shp.geometry.shape(self.polygon_geojson)
        
        self.assertTrue(point_from_geojson.equals(point_from_text))
        self.assertTrue(polygon_from_geojson.equals(polygon_from_text))
        
        self.assertTrue(isinstance(point_from_text, shp.geometry.point.Point))
        self.assertTrue(isinstance(polygon_from_text, shp.geometry.polygon.Polygon))

    def test_st_geomFromWKB(self):
        
        point_from_wkb = _st_geomFromWKB(self.point_wkb)
        polygon_from_wkb = _st_geomFromWKB(self.polygon_wkb)
        
        point_from_wkt = shp.wkt.loads(self.point_wkt)
        polygon_from_wkt = shp.wkt.loads(self.polygon_wkt)
        
        self.assertTrue(point_from_wkt.equals(point_from_wkb))
        self.assertTrue(polygon_from_wkt.equals(polygon_from_wkb))
        
        self.assertTrue(isinstance(point_from_wkb, shp.geometry.point.Point))
        self.assertTrue(isinstance(polygon_from_wkb, shp.geometry.polygon.Polygon))
        
    def test_st_geomFromWKT(self):
        
        point_from_wkt = _st_geomFromWKT(self.point_wkt)
        polygon_from_wkt = _st_geomFromWKT(self.polygon_wkt)
        
        point_from_geojson = shp.geometry.shape(self.point_geojson)
        polygon_from_geojson = shp.geometry.shape(self.polygon_geojson)
        
        self.assertTrue(point_from_geojson.equals(point_from_wkt))
        self.assertTrue(polygon_from_geojson.equals(polygon_from_wkt))
        
        self.assertTrue(isinstance(point_from_wkt, shp.geometry.point.Point))
        self.assertTrue(isinstance(polygon_from_wkt, shp.geometry.polygon.Polygon))

    def test_st_geometryFromText(self):
        
        point_from_text = _st_geometryFromText(self.point_wkt)
        polygon_from_text = _st_geometryFromText(self.polygon_wkt)
        
        point_from_geojson = shp.geometry.shape(self.point_geojson)
        polygon_from_geojson = shp.geometry.shape(self.polygon_geojson)
        
        self.assertTrue(point_from_geojson.equals(point_from_text))
        self.assertTrue(polygon_from_geojson.equals(polygon_from_text))
        
        self.assertTrue(isinstance(point_from_text, shp.geometry.point.Point))
        self.assertTrue(isinstance(polygon_from_text, shp.geometry.polygon.Polygon))

    def test_st_lineFromText(self):
        
        line_from_text = _st_lineFromText(self.line_wkt)
        line_from_geojson = shp.geometry.shape(self.line_geojson)
        
        self.assertTrue(line_from_geojson.equals(line_from_text))
        self.assertTrue(isinstance(line_from_text, shp.geometry.linestring.LineString))

    def test_st_mLineFromText(self):
        
        mline_from_text = _st_mLineFromText(self.mline_wkt)
        mline_from_geojson = shp.geometry.shape(self.mline_geojson)
        
        self.assertTrue(mline_from_geojson.equals(mline_from_text))
        self.assertTrue(isinstance(mline_from_text, shp.geometry.multilinestring.MultiLineString))

    def test_st_mPointFromText(self):
        
        mpoint_from_text = _st_mPointFromText(self.mpoint_wkt)
        mpoint_from_geojson = shp.geometry.shape(self.mpoint_geojson)
        
        self.assertTrue(mpoint_from_geojson.equals(mpoint_from_text))
        self.assertTrue(isinstance(mpoint_from_text, shp.geometry.multipoint.MultiPoint))

    def test_st_mPolyFromText(self):
        
        mpoly_from_text = _st_mPolyFromText(self.mpolygon_wkt)
        mpoly_from_geojson = shp.geometry.shape(self.mpolygon_geojson)
        
        self.assertTrue(mpoly_from_geojson.equals(mpoly_from_text))
        self.assertTrue(isinstance(mpoly_from_text, shp.geometry.multipolygon.MultiPolygon))

    def test_st_makeBBOX(self):
        
        lower_x, lower_y, upper_x, upper_y = 0, 0, 5, 5
        
        bbox_from_coordinates = _st_makeBBOX(lower_x, lower_y, upper_x, upper_y)
        bbox_from_geojson = shp.geometry.shape(self.bbox_geojson)
        
        self.assertTrue(bbox_from_coordinates.equals(bbox_from_geojson))
        self.assertTrue(isinstance(bbox_from_coordinates, shp.geometry.polygon.Polygon))

    def test_st_makeBox2D(self):

        lower_point = shp.geometry.Point(0, 0)
        upper_point = shp.geometry.Point(5, 5)
        
        bbox_from_points = _st_makeBox2D(lower_point, upper_point)
        bbox_from_geojson = shp.geometry.shape(self.bbox_geojson)
        
        self.assertTrue(bbox_from_points.equals(bbox_from_geojson))
        self.assertTrue(isinstance(bbox_from_points, shp.geometry.polygon.Polygon))

    def test_st_makeLine(self):
                        
        point_wkt = ['Point(0 0)', 'Point(0 1)', 'Point(1 1)', 'POINT(0 0)']
        point_seq = [shp.wkt.loads(wkt) for wkt in point_wkt]
        
        line_from_point_seq = _st_makeLine(point_seq)
        line_from_geojson = shp.geometry.shape(self.line_geojson)
                        
        self.assertTrue(line_from_point_seq.equals(line_from_geojson))
        self.assertTrue(isinstance(line_from_point_seq, shp.geometry.linestring.LineString))

    def test_st_makePoint(self):
        
        x, y = 7, 13
        
        point_from_xy = _st_makePoint(x, y)
        point_from_geojson = shp.geometry.shape(self.point_geojson)
        
        self.assertTrue(point_from_xy.equals(point_from_geojson))
        self.assertTrue(isinstance(point_from_xy, shp.geometry.point.Point))

    def test_st_makePointM(self):
        
        x, y, m = 7, 13, 9
        
        point_with_m_coordinate = _st_makePointM(x, y, m)
        point_from_geojson = shp.geometry.shape(self.point_geojson)
        
        self.assertTrue(point_with_m_coordinate.equals(point_from_geojson))
        self.assertTrue(point_with_m_coordinate.m == m)
        self.assertTrue(isinstance(point_with_m_coordinate, shp.geometry.point.Point))

    def test_st_makePolygon(self):
        
        shell = shp.geometry.shape(self.line_geojson)
        polygon_from_shell = _st_makePolygon(shell)
        polygon_from_geojson = shp.geometry.shape(self.polygon_geojson)
        
        self.assertTrue(polygon_from_shell.equals(polygon_from_geojson))
        self.assertTrue(isinstance(polygon_from_shell, shp.geometry.polygon.Polygon))

    def test_st_point(self):
        
        x, y = 7, 13
        
        point_from_xy = _st_point(x, y)
        point_from_geojson = shp.geometry.shape(self.point_geojson)
        
        self.assertTrue(point_from_xy.equals(point_from_geojson))
        self.assertTrue(isinstance(point_from_xy, shp.geometry.point.Point))

    def test_st_pointFromGeoHash(self):
        pass

    def test_st_pointFromText(self):
        
        point_from_text = _st_pointFromText(self.point_wkt)
        point_from_geojson = shp.geometry.shape(self.point_geojson)
        
        self.assertTrue(point_from_text.equals(point_from_geojson))
        self.assertTrue(isinstance(point_from_text, shp.geometry.point.Point))

    def test_st_pointFromWKB(self):
        
        point_from_wkb = _st_pointFromWKB(self.point_wkb)
        point_from_wkt = shp.wkt.loads(self.point_wkt)
        
        self.assertTrue(point_from_wkt.equals(point_from_wkb))
        self.assertTrue(isinstance(point_from_wkb, shp.geometry.point.Point))

    def test_st_polygon(self):
        
        shell = shp.geometry.shape(self.line_geojson)
        polygon_from_shell = _st_polygon(shell)
        polygon_from_geojson = shp.geometry.shape(self.polygon_geojson)
        
        self.assertTrue(polygon_from_shell.equals(polygon_from_geojson))
        self.assertTrue(isinstance(polygon_from_shell, shp.geometry.polygon.Polygon))

    def test_st_polygonFromText(self):
        
        polygon_from_text = _st_polygonFromText(self.polygon_wkt)
        polygon_from_geojson = shp.geometry.shape(self.polygon_geojson)
        
        self.assertTrue(polygon_from_text.equals(polygon_from_geojson))
        self.assertTrue(isinstance(polygon_from_text, shp.geometry.polygon.Polygon))


if __name__ == '__main__':
    main()