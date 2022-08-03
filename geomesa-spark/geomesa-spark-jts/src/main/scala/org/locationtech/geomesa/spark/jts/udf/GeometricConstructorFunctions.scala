/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.spark.jts.udf

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf
import org.locationtech.geomesa.spark.jts.util.GeoHashUtils._
import org.locationtech.geomesa.spark.jts.util.SQLFunctionHelper._
import org.locationtech.geomesa.spark.jts.util.{GeometryUtils, WKBUtils, WKTUtils}
import org.locationtech.jts.geom._

object GeometricConstructorFunctions {

  @transient
  private val geomFactory: GeometryFactory = new GeometryFactory()

  def geomFromGeoHash(hash: String, prec: Int): Geometry = decode(hash, prec)
  def geomFromWKT(text: String): Geometry = WKTUtils.read(text)
  def geomFromWKB(array: Array[Byte]): Geometry = WKBUtils.read(array)
  def lineFromText(text: String): LineString = WKTUtils.read(text).asInstanceOf[LineString]
  def makeBox2D(lowerLeft: Point, upperRight: Point): Geometry = {
    val envelope = new Envelope(lowerLeft.getX, upperRight.getX, lowerLeft.getY, upperRight.getY)
    geomFactory.toGeometry(envelope)
  }
  def makeBBOX(lowerX: Double, upperX: Double, lowerY: Double, upperY: Double): Geometry = {
    val envelope = new Envelope(lowerX, upperX, lowerY, upperY)
    GeometryUtils.addWayPointsToBBOX(geomFactory.toGeometry(envelope))
  }
  def makePolygon(shell: LineString): Polygon = {
    val ring = geomFactory.createLinearRing(shell.getCoordinateSequence)
    geomFactory.createPolygon(ring)
  }
  def makePoint(x: Double, y: Double): Point = geomFactory.createPoint(new Coordinate(x, y))
  def makeLine(s: Seq[Point]): LineString = geomFactory.createLineString(s.map(_.getCoordinate).toArray)
  def makePointM(x: Double, y: Double, m: Double): Point = WKTUtils.read(s"POINT($x $y $m)").asInstanceOf[Point]
  def mLineFromText(text: String): MultiLineString = WKTUtils.read(text).asInstanceOf[MultiLineString]
  def mPointFromText(text: String): MultiPoint = WKTUtils.read(text).asInstanceOf[MultiPoint]
  def mPolyFromText(text: String): MultiPolygon = WKTUtils.read(text).asInstanceOf[MultiPolygon]
  def point(x: Double, y: Double): Point = makePoint(x, y)
  def pointFromGeoHash(hash: String, prec: Int): Point = decode(hash, prec).getInteriorPoint
  def pointFromText(text: String): Point = WKTUtils.read(text).asInstanceOf[Point]
  def pointFromWKB(array: Array[Byte]): Point = geomFromWKB(array).asInstanceOf[Point]
  def polygon(shell: LineString): Polygon = makePolygon(shell)
  def polygonFromText(text: String): Polygon = WKTUtils.read(text).asInstanceOf[Polygon]

  val ST_GeomFromGeoHash: (String, Int) => Geometry = nullableUDF(geomFromGeoHash)
  val ST_GeomFromWKT: String => Geometry = nullableUDF(geomFromWKT)
  val ST_GeomFromWKB: Array[Byte] => Geometry = nullableUDF(geomFromWKB)
  val ST_LineFromText: String => LineString = nullableUDF(lineFromText)
  val ST_MakeBox2D: (Point, Point) => Geometry = nullableUDF(makeBox2D)
  val ST_MakeBBOX: (Double, Double, Double, Double) => Geometry = nullableUDF(makeBBOX)
  val ST_MakePolygon: LineString => Polygon = nullableUDF(makePolygon)
  val ST_MakePoint: (Double, Double) => Point = nullableUDF(makePoint)
  val ST_MakeLine: Seq[Point] => LineString = nullableUDF(makeLine)
  val ST_MakePointM: (Double, Double, Double) => Point = nullableUDF(makePointM)
  val ST_MLineFromText: String => MultiLineString = nullableUDF(mLineFromText)
  val ST_MPointFromText: String => MultiPoint = nullableUDF(mPointFromText)
  val ST_MPolyFromText: String => MultiPolygon = nullableUDF(mPolyFromText)
  val ST_Point: (Double, Double) => Point = nullableUDF(makePoint)
  val ST_PointFromGeoHash: (String, Int) => Point = nullableUDF(pointFromGeoHash)
  val ST_PointFromText: String => Point = nullableUDF(pointFromText)
  val ST_PointFromWKB: Array[Byte] => Point = nullableUDF(pointFromWKB)
  val ST_Polygon: LineString => Polygon = nullableUDF(polygon)
  val ST_PolygonFromText: String => Polygon = nullableUDF(polygonFromText)

  def ST_GeomFromGeoHash_UDF: UserDefinedFunction = udf(geomFromGeoHash _)
  def ST_GeomFromWKT_UDF: UserDefinedFunction = udf(geomFromWKT _)
  def ST_GeomFromWKB_UDF: UserDefinedFunction = udf(geomFromWKB _)
  def ST_LineFromText_UDF: UserDefinedFunction = udf(lineFromText _)
  def ST_MakeBox2D_UDF: UserDefinedFunction = udf(makeBox2D _)
  def ST_MakeBBOX_UDF: UserDefinedFunction = udf(makeBBOX _)
  def ST_MakePolygon_UDF: UserDefinedFunction = udf(makePolygon _)
  def ST_MakePoint_UDF: UserDefinedFunction = udf(makePoint _)
  def ST_MakeLine_UDF: UserDefinedFunction = udf(makeLine _)
  def ST_MakePointM_UDF: UserDefinedFunction = udf(makePointM _)
  def ST_MLineFromText_UDF: UserDefinedFunction = udf(mLineFromText _)
  def ST_MPointFromText_UDF: UserDefinedFunction = udf(mPointFromText _)
  def ST_MPolyFromText_UDF: UserDefinedFunction = udf(mPolyFromText _)
  def ST_Point_UDF: UserDefinedFunction = udf(makePoint _)
  def ST_PointFromGeoHash_UDF: UserDefinedFunction = udf(pointFromGeoHash _)
  def ST_PointFromText_UDF: UserDefinedFunction = udf(pointFromText _)
  def ST_PointFromWKB_UDF: UserDefinedFunction = udf(pointFromWKB _)
  def ST_Polygon_UDF: UserDefinedFunction = udf(polygon _)
  def ST_PolygonFromText_UDF: UserDefinedFunction = udf(polygonFromText _)

  private[geomesa] val constructorNames = Map(
    ST_GeomFromGeoHash -> "st_geomFromGeoHash",
    ST_GeomFromWKT -> "st_geomFromWKT",
    ST_GeomFromWKB -> "st_geomFromWKB",
    ST_LineFromText -> "st_lineFromText",
    ST_MakeBox2D -> "st_makeBox2D",
    ST_MakeBBOX -> "st_makeBBOX",
    ST_MakePolygon -> "st_makePolygon",
    ST_MakePoint -> "st_makePoint",
    ST_MakeLine -> "st_makeLine",
    ST_MakePointM -> "st_makePointM",
    ST_MLineFromText -> "st_mLineFromText",
    ST_MPointFromText -> "st_mPointFromText",
    ST_MPolyFromText -> "st_mPolyFromText",
    ST_Point -> "st_point",
    ST_PointFromGeoHash -> "st_pointFromGeoHash",
    ST_PointFromText -> "st_pointFromText",
    ST_PointFromWKB -> "st_pointFromWKB",
    ST_Polygon -> "st_polygon",
    ST_PolygonFromText  -> "st_polygonFromText"
  )

  private[jts] def registerFunctions(sqlContext: SQLContext): Unit = {
    sqlContext.udf.register("st_box2DFromGeoHash", ST_GeomFromGeoHash)
    sqlContext.udf.register(constructorNames(ST_GeomFromGeoHash), ST_GeomFromGeoHash)
    sqlContext.udf.register("st_geomFromText", ST_GeomFromWKT)
    sqlContext.udf.register("st_geometryFromText", ST_GeomFromWKT)
    sqlContext.udf.register(constructorNames(ST_GeomFromWKT), ST_GeomFromWKT)
    sqlContext.udf.register(constructorNames(ST_GeomFromWKB), ST_GeomFromWKB)
    sqlContext.udf.register(constructorNames(ST_LineFromText), ST_LineFromText)
    sqlContext.udf.register(constructorNames(ST_MLineFromText), ST_MLineFromText)
    sqlContext.udf.register(constructorNames(ST_MPointFromText), ST_MPointFromText)
    sqlContext.udf.register(constructorNames(ST_MPolyFromText), ST_MPolyFromText)
    sqlContext.udf.register(constructorNames(ST_MakeBBOX), ST_MakeBBOX)
    sqlContext.udf.register(constructorNames(ST_MakeBox2D), ST_MakeBox2D)
    sqlContext.udf.register(constructorNames(ST_MakeLine), ST_MakeLine)
    sqlContext.udf.register(constructorNames(ST_MakePoint), ST_MakePoint)
    sqlContext.udf.register(constructorNames(ST_MakePointM), ST_MakePointM)
    sqlContext.udf.register(constructorNames(ST_MakePolygon), ST_MakePolygon)
    sqlContext.udf.register(constructorNames(ST_Point), ST_Point)
    sqlContext.udf.register(constructorNames(ST_PointFromGeoHash), ST_PointFromGeoHash)
    sqlContext.udf.register(constructorNames(ST_PointFromText), ST_PointFromText)
    sqlContext.udf.register(constructorNames(ST_PointFromWKB), ST_PointFromWKB)
    sqlContext.udf.register(constructorNames(ST_Polygon), ST_Polygon)
    sqlContext.udf.register(constructorNames(ST_PolygonFromText), ST_PolygonFromText)
  }

  def addOne(x: Int): Int = { x + 1 }
  def addOneUDF(): UserDefinedFunction = udf( addOne _ )

  def toGeom(s: String): String = { WKTUtils.read(s).toString }
  def toGeomUDF(): UserDefinedFunction = udf( toGeom _ )

  def toGeom2(s: String): Geometry = { WKTUtils.read(s) }
  def toGeomUDF2(): UserDefinedFunction = udf( toGeom2 _ )

}
