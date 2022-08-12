/***********************************************************************
 * Copyright (c) 2013-2022 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.hbase.server.common

import org.apache.hadoop.hbase.Cell
import org.apache.hadoop.hbase.regionserver.RegionScanner
import org.locationtech.geomesa.index.iterators.AggregatingScan
import org.locationtech.geomesa.index.iterators.AggregatingScan.RowValue

<<<<<<< HEAD:geomesa-hbase/geomesa-hbase-datastore/src/main/scala/org/locationtech/geomesa/hbase/coprocessor/aggregators/HBaseAggregator.scala
=======
/**
 * HBase mixin for aggregating scans
 *
 * @tparam T aggregate result type
 */
>>>>>>> main:geomesa-hbase/geomesa-hbase-server/src/main/scala/org/locationtech/geomesa/hbase/server/common/HBaseAggregator.scala
trait HBaseAggregator[T <: AggregatingScan.Result] extends AggregatingScan[T] {

  private val results = new java.util.ArrayList[Cell]
  private var scanner: RegionScanner = _
  private var more: Boolean = false
  private var iter: java.util.Iterator[Cell] = _
  private var cell: Cell = _

  def setScanner(scanner: RegionScanner): Unit = {
    this.scanner = scanner
    results.clear()
    cell = null
    more = scanner.next(results)
    iter = results.iterator()
  }

<<<<<<< HEAD:geomesa-hbase/geomesa-hbase-datastore/src/main/scala/org/locationtech/geomesa/hbase/coprocessor/aggregators/HBaseAggregator.scala
=======
  def getLastScanned: Array[Byte] = {
    if (cell == null) { null } else {
      val bytes = Array.ofDim[Byte](cell.getRowLength)
      System.arraycopy(cell.getRowArray, cell.getRowOffset, bytes, 0, bytes.length)
      bytes
    }
  }

>>>>>>> main:geomesa-hbase/geomesa-hbase-server/src/main/scala/org/locationtech/geomesa/hbase/server/common/HBaseAggregator.scala
  override protected def hasNextData: Boolean = iter.hasNext || more && {
    results.clear()
    more = scanner.next(results)
    iter = results.iterator()
    hasNextData
  }

  override protected def nextData(): RowValue = {
<<<<<<< HEAD:geomesa-hbase/geomesa-hbase-datastore/src/main/scala/org/locationtech/geomesa/hbase/coprocessor/aggregators/HBaseAggregator.scala
    val cell = iter.next()
=======
    cell = iter.next()
>>>>>>> main:geomesa-hbase/geomesa-hbase-server/src/main/scala/org/locationtech/geomesa/hbase/server/common/HBaseAggregator.scala
    RowValue(cell.getRowArray, cell.getRowOffset, cell.getRowLength,
      cell.getValueArray, cell.getValueOffset, cell.getValueLength)
  }
}
