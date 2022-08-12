/***********************************************************************
 * Copyright (c) 2013-2022 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.lambda.data

import com.github.benmanes.caffeine.cache.LoadingCache
import com.typesafe.scalalogging.StrictLogging
import org.geotools.data.simple.SimpleFeatureReader
import org.geotools.data.{DataStore, FeatureReader, Query, Transaction}
import org.locationtech.geomesa.filter.filterToString
import org.locationtech.geomesa.index.audit.QueryEvent
import org.locationtech.geomesa.index.geoserver.ViewParams
import org.locationtech.geomesa.index.geotools.GeoMesaDataStore
import org.locationtech.geomesa.index.utils.{ExplainLogger, Explainer}
import org.locationtech.geomesa.index.view.MergedQueryRunner
import org.locationtech.geomesa.index.view.MergedQueryRunner.DataStoreQueryable
import org.locationtech.geomesa.lambda.data.LambdaQueryRunner.TransientQueryable
import org.locationtech.geomesa.lambda.stream.TransientStore
import org.locationtech.geomesa.utils.collection.{CloseableIterator, SelfClosingIterator}
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

class LambdaQueryRunner(ds: LambdaDataStore, persistence: DataStore, transients: LoadingCache[String, TransientStore])
    extends MergedQueryRunner(
      ds, Seq(TransientQueryable(transients) -> None, DataStoreQueryable(persistence) -> None), true, false) {

  import org.locationtech.geomesa.index.conf.QueryHints.RichHints

  // TODO pass explain through?

  override def runQuery(sft: SimpleFeatureType, query: Query, explain: Explainer): CloseableIterator[SimpleFeature] = {
    val hints = configureQuery(sft, query).getHints // configure the query so we get viewparams, transforms, etc
    if (hints.isLambdaQueryPersistent && hints.isLambdaQueryTransient) {
      super.runQuery(sft, query, explain)
    } else if (hints.isLambdaQueryPersistent) {
      SelfClosingIterator(persistence.getFeatureReader(query, Transaction.AUTO_COMMIT))
    } else {
      // ensure that we still audit the query
      val audit = Option(persistence).collect { case ds: GeoMesaDataStore[_] => ds.config.audit }.flatten
      audit.foreach { case (writer, provider, typ) =>
        val stat = QueryEvent(
          s"$typ-lambda",
          sft.getTypeName,
          System.currentTimeMillis(),
          provider.getCurrentUserId,
          filterToString(query.getFilter),
          ViewParams.getReadableHints(query),
          0,
          0,
          0
        )
        writer.writeEvent(stat) // note: implementations should be asynchronous
      }
      transients.get(sft.getTypeName)
          .read(Option(query.getFilter), Option(query.getPropertyNames), Option(query.getHints), explain)
    }
  }
}

object LambdaQueryRunner {

  import org.locationtech.geomesa.index.conf.QueryHints.RichHints

  case class TransientQueryable(transients: LoadingCache[String, TransientStore])
      extends MergedQueryRunner.Queryable with StrictLogging {
    override def getFeatureReader(q: Query, t: Transaction): FeatureReader[SimpleFeatureType, SimpleFeature] = {
      val store = transients.get(q.getTypeName)
      val explain = new ExplainLogger(logger)
      val iter = store.read(Option(q.getFilter), Option(q.getPropertyNames), Option(q.getHints), explain)

      new SimpleFeatureReader() {
        override def getFeatureType: SimpleFeatureType = q.getHints.getReturnSft
        override def hasNext: Boolean = iter.hasNext
        override def next(): SimpleFeature = iter.next()
        override def close(): Unit = iter.close()
      }
<<<<<<< HEAD
      hints.put(QueryHints.Internal.SKIP_REDUCE, java.lang.Boolean.TRUE)
      reducer(standardQuery(sft, query, explain))
    } else {
      standardQuery(sft, query, explain)
    }
  }

  private def standardQuery(sft: SimpleFeatureType, query: Query, explain: Explainer): CloseableIterator[SimpleFeature] = {
    import scala.concurrent.ExecutionContext.Implicits.global

    val transientFeatures =
      transients.get(sft.getTypeName)
          .read(Option(query.getFilter), Option(query.getPropertyNames), Option(query.getHints), explain)
    val fc = persistence.getFeatureSource(sft.getTypeName).getFeatures(query)
    // kick off the persistent query in a future, but don't wait for results yet
    val persistentFeatures = Future(CloseableIterator(fc.features))
    // ++ is evaluated lazily, so we will block on the persistent features once the transient iterator is exhausted
    transientFeatures ++ Await.result(persistentFeatures, Duration.Inf)
  }

  override protected [geomesa] def getReturnSft(sft: SimpleFeatureType, hints: Hints): SimpleFeatureType = {
    if (hints.isBinQuery) {
      BinaryOutputEncoder.BinEncodedSft
    } else if (hints.isArrowQuery) {
      org.locationtech.geomesa.arrow.ArrowEncodedSft
    } else if (hints.isDensityQuery) {
      DensityScan.DensitySft
    } else if (hints.isStatsQuery) {
      StatsScan.StatsSft
    } else {
      super.getReturnSft(sft, hints)
=======
>>>>>>> main
    }
  }
}
