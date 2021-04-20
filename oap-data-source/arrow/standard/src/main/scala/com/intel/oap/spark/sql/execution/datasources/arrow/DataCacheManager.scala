package com.intel.oap.spark.sql.execution.datasources.arrow

import org.apache.arrow.dataset.scanner.ScanTask

import scala.collection.mutable.HashMap


class DataCacheManager {
  val dataCacheManager = new HashMap[String, List[ScanTask.ArrowBundledVectors]]()
}

object DataCacheManager {
  private val _data = new DataCacheManager

  def getVectorData(file: String): Option[List[ScanTask.ArrowBundledVectors]] = {
    _data.dataCacheManager.get(file)
  }

  def saveVectorData(file: String, vectorData: List[ScanTask.ArrowBundledVectors]): Unit = {
    val vData = _data.dataCacheManager.get(file)
    vData match {
      case Some(list) =>
        _data.dataCacheManager.put(file, list ++ vectorData)
      case None =>
        _data.dataCacheManager.put(file, vectorData)
    }
  }
}
