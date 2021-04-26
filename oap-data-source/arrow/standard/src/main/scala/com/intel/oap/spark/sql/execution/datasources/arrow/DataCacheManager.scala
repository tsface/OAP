package com.intel.oap.spark.sql.execution.datasources.arrow

import java.util

import org.apache.arrow.dataset.scanner.ScanTask
import org.apache.arrow.vector.dictionary.Dictionary
import org.apache.arrow.vector.types.pojo.DictionaryEncoding
import org.apache.spark.internal.Logging

import scala.collection.mutable.HashMap


class DataCacheManager {
  val dataCacheManager = new HashMap[String, List[ScanTask.ArrowBundledVectors]]()
}

object DataCacheManager extends Logging {
  private val _data = new DataCacheManager

  def getVectorData(file: String): Option[List[ScanTask.ArrowBundledVectors]] = {
    logError(s"-=-==-=-getVectorData , file = ${file}")
    _data.dataCacheManager.get(file)
  }

  def saveVectorData(file: String, vectorData: List[ScanTask.ArrowBundledVectors]): Unit = {
    logError(s"-=-==-=-saveVectorData , file = ${file}")
    val vData = _data.dataCacheManager.get(file)
    val newVectorData = vectorData.map {
      x =>
        val oldValueVectors = x.valueVectors
        val newValueVectors = oldValueVectors.slice(0, oldValueVectors.getRowCount)
        val newDic = new util.HashMap[java.lang.Long, Dictionary]()
        x.dictionaryVectors.forEach {
          (k, v) =>
            newDic.put(k, new Dictionary(v.getVector,
              new DictionaryEncoding(v.getEncoding.getId, v.getEncoding.isOrdered, v.getEncoding.getIndexType)))
        }
        new ScanTask.ArrowBundledVectors(newValueVectors, newDic)
    }
    vData match {
      case Some(list) =>
        _data.dataCacheManager.put(file, list ++ newVectorData)
      case None =>
        _data.dataCacheManager.put(file, newVectorData)
    }
  }
}
