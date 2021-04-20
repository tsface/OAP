package com.intel.oap.spark.sql.execution.datasources.arrow

import org.apache.arrow.dataset.scanner.ScanTask

import scala.collection.mutable

class DataCacheManager {
  val dataCacheManager = new mutable.HashMap[String,List[ScanTask.ArrowBundledVectors]]()
}
object DataCacheManager{
  private val _data = new DataCacheManager
  def getVectorData(file : String): Option[List[ScanTask.ArrowBundledVectors]] ={
    _data.dataCacheManager.get(file)
  }
}
