package com.intel.oap.spark.sql.execution.datasources.arrow

import java.util
import java.util.UUID

import org.apache.arrow.dataset.scanner.ScanTask
import org.apache.arrow.memory.BaseAllocator
import org.apache.arrow.vector.VectorSchemaRoot
import org.apache.arrow.vector.dictionary.Dictionary
import org.apache.arrow.vector.types.pojo.DictionaryEncoding
import org.apache.spark.internal.Logging
import org.apache.spark.sql.execution.datasources.v2.arrow.{SparkMemoryUtils, SparkMemoryUtils1}
import org.apache.spark.sql.execution.datasources.v2.arrow.SparkMemoryUtils.{ExecutionMemoryAllocationListener, close, getLocalTaskContext, getTaskMemoryManager, softClose, taskToAllocatorMap}
import org.apache.spark.util.TaskCompletionListener

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

        import org.apache.arrow.vector.VectorUnloader
        val unloader = new VectorUnloader(oldValueVectors)
        import org.apache.arrow.vector.ipc.message.ArrowRecordBatch
        val recordBatch = unloader.getRecordBatch


        val new_root =
          VectorSchemaRoot.create(oldValueVectors.getSchema, SparkMemoryUtils1.arrowAllocator(file))
        import org.apache.arrow.vector.VectorLoader
        val loader = new VectorLoader(new_root)
        loader.load(recordBatch)


//        val newValueVectors = oldValueVectors.slice(0, oldValueVectors.getRowCount)
        val newDic = new util.HashMap[java.lang.Long, Dictionary]()
        x.dictionaryVectors.forEach {
          (k, v) =>
            newDic.put(k, new Dictionary(v.getVector,
              new DictionaryEncoding(v.getEncoding.getId, v.getEncoding.isOrdered, v.getEncoding.getIndexType)))
        }
        new ScanTask.ArrowBundledVectors(new_root, newDic)
    }
    vData match {
      case Some(list) =>
        _data.dataCacheManager.put(file, list ++ newVectorData)
      case None =>
        _data.dataCacheManager.put(file, newVectorData)
    }
  }
}
