/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.execution.datasources.spinach

import java.util.concurrent.TimeUnit

import com.google.common.cache._
import org.apache.hadoop.fs.{FSDataInputStream, FileSystem, Path}
import org.apache.hadoop.mapreduce.TaskAttemptContext
import org.apache.hadoop.util.StringUtils
import org.apache.spark.Logging
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types.StructType

/**
  * Fiber Cache Manager
  */
object FiberCacheManager extends Logging {
  @transient private val cache =
    CacheBuilder
      .newBuilder()
      .concurrencyLevel(4) // DEFAULT_CONCURRENCY_LEVEL TODO verify that if it works
      .weigher(new Weigher[Fiber, FiberByteData] {
        override def weigh(key: Fiber, value: FiberByteData): Int = value.buf.length
      })
      .maximumWeight(MemoryManager.SPINACH_FIBER_CACHE_SIZE_IN_BYTES)
      .removalListener(new RemovalListener[Fiber, FiberByteData] {
        override def onRemoval(n: RemovalNotification[Fiber, FiberByteData]): Unit = {
          MemoryManager.instance.free(n.getValue)
        }
      }).build(new CacheLoader[Fiber, FiberByteData] {
      override def load(key: Fiber): FiberByteData = {
        key.file.getFiberData(key.rowGroupId, key.columnIndex)
      }
    })

  def apply(fiberCache: Fiber): FiberByteData = {
    cache(fiberCache)
  }

  // TODO this will be called via heartbeat
  def status: String = throw new NotImplementedError()
  def update(status: String): Unit = throw new NotImplementedError()
}

private[spinach] case class InputDataFileDescriptor(fin: FSDataInputStream, len: Long)

private[spinach] object DataMetaCacheManager extends Logging {
  @transient private val cache =
    CacheBuilder
      .newBuilder()
      .maximumSize(MemoryManager.SPINACH_DATA_META_CACHE_SIZE)
      .build(new CacheLoader[DataFileScanner, DataFileMeta] {
      override def load(key: DataFileScanner): DataFileMeta = {
        val meta = new DataFileMeta()
        val fd = FiberDataFileHandler(key)
        new DataFileMeta().read(fd.fin, fd.len)
      }
    })

  def apply(fiberCache: DataFileScanner): DataFileMeta = {
    cache(fiberCache)
  }
}

private[spinach] object FiberDataFileHandler extends Logging {
  @transient private val cache =
    CacheBuilder
      .newBuilder()
      .concurrencyLevel(4) // DEFAULT_CONCURRENCY_LEVEL TODO verify that if it works
      .maximumSize(MemoryManager.SPINACH_FIBER_CACHE_SIZE_IN_BYTES)
      .expireAfterAccess(100, TimeUnit.SECONDS) // auto expire after 100 seconds.
      .removalListener(new RemovalListener[DataFileScanner, InputDataFileDescriptor] {
        override def onRemoval(n: RemovalNotification[DataFileScanner, InputDataFileDescriptor])
        : Unit = {
          n.getValue.fin.close()
        }
      }).build(new CacheLoader[DataFileScanner, InputDataFileDescriptor] {
      override def load(key: DataFileScanner): InputDataFileDescriptor = {
        val ctx = SparkHadoopUtil.get.getConfigurationFromJobContext(key.context)
        val path = new Path(StringUtils.unEscapeString(key.path))
        val fs = FileSystem.get(ctx)

        InputDataFileDescriptor(fs.open(path), fs.getFileStatus(path).getLen)
      }
    })

  def apply(fiberCache: DataFileScanner): InputDataFileDescriptor = {
    cache(fiberCache)
  }
}

case class FiberByteData(buf: Array[Byte]) // TODO add FiberDirectByteData

private[spinach] case class Fiber(file: DataFileScanner, columnIndex: Int, rowGroupId: Int)

private[spinach] case class DataFileScanner(
    path: String, schema: StructType, context: TaskAttemptContext) {
  val meta: DataFileMeta = DataMetaCacheManager(this)

  override def hashCode(): Int = path.hashCode
  override def equals(that: Any): Boolean = that match {
    case DataFileScanner(thatPath, _, _) => path == thatPath
    case _ => false
  }

  def getFiberData(groupId: Int, fiberId: Int): FiberByteData = {
    val is = FiberDataFileHandler(this).fin
    val groupMeta = meta.rowGroupsMeta(groupId)
    // get the fiber data start position TODO update the meta to store the fiber start pos
    var i = 0
    var fiberStart = groupMeta.start
    while (i < fiberId) {
      fiberStart += groupMeta.fiberLens(i)
      i += 1
    }
    val lens = groupMeta.fiberLens(fiberId)
    val bytes = new Array[Byte](lens)

    is.synchronized {
      is.seek(fiberStart)
      is.read(bytes)
    }
    new FiberByteData(bytes)
  }

  def iterator(requiredIds: Array[Int]): Iterator[InternalRow] = {
    val row = new BatchColumn()
    val columns: Array[ColumnValues] = new Array[ColumnValues](requiredIds.length)
    (0 until meta.groupCount).iterator.flatMap { groupId =>
      var i = 0
      while (i < columns.length) {
        columns(i) = new ColumnValues(
          meta.rowCountInEachGroup,
          schema(requiredIds(i)).dataType,
          FiberCacheManager(Fiber(this, requiredIds(i), groupId)))
        i += 1
      }

      if (groupId < meta.groupCount - 1) {
        // not the last row group
        row.reset(meta.rowCountInEachGroup, columns).toIterator
      } else {
        row.reset(meta.rowCountInLastGroup, columns).toIterator
      }
    }
  }
}
