/*
 * Copyright 2017 Standard Chartered Bank
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.sc.sdm.processor

import com.sc.sdm.models.OpsModels._
import com.sc.sdm.models.SriParams
import org.apache.log4j.Logger
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{GenericRowWithSchema, UnsafeRow}
import org.apache.spark.sql.types.{Metadata, StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}
import org.joda.time.DateTime

import scala.collection.mutable.WrappedArray

/**
  * Entry point for the Source Image construction process
  */
object Sri {

  val logger: Logger = Logger.getLogger(Sri.getClass)

  def main(args: Array[String]) {
    val nArgs = 5
    if (args.length != nArgs) {
      logger.error("Usage : Sri <eod Marker> <business Day> <parameter xml hdfs path> <fulldump|incremental> <comma separated reruntable list")
      sys.error("Usage : Sri <eod Marker> <business Day> <parameter xml hdfs path> <fulldump|incremental> <comma separated reruntable list")
      System.exit(1)
    } else {
      logger.info(s"Start time of the Job ${DateTime.now()}")
      logger.info("Spark Driver arguments - " + args.mkString(" | "))

      val sparkSession = SparkSession
        .builder()
        .appName("SriProcess_0.1")
        .enableHiveSupport()
        //.master("local[*]")
        .getOrCreate()

      sparkSession.conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      sparkSession.conf.set("spark.kryo.registrationRequired", "true")
      sparkSession.conf.set("mapreduce.fileoutputcommitter.algorithm.version", "2")
      sparkSession.conf.set("spark.driver.extraJavaOptions", "-XX:+UseG1GC")
      sparkSession.conf.set("spark.executor.extraJavaOptions", "--XX:+UseG1GC")
      sparkSession.conf.set("spark.scheduler.mode", "FAIR")
      sparkSession.sparkContext.setLogLevel("INFO")

      sparkSession.sparkContext.getConf.registerKryoClasses(
        Array(
          classOf[WrappedArray.ofRef[_]],
          classOf[RowCount],
          classOf[Array[RowCount]],
          classOf[SriParams],
          classOf[Array[Row]],
          Class.forName("com.databricks.spark.avro.DefaultSource$SerializableConfiguration"),
          Class.forName("scala.reflect.ClassTag$$anon$1"), //for SPARK-6497
          Class.forName("java.lang.Class"),
          Class.forName("org.apache.spark.sql.types.StringType$"),
          Class.forName("scala.collection.immutable.Map$EmptyMap$"),
          classOf[Array[InternalRow]],
          classOf[Array[StructField]],
          classOf[Array[Object]],
          classOf[Array[String]],
          classOf[StructField],
          classOf[Metadata],
          classOf[StringType],
          classOf[UnsafeRow],
          classOf[GenericRowWithSchema],
          classOf[StructType],
          classOf[OpsEvent],
          classOf[Array[OpsEvent]],
          classOf[ProcessMetadataEvent],
          classOf[Array[ProcessMetadataEvent]],
          Class.forName("org.apache.spark.sql.execution.joins.UnsafeHashedRelation"),
          Class.forName("org.apache.spark.sql.execution.joins.LongHashedRelation"),
          Class.forName("org.apache.spark.sql.execution.joins.LongToUnsafeRowMap"),
          Class.forName("org.apache.spark.sql.execution.columnar.CachedBatch"),
          classOf[org.apache.spark.sql.catalyst.expressions.GenericInternalRow],
          classOf[org.apache.spark.unsafe.types.UTF8String],
          Class.forName("[[B")
        )
      )

      BootstrapProcessor.businessDateProcess(args)(sparkSession)
    }
  }

}
