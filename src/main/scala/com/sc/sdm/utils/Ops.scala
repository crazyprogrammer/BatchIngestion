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

package com.sc.sdm.utils

import java.sql.Timestamp
import java.util.Date

import com.sc.sdm.models.OpsModels._
import com.sc.sdm.models.SriParams
import com.sc.sdm.processor.SriOutput
import com.sc.sdm.utils.Enums.RunType.RunType
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

/** A composition of utility and persistence functions for operation tables **/
object Ops {

  /**
    * This function is used to write
    *   1. Duplicates in Verify Types in case of Incremental.
    *   2. Previous SRI vs VerifyTypes in case of FullDump.
    *
    * @param sriParams  instance of [[SriParams]]
    * @param spark      Spark Session
    * @param duplicates Dataframe that wraps the duplicate row ids
    * @param tableName  Table that corresponds to these duplicate entries
    */
  def writeDuplicates(sriParams: SriParams, duplicates: DataFrame, tableName: String)(implicit spark: SparkSession): Unit = {
    import sriParams._

    val duplicatesTableName = s"${source}_${country}_duplicates"
    val duplicatesDf = duplicates.select("ROWID", "DUPLICATE_ROWID").withColumn("STEPNAME", lit("SRI")).withColumn("TABLENAME", lit(tableName)).withColumn("asof", lit(SdmConstants.nowAsString()))
    val targetPath = hdfsBaseDir + sriParams.opsSchema + "/" + duplicatesTableName
    SriUtils.persistAsHiveTable(duplicatesDf, targetPath, sriParams.opsSchema, duplicatesTableName, "dds", sriParams.businessDate, SaveMode.Append)
  }

  /**
    * Persists the [[com.sc.sdm.models.OpsModels.RowCount]] [[DataFrame]] to the appropriate HDFS location and creates a
    * Hive partition over it
    *
    * @param sriParams   instance of [[SriParams]] for this job
    * @param rowCountsDf [[DataFrame]] of [[com.sc.sdm.models.OpsModels.RowCount]] objects
    * @param spark       Spark Session
    */
  def writeRowCounts(sriParams: SriParams, rowCountsDf: DataFrame)(implicit spark: SparkSession): Unit = {
    val rowCountTableName: String = sriParams.source + "_" + sriParams.country + "_rowcounts"
    val processMetadataTableLocation = s"${sriParams.hdfsBaseDir}${sriParams.opsSchema}/$rowCountTableName"
    val partition = s"""rcds=${sriParams.businessDate}"""
    val partitionQuoted = s"""rcds='${sriParams.businessDate}'"""
    rowCountsDf
      .write
      .format("orc")
      .mode(SaveMode.Overwrite)
      .save(s"$processMetadataTableLocation/$partition")

    SriUtils.createHivePartition(sriParams.opsSchema, rowCountTableName, partitionQuoted)
  }


  /**
    * Given the result of the Sri process [[SriOutput]] and verifyTyples generates a [[Seq]] of loggable [[RowCount]] instances
    */
  def getRowCount(sriParams: SriParams, tableName: String, sourceType: String, verifyTypes: DataFrame, sriOutput: SriOutput, earlyCount: Long, attemptId: String): Seq[RowCount] = {
    val nowAsString: String = SdmConstants.nowAsString()

    val stepName = if (sourceType.equals("delta")) "SRI OPEN" else "SRI TXN"

    Seq(
      RowCount(sriParams.sriOpenSchema, tableName, "Sri Open", sriOutput.sriOpen.count(), tableName, nowAsString, stepName, attemptId),
      RowCount(sriParams.sriNonOpenSchema, tableName, "Sri Non Open", sriOutput.sriNonOpen.count(), tableName, nowAsString, "SRI NONOPEN", attemptId),
      RowCount(sriParams.verifyTypesSchema, "BRecordsSri", "Storage", sriOutput.bRecords.map(_.count()).getOrElse(0), tableName, nowAsString, "BRecordsSri", attemptId),
      RowCount(sriParams.opsSchema, "Duplicates", "Storage", sriOutput.vTypesDuplicates.count(), tableName, nowAsString, "Duplicates", attemptId),
      RowCount(sriParams.opsSchema, "FullDumpDuplicates", "Storage", sriOutput.prevSriVsVtypesDuplicates.map(_.count()).getOrElse(0), tableName, nowAsString, "FullDumpDuplicates", attemptId),
      RowCount(sriParams.verifyTypesSchema, "VerifyTypesSri", "Storage", verifyTypes.count(), tableName, nowAsString, "VerifyTypesSri", attemptId),
      RowCount(sriParams.verifyTypesSchema, "EarlyArrivalSri", "Storage", earlyCount, tableName, nowAsString, "EarlyArrivalSri", attemptId)
    )
  }


  /**
    * Persists the [[com.sc.sdm.models.OpsModels.ProcessMetadataEvent]] [[DataFrame]] to the appropriate HDFS location and creates a
    * Hive partition over it
    *
    * @param events    [[DataFrame]] of [[ProcessMetadataEvent]] objects
    * @param sriParams instance of [[SriParams]] for this job
    * @param spark     Spark Session
    */
  def writeProcessMetadataEvents(events: List[ProcessMetadataEvent], sriParams: SriParams)(implicit spark: SparkSession): Unit = {

    val processMetadataTableLocation = s"${sriParams.hdfsBaseDir}${sriParams.opsSchema}/${sriParams.source}_process_metadata"
    val partition = s"""partition_date=${sriParams.businessDate}"""
    val partitionQuoted = s"""partition_date='${sriParams.businessDate}'"""

    import spark.sqlContext.implicits._
    spark.sparkContext.parallelize(events)
      .toDF()
      .write
      .format("orc")
      .mode(SaveMode.Overwrite)
      .save(s"$processMetadataTableLocation/$partition")

    SriUtils.createHivePartition(sriParams.opsSchema, s"${sriParams.source}_process_metadata", partitionQuoted)
  }

  /** Converts the simplified version of Operational Events ([[com.sc.sdm.models.OpsModels.OpsEvent]] to the full version ([[com.sc.sdm.models.OpsModels.ProcessMetadataEvent]] */
  def convertToProcessMetadataEvents(opsEvents: List[OpsEvent], tableName: String, runType: RunType, sriParams: SriParams): List[ProcessMetadataEvent] = {
    opsEvents.map { opsEvent =>
      ProcessMetadataEvent(new Timestamp(new Date().getTime), "processing", tableName: String, "completed", opsEvent.event,
        opsEvent.time, "processing", "", runType.toString, sriParams.businessDate, "", sriParams.source,
        sriParams.country, "Sri Processing")
    }
  }

  /**
    * Persists the EOD Record for today to the appropriate HDFS location and creates a
    * Hive partition over it
    *
    * @param paramConf Unparsed configuration for this Job
    * @param sriParams instance of [[SriParams]] for this job
    * @param previousBusinessDate
    * @param spark     Spark Session
    */
  def writeEodTableAndMarker(paramConf: Configuration, sriParams: SriParams, previousBusinessDate: String)(implicit spark: SparkSession): Unit = {
    import sriParams._
    val eodTableRecord = List(EODTableRecord(source, country, sriParams.eodMarker, sriParams.businessDate, previousBusinessDate))
    val tableName = paramConf.get("edmhdpif.config.eod.table.name", "eod_table")

    val eodTableLocation = s"${sriParams.hdfsBaseDir}/${sriParams.opsSchema}/$tableName"
    val partition = s"""businessdate=${sriParams.businessDate}"""
    val partitionQuoted = s"""businessdate='${sriParams.businessDate}'"""
    spark.createDataFrame(eodTableRecord)
      .write
      .format("orc")
      .mode(SaveMode.Overwrite)
      .save(s"$eodTableLocation/$partition")

    SriUtils.createHivePartition(sriParams.opsSchema, tableName, partitionQuoted)
  }


  /**
    * If the EOD marker strategy is table-level, then this function will retrieve all the EOD markers for the tables in this source application.
    *
    * @param spark                Instance of [[SparkSession]]
    * @param sriParams            Instance of [[SriParams]]
    * @param paramConf            Unparsed Hadoop [[Configuration]]
    * @param previousBusinessDate Previous business date, in order to lookup the markers
    * @return List of marker times
    */
  def parseEodMarkerForTable(spark: SparkSession, sriParams: SriParams, paramConf: Configuration, previousBusinessDate: String): List[TableLevelEodMarkers] = {
    if (sriParams.isTableLevelEod) {
      val markerTable = sriParams.getVerifyTypesPath(paramConf.get("edmhdpif.config.eod.marker.table.name"))
      val reconTablePartitionPath = sriParams.getVerifyTypesPartitionPath(markerTable, previousBusinessDate)
      val colName = paramConf.get("edmhdpif.config.eod.marker.column.name")
      val tableColName = paramConf.get("edmhdpif.config.eod.marker.column.table.name")
      val markerDetails = spark.read.format("orc").load(reconTablePartitionPath + "*")
        .select(colName, tableColName).rdd
        .map { row =>
          //FIXME This is incorrect. This has to be sourced from timestampcolformat
          val marker = SdmConstants.yyyyMMddHHmmssFormat.parseDateTime(row.getString(0)).toDate
          val tableName = row.getString(1)
          (tableName, marker)
        }.groupByKey()
        .mapValues(x => x.max(Ordering.by[Date, Long](_.getTime())))
        .collect()
      //FIXME Converting Date to string again.
      markerDetails.map { x => TableLevelEodMarkers(x._1, x._2.toString) }.toList
    }
    else List.empty
  }

  /** Gets the Previous business day's global EOD time */
  def getEODMarkerFor(fs: FileSystem, paramConf: Configuration, sriParams: SriParams, businessDate: String)(implicit spark: SparkSession): Option[String] = {
    val tableName = paramConf.get("edmhdpif.config.eod.table.name", "eod_table")

    val previousEodRecordPath = s"${sriParams.hdfsBaseDir}/${sriParams.opsSchema}/$tableName/businessdate=$businessDate"

    val pathExists: PartialFunction[String, Option[String]] = {
      case path if fs.exists(new Path(path)) =>
        val prevEodMarkerFrame = spark.read.format("orc").load(path).select("markertime")
        if (prevEodMarkerFrame.count() == 1) {
          Some(prevEodMarkerFrame.collect()(0).get(0).toString)
        }
        else None
    }

    val pathDoesNotExist: PartialFunction[String, Option[String]] = {
      case _ => None
    }

    pathExists.orElse(pathDoesNotExist)(previousEodRecordPath)
  }

  /**
    * Create a status file
    *
    * @param fs        HDFS FS
    * @param paramConf parameter configuration
    * @param sriParams sri params
    * @param scope     scope : global/table
    * @param marker    name of status marker file
    * @param content   Optional file content
    */
  def createStatusFile(fs: FileSystem, paramConf: Configuration, sriParams: SriParams, scope: String, marker: String, content: String = ""): Unit = {
    fs.mkdirs(new Path(paramConf.get("edmhdpif.config.statusfile.location", "target/") + s"$scope/"))
    val outputStream = fs.create(new Path(paramConf.get("edmhdpif.config.statusfile.location", "target/") + "/" + s"$scope/${sriParams.businessDate}" + marker), true)
    outputStream.writeUTF(content)
    outputStream.close()
  }
}
