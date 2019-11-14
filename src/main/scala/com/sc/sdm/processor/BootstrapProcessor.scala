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

import java.util.{Date, UUID}

import com.sc.sdm.models.OpsModels._
import com.sc.sdm.models.SriParams
import com.sc.sdm.utils.Enums.{RunType, TableType}
import com.sc.sdm.utils.PartitionUtils.getNextBusinessDatePartition
import com.sc.sdm.utils.ReconUtils._
import com.sc.sdm.utils.SdmConstants.PartitionFormatString
import com.sc.sdm.utils.SriUtils._
import com.sc.sdm.utils.XmlParser._
import com.sc.sdm.utils._
import org.apache.commons.lang3.StringUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.log4j.Logger
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

import scala.collection.GenSeq
import scala.collection.mutable.ArrayBuffer
import scala.collection.parallel.ForkJoinTaskSupport
import scala.util.{Failure, Success, Try}

/**
  * This is the core workhorse of the Sri application. While the [[Sri]] sets the configuration,
  * the building of the [[TableConfig]] and the [[SriParams]] is done within this class. Once that's done, Sri process for
  * each table is driven out of this class.
  */
object BootstrapProcessor {
  val logger: Logger = Logger.getLogger(RerunProcessor.getClass)

  /**
    * Prepares the configuration before invoking the processSri
    *
    * @param args       Spark program arguments
    * @param spark Spark Session
    */
  def businessDateProcess(args: Array[String])(implicit spark: SparkSession): Unit = {

    val Array(eodMarker, submitTime, paramPath, runType, rerunTableList) = args

    val fs: FileSystem = FileSystemUtils.fetchFileSystem
    val paramConf: Configuration = getParamConf(fs, paramPath)
    val todaysPartition: String = getBusinessDayPartition(submitTime, paramConf.get("edmhdpif.config.timestampcolformat")) match {
      case Some (part) => part
      case None => throw new scala.Exception(" Business date and todays partition format were incorrectly specified!")
    }

    val format = paramConf.get("edmhdpif.config.storage.input.format", "com.databricks.spark.avro")
    val sriParams: SriParams = SriParams(paramConf, todaysPartition, todaysPartition, runType, eodMarker)

    val tableConfigs = parseXmlConfig(sriParams.tableConfigXml, fs)
    val rerunTables: List[String] = RerunProcessor.getRerunTableList(rerunTableList, fs, tableConfigs)

    val tableDictionary = if (rerunTables.isEmpty) tableConfigs else tableConfigs.filter(x => rerunTables.contains(x.name))

    try {
      processSri(sriParams, fs, tableDictionary, paramConf)
      if (sriParams.reconTableName.nonEmpty) {
        recon(sriParams, tableDictionary, fs, paramConf)
      }
      Ops.createStatusFile(fs, paramConf, sriParams, "global", "_COMPLETED_PROCESSING_BATCH")
    }
    catch {
      case e: Exception =>
        logger.error("Process SRI encountered error: ", e)
        Ops.createStatusFile(fs, paramConf, sriParams, "global", "_ERROR_PROCESSING_BATCH", e.getStackTrace.mkString("\n"))
    } finally {
      //cascaded rerun only if any of the tables are delta tables
      RerunProcessor.cascadedReruns(paramPath, runType, rerunTableList, todaysPartition, rerunTables, paramConf, sriParams, tableDictionary)
    }
  }

  /**
    * The root method for Source Image processing
    *
    * @param sriParams       Global Parameters
    * @param fs              Hadoop File system
    * @param tableDictionary Table configs
    * @param paramConf       Hadoop Configuration
    * @param spark              SparkSession
    */
  def processSri(sriParams: SriParams, fs: FileSystem, tableDictionary: List[TableConfig], paramConf: Configuration)
                (implicit spark: SparkSession) = {

    val markers = Ops.parseEodMarkerForTable(spark, sriParams, paramConf, getPreviousExistingSriPartition(sriParams, sriParams.businessDate, tableDictionary.head.name, 10))

    val jobUuid = UUID.randomUUID().toString

    val tableConfigs: GenSeq[TableConfig] with Immutable =
      if (sriParams.parallelExecution) {
        //val parTableDictionary = tableDictionary.filterNot(_.name.isEmpty).filter(_.name=="a1_mantas_acctemailaddr").par
        val parTableDictionary = tableDictionary.filterNot(_.name.isEmpty).par
        parTableDictionary.tasksupport = new ForkJoinTaskSupport(new scala.concurrent.forkjoin.ForkJoinPool(100))
        parTableDictionary
      } else {
        tableDictionary.filterNot(_.name.isEmpty)
      }

    val auditOutputs: GenSeq[AuditOutput] = tableConfigs.flatMap { tableConfig =>

      val start = System.currentTimeMillis

      val processMetadataEvents = ArrayBuffer[OpsEvent]()

      val runType = tableConfig.runType match {
        case Some(lRunType) => RunType.fromString(lRunType).get
        case None if sriParams.fullDump => RunType.FULLDUMP
        case _ => RunType.INCREMENTAL
      }

      val marker = markers.filter(x => tableConfig.name.equals(x.tableName)).map(_.marker).headOption
      val updatedSriParams = sriParams.copy(hdfsTmpDir = sriParams.hdfsTmpDir + UUID.randomUUID().toString.substring(3), eodMarker = marker.getOrElse(sriParams.eodMarker))
      val businessDateContext = getInputFiles(updatedSriParams, fs, tableConfig)

      val auditOutput = Try {
        fs.mkdirs(new Path(sriParams.hdfsTmpDir))
        fs.deleteOnExit(new Path(sriParams.hdfsTmpDir))

        logger.info("Fetching Verify Types for " + tableConfig.name)
        processMetadataEvents += OpsEvent(new Date().toString, s"Fetching Verify Types for ${tableConfig.name}")

        val previousEodMarkerStr = Ops.getEODMarkerFor(fs, paramConf, sriParams, businessDateContext.previousBusinessDate).getOrElse("1970-01-01 00:00:00")
        logger.info(s"Current EOD Marker time for Table ${tableConfig.name}  : ${updatedSriParams.eodMarker}")
        logger.info(s"Previous EOD Marker time for Table ${tableConfig.name} : $previousEodMarkerStr")
        val tomorrow: String = getNextBusinessDatePartition(sriParams.businessDate, PartitionFormatString, "days", 1)

        //Fetch VerifyTypes
        val vTypesOfYesterday =
          getVerifyTypes(fs, sriParams, tableConfig, businessDateContext.previousBusinessDate)
            .filter(date_format(col(sriParams.timestampCol), sriParams.timestampColFormat)
              .gt(date_format(lit(previousEodMarkerStr), sriParams.timestampColFormat)))

        val vTypesOfTomorrow =
          getVerifyTypes(fs, sriParams, tableConfig, tomorrow)
            .filter(date_format(col(sriParams.timestampCol), sriParams.timestampColFormat)
              .leq(date_format(lit(updatedSriParams.eodMarker), sriParams.timestampColFormat)))

        val vTypesOfTodayIncludingEarly = getVerifyTypes(fs, sriParams, tableConfig, sriParams.businessDate)
        val vTypesOfToday = vTypesOfTodayIncludingEarly
          .filter(date_format(col(sriParams.timestampCol), sriParams.timestampColFormat)
            .gt(date_format(lit(previousEodMarkerStr), sriParams.timestampColFormat)))
          .filter(date_format(col(sriParams.timestampCol), sriParams.timestampColFormat)
            .leq(date_format(lit(updatedSriParams.eodMarker), sriParams.timestampColFormat)))

        val consolidatedVerifyTypes = vTypesOfToday.union(vTypesOfYesterday).union(vTypesOfTomorrow).transformExceptionalTypes(sriParams, tableConfig)

        val previousSri = if (fs.exists(new Path(businessDateContext.sriOpenPrevPartitionPath))) {
          spark.read.format("orc").load(businessDateContext.sriOpenPrevPartitionPath)
        } else {
          val tableStructType = fetchSriStructure(tableConfig, sriParams.sriOpenPartitionColumn, sriParams.isCdcSource)
          spark.createDataFrame(spark.sparkContext.emptyRDD[Row], tableStructType)
        }

        val primaryKeyCols = XmlParser.fetchKeyColsFromTable(tableConfig)
        val tableColNames = XmlParser.fetchColNamesFromTable(tableConfig)

        logger.info("Processing Sri for " + tableConfig.name)
        processMetadataEvents += OpsEvent(new Date().toString, s"Processing Sri for ${tableConfig.name}")

        val vTypesAuditCols = XmlParser.getColumnConfigs(sriParams.vTypesAuditColSchema, "\\^").map(_.name)
        val sriAuditCols = XmlParser.getColumnConfigs(sriParams.sriAuditColSchema, "\\^").map(_.name)
        val systemAuditCols = XmlParser.getColumnConfigs(sriParams.systemAuditColSchema, "\\^").map(_.name)


        val sriOutput@SriOutput(sriOpen, sriNonOpen, vTypesDuplicates, prevSriVsVtypesDuplicates, _) =
          if (sriParams.isRecordTypeBatch || sriParams.isCdcSource) {

            val recordTypeMeta = getRecordTypeMetaInfo(sriParams, tableConfig, vTypesAuditCols, sriAuditCols, systemAuditCols)
            val recordTypeBasedProcessor = new RecordTypeBasedProcessor
            recordTypeBasedProcessor.processSri(tableConfig.name, tableColNames, primaryKeyCols, consolidatedVerifyTypes,
              previousSri, recordTypeMeta, TableType.fromString(tableConfig.sourceType).get, runType, sriParams.businessDt)
          }
          else {

            val metaInfo = NonRecordTypeMetaInfo(
              timestampColName = sriParams.timestampCol,
              timestampColFormat = sriParams.timestampColFormat,
              vTypesAuditCols, sriAuditCols, systemAuditCols
            )
            val nonRecordTypeBasedProcessor = new NonRecordTypeBasedProcessor
            nonRecordTypeBasedProcessor.processSri(tableConfig.name, tableColNames, primaryKeyCols, consolidatedVerifyTypes,
              previousSri, metaInfo, TableType.fromString(tableConfig.sourceType).get, runType, sriParams.businessDt)
          }

        //SRI OPEN
        logger.info("Writing SriOpen " + tableConfig.name)
        processMetadataEvents += OpsEvent(new Date().toString, s"Writing SriOpen ${tableConfig.name}")
        SriUtils.persistAsHiveTable(sriOpen, sriParams.getSriOpenPath(tableConfig.name), sriParams.sriOpenSchema, tableConfig.name, sriParams.sriOpenPartitionColumn, sriParams.businessDate)

        //SRI NON-OPEN
        logger.info("Writing SriNonOpen " + tableConfig.name)
        processMetadataEvents += OpsEvent(new Date().toString, s"Writing SriNonOpen ${tableConfig.name}")
        SriUtils.persistAsHiveTable(sriNonOpen, sriParams.getSriNonOpenPath(tableConfig.name), sriParams.sriNonOpenSchema, tableConfig.name, sriParams.sriNonOpenPartitionColumn, sriParams.businessDate)

        //VTypes DUPLICATES
        logger.info("Writing Duplicates for " + tableConfig.name)
        Ops.writeDuplicates(sriParams, vTypesDuplicates, tableConfig.name)

        //PrevSRI Duplicates - Full Dump
        prevSriVsVtypesDuplicates match {
          case Some(df) =>
            logger.info("Writing Previous SRI vs VTypes Duplicates for " + tableConfig.name)
            Ops.writeDuplicates(sriParams, df, tableConfig.name)
          case None =>
        }

        //ROWCOUNTS
        val earlyCount = vTypesOfTodayIncludingEarly.count() - vTypesOfToday.count()
        val tableRowCount: Seq[RowCount] = Ops.getRowCount(updatedSriParams, tableConfig.name, tableConfig.sourceType, vTypesOfToday, sriOutput, earlyCount, jobUuid)
        tableRowCount.foreach(rowCount => logger.info("Table:" + tableConfig.name + " RowCount Record:" + rowCount))

        val elapsed = (System.currentTimeMillis - start) / 1000L
        logger.info(s"Time taken to process Sri for ${tableConfig.name} is $elapsed seconds")

        sriOpen.unpersist()
        sriNonOpen.unpersist()
        vTypesDuplicates.unpersist()

        tableRowCount.map(rowCount => AuditOutput(Option(rowCount), Ops.convertToProcessMetadataEvents(processMetadataEvents.toList, tableConfig.name, runType, sriParams), businessDateContext.previousBusinessDate))

      }
      auditOutput match {
        case Success(res) =>
          logger.info("Successfully Processed SRI for " + tableConfig.name)
          processMetadataEvents += OpsEvent(new Date().toString, s"Successfully Processed SRI for ${tableConfig.name}")
          Ops.createStatusFile(fs, paramConf, sriParams, "tables", "_COMPLETED_PROCESSING_TABLE_" + tableConfig.name)
          res
        case Failure(ex) =>
          processMetadataEvents += OpsEvent(new Date().toString, s" ERROR processing SRI for ${tableConfig.name}: ${ex.getMessage}")
          logger.error(s" Exception processing ${tableConfig.name} for ${sriParams.toString} " + ex.getMessage, ex)
          Ops.createStatusFile(fs, paramConf, sriParams, "tables", "_ERROR_PROCESSING_TABLE_" + tableConfig.name, ex.getStackTrace.mkString("\n"))
          Seq(AuditOutput(None, Ops.convertToProcessMetadataEvents(processMetadataEvents.toList, tableConfig.name, runType, sriParams), businessDateContext.previousBusinessDate))
      }
    }

    import spark.sqlContext.implicits._
    val auditOutputsL = auditOutputs.toList
    val rowCountsDf = auditOutputsL.flatMap(_.rowCounts).toDF()

    logger.info("Writing RowCounts all tables in one go")
    Ops.writeProcessMetadataEvents(auditOutputsL.flatMap(_.opsEvents), sriParams)
    Ops.writeRowCounts(sriParams, rowCountsDf)

    logger.info("Writing EOD marker to be used for tomorrow")
    //FIXME There must be a better way to get the previous business date other than this
    Ops.writeEodTableAndMarker(paramConf, sriParams, auditOutputs.map(_.businessDate).find(StringUtils.isNotEmpty).getOrElse("BOOTSTRAP"))

  }

  /**
    * Reads Verified Types data as a dataframe normalizing it with the structure of SRI for Union-ing
    *
    * @param fs
    * @param sriParams
    * @param tableConfig
    * @param businessDate
    * @param spark SparkSession
    * @return
    */
  def getVerifyTypes(fs: FileSystem, sriParams: SriParams, tableConfig: TableConfig, businessDate: String)(implicit spark: SparkSession) = {
    val partitionPath = sriParams.getVerifyTypesPartitionPath(tableConfig.name, businessDate)

    val dataFrame = if (fs.exists(new Path(partitionPath))) {
      spark.read.format("com.databricks.spark.avro").load(partitionPath).drop("vds")
    }
    else {
      val tableStructType = fetchVerifyStructure(tableConfig, sriParams.verifyTypesPartitionColumn, sriParams.isCdcSource)
      spark.createDataFrame(spark.sparkContext.emptyRDD[Row], tableStructType).drop("vds")
    }

    //Add Sri audit columns to enable Unions across Previous Sri and VTypes
    dataFrame
      .withColumn("start_date", lit(businessDate))
      .withColumn("start_time", lit(SdmConstants.nowAsString()))
      .withColumn("end_date", lit(SdmConstants.EndOfDays))
      .withColumn("end_time", lit(SdmConstants.EndOfTimes))
      .withColumn("del_flag", lit("0"))
  }

  def getRecordTypeMetaInfo(sriParams: SriParams, tableConfig: TableConfig, vTypesAuditCols: List[String], sriAuditCols: List[String], systemAuditCols: List[String]) = {

    sriParams match {
      case params if sriParams.isCdcSource =>
        RecordTypeMetaInfo(
          timestampColName = sriParams.timestampCol,
          timestampColFormat = sriParams.timestampColFormat,
          operationTypeColName = "c_operationtype",
          insertType = Option(tableConfig.insertValue).getOrElse("I"),
          beforeType = Option(tableConfig.beforeValue).getOrElse("B"),
          afterType = Option(tableConfig.afterValue).getOrElse("A"),
          deleteType = Option(tableConfig.deleteValue).getOrElse("D"),
          vTypesAuditCols, sriAuditCols, systemAuditCols)
      case params if sriParams.isRecordTypeBatch =>
        RecordTypeMetaInfo(
          timestampColName = sriParams.timestampCol,
          timestampColFormat = sriParams.timestampColFormat,
          operationTypeColName = tableConfig.operationTypeCol,
          insertType = tableConfig.insertValue,
          beforeType = tableConfig.beforeValue,
          afterType = tableConfig.afterValue,
          deleteType = tableConfig.deleteValue,
          vTypesAuditCols, sriAuditCols, systemAuditCols)
      //Possible MatchError for Incorrect config
    }
  }
}
