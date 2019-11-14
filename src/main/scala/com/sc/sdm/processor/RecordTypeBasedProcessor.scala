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

import java.text.SimpleDateFormat

import com.sc.sdm.processor.SriBaseProcessor._
import com.sc.sdm.utils.Enums.RunType.RunType
import com.sc.sdm.utils.Enums.TableType.TableType
import com.sc.sdm.utils.Enums.{RunType, TableType}
import com.sc.sdm.utils.SriUDFs.getDeleteFlagUdf
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.storage.StorageLevel
import org.joda.time.DateTime

/**
  * Holds implementation methods for handling CDC and RecordType/OperationType based Flatfile processing
  * The primary functions are :
  * 1. processSriForIncremental
  * 2. processSriForFullDump
  */
class RecordTypeBasedProcessor extends SriBaseProcessor {

  /**
    * Entry point for processing Source Image for CDC and RecordType based filesets
    */
  def processSri(tableName: String, dataCols: List[String], primaryKeyCols: List[String], verifyTypesDf: DataFrame, previousSriDf: DataFrame,
                 recordTypeMeta: RecordTypeMetaInfo, tableType: TableType, runType: RunType, businessDate: DateTime)(implicit spark: SparkSession): SriOutput = {

    runType match {
      case RunType.INCREMENTAL => processSriForIncremental(tableName, dataCols, primaryKeyCols, verifyTypesDf, previousSriDf, recordTypeMeta, tableType, businessDate)(spark)
      case RunType.FULLDUMP => processSriForFullDump(tableName, dataCols, primaryKeyCols, verifyTypesDf, previousSriDf, recordTypeMeta, tableType, businessDate)(spark)
    }
  }

  /**
    * Function that handles data that is a delta of the previous day.  This new data is
    * 1. Filtered for duplicates &
    * 2. Analyzed for primary key changes
    *
    * All transitions of data for a primary key which is deleted will also move into SRI-NON-OPEN
    * Only the latest record will get into SRI-OPEN
    */
  private def processSriForIncremental(tableName: String, dataCols: List[String], primaryKeyCols: List[String], verifyTypesDfR: DataFrame, previousSriDfR: DataFrame,
                                       recordTypeMeta: RecordTypeMetaInfo, tableType: TableType, businessDate: DateTime)(implicit spark: SparkSession): SriOutput = {

    import recordTypeMeta._


    val sriApplicableVTypesAuditCols = vTypesAuditCols.diff(List("vds", "filename"))
    val auditColList = sriApplicableVTypesAuditCols ++ sriAuditCols ++ systemAuditCols
    val orderColList = auditColList ++ dataCols

    val verifyTypesDf = verifyTypesDfR.orderedBy(orderColList).persist(StorageLevel.MEMORY_AND_DISK)
    val previousSriDf = previousSriDfR.orderedBy(orderColList).persist(StorageLevel.MEMORY_AND_DISK)

    //If there's no data today, just carry over the previousSri as SriOpen
    if (verifyTypesDf.count() == 0 && previousSriDf.count() != 0) {

      val emptySriRdd = spark.createDataFrame(spark.sparkContext.emptyRDD[Row], previousSriDf.schema)
      val emptyDuplicateRdd = spark.createDataFrame(spark.sparkContext.emptyRDD[Row], StructType(Seq(StructField("rowid", StringType), StructField("duplicate_rowid", StringType))))

      SriOutput(
        previousSriDfR,
        emptySriRdd,
        emptyDuplicateRdd,
        None,
        None)
    }
    else {
      val primarySortKey = recordTypeMeta.timestampColName

      val dateParser: (String => Long) = { sDate =>
        val formatter = new SimpleDateFormat(recordTypeMeta.timestampColFormat)
        formatter.parse(sDate).getTime
      }
      val dateParserUDF = udf(dateParser)

      val getRecordsOfOperationType = filterRecordsByColumnValue(recordTypeMeta.operationTypeColName) _ //either the operationtype column of CDC that contains I,A,B,D as values or D/X for Delimited

      //bRecords - Will be used to filter out from Open
      val bRecords = getRecordsOfOperationType(verifyTypesDf, beforeType)
      val bRecordRowIds = bRecords.select(col(RowId))

      val (deDupVTypesDf, vTypeDups) = findDuplicates(verifyTypesDf, orderColList, auditColList, ignorableColsForDeDuplication, orderColList, pickLatest = false)

      val allDataDF = tableType match {
        case TableType.DELTA => previousSriDf.union(deDupVTypesDf)
        case TableType.TXN => deDupVTypesDf
      }

      val (sriOpen, sriNonOpen) = tableType match {
        case TableType.TXN if primaryKeyCols.isEmpty =>

          val sriOpen = allDataDF.filter(col(operationTypeColName).isin(afterType, insertType, deleteType))
          val sriNonOpen = spark.createDataFrame(spark.sparkContext.emptyRDD[Row], sriOpen.schema)
          (sriOpen, sriNonOpen)

        case _ =>
          val allDataWithTSNumber = allDataDF.withColumn(SortKeyAsNumberColName, dateParserUDF(col(primarySortKey)))

          val sortKeyLatestDf =
            getLatestDataDf(allDataWithTSNumber, primaryKeyCols, SortKeyAsNumberColName)
              .drop(col(SortKeyAsNumberColName))
              .orderedBy(orderColList)

          val latestDataDF = getLatestDataDf(sortKeyLatestDf.withColumn(RowIdAsNumber, rowIdUDF(col(RowId))), primaryKeyCols, RowIdAsNumber).drop(col(RowIdAsNumber))

          val sriOpen = latestDataDF.filter(col(operationTypeColName).isin(afterType, insertType))

          //PKV Validation
          val (updatedAllDataDF, pkRecordsChangedToD) = updateLatestBeforeToDeleteRecords(allDataDF, latestDataDF, recordTypeMeta, orderColList)

          val excludableRowIds = bRecordRowIds.join(pkRecordsChangedToD, List(RowId), "leftanti").union (vTypeDups.select(RowId))

          //SRI_OPEN
          val sriNonOpen = updatedAllDataDF.
            join(sriOpen.select(col(RowId))
              .union(excludableRowIds), List(RowId), "leftanti")

          (sriOpen, sriNonOpen)

      }

      val bRecordsOpt = Option(bRecords) //if (bRecords.count() > 0) Some(bRecords) else None

      val sriOutputWithAuditFields = SriOutput(sriOpen, sriNonOpen, vTypeDups, None, bRecordsOpt).withAuditFields(businessDate)

      SriOutput(
        sriOutputWithAuditFields.sriOpen.orderedBy(orderColList),
        sriOutputWithAuditFields.sriNonOpen.orderedBy(orderColList),
        sriOutputWithAuditFields.vTypesDuplicates.persist(StorageLevel.MEMORY_AND_DISK),
        None,
        bRecordsOpt.map(_.orderedBy(orderColList))
      )
    }

  }


  /**
    * If for a primary key, the last record is a 'B', then it means that the primary key for that record has changed.
    * Utility function that converts such a record from B to D.  This record will eventually move into SRI-NON-OPEN
    */
  private def updateLatestBeforeToDeleteRecords(allDataDF: DataFrame, latestDataDF: DataFrame, recordTypeMeta: RecordTypeMetaInfo, orderColList: List[String]) = {
    import recordTypeMeta._
    val primaryKeyChangedRecords = latestDataDF.filter(col(operationTypeColName).isin(beforeType)) //last record is a B
    val primaryKeyChangedRecordsConvertedToD =
      primaryKeyChangedRecords
        .drop(operationTypeColName)
        .withColumn(operationTypeColName, lit(deleteType))
        .orderedBy(orderColList)

    val updatedAllDataWithD = allDataDF
      .join(broadcast(primaryKeyChangedRecordsConvertedToD.select(col(RowId))), List(RowId), "leftanti")
      .union(primaryKeyChangedRecordsConvertedToD)
      .withColumn("del_flag_upd", getDeleteFlagUdf(col("del_flag"), lit(recordTypeMeta.deleteType)))
      .drop("del_flag")
      .withColumnRenamed("del_flag_upd", "del_flag")

    (updatedAllDataWithD, primaryKeyChangedRecordsConvertedToD)
  }


  /**
    * For cases when the source system sends the entire snapshot of a table as a "full dump", this function loads into the SRI-OPEN and
    * SRI-NON-OPEN.
    *
    * About 90% of the records would be duplicates of the existing SRI-OPEN. These "duplicates" are extracted and logged. The original record
    * with the earlier "start_date" and "start_time" are retained to maintain history
    */
  private def processSriForFullDump(tableName: String, dataCols: List[String], primaryKeyCols: List[String], verifyTypesDfR: DataFrame, previousSriDfR: DataFrame, recordTypeMeta: RecordTypeMetaInfo,
                                    tableType: TableType, businessDate: DateTime)(implicit spark: SparkSession): SriOutput = {

    import recordTypeMeta._

    val auditColList = vTypesAuditCols.diff(List("vds", "filename")) ++ sriAuditCols ++ systemAuditCols
    val orderColList = auditColList ++ dataCols
    val groupByCols = if (primaryKeyCols.isEmpty) dataCols else primaryKeyCols

    val verifyTypesDf = verifyTypesDfR.orderedBy(orderColList).persist(StorageLevel.MEMORY_AND_DISK)
    val previousSriDf = previousSriDfR.orderedBy(orderColList).persist(StorageLevel.MEMORY_AND_DISK)

    if (verifyTypesDf.count() == 0 && previousSriDf.count() != 0) {

      val emptySriRdd = spark.createDataFrame(spark.sparkContext.emptyRDD[Row], previousSriDf.schema)
      val emptyDuplicateRdd = spark.createDataFrame(spark.sparkContext.emptyRDD[Row], StructType(Seq(StructField("rowid", StringType), StructField("duplicate_rowid", StringType))))

      tableType match {
        case TableType.DELTA =>
          SriOutput(
            previousSriDfR,
            emptySriRdd,
            emptyDuplicateRdd,
            None,
            None)
        case TableType.TXN =>
          SriOutput(
            emptySriRdd,
            emptySriRdd,
            emptyDuplicateRdd,
            None,
            None)
      }
    }
    else {

      val (deDupVTypesDf, vTypeDups) = findDuplicates(verifyTypesDf, dataCols, auditColList, ignorableColsForDeDuplication, orderColList, pickLatest = false)

      val allDataDF = tableType match {
        case TableType.DELTA => previousSriDf.union(deDupVTypesDf)
        case TableType.TXN => deDupVTypesDf
      }

      val deletedPkRowIds = previousSriDf
        .select((List(RowId) ++ groupByCols).map(col): _*)
        .join(verifyTypesDf.select(groupByCols.map(col): _*), groupByCols, "leftanti")
        .select(RowId)
        .persist(StorageLevel.MEMORY_AND_DISK)


      val sriNonOpen = previousSriDf.join(deletedPkRowIds, List(RowId))

      val (deDuplicates, sriVsVTypeDups) = findDuplicates(allDataDF, dataCols, auditColList, ignorableColsForDeDuplication, orderColList, pickLatest = false)

      val sriOpen = deDuplicates.join(deletedPkRowIds, List(RowId), "leftanti")

      val sriOutputWithAuditFields = SriOutput(sriOpen, sriNonOpen, vTypeDups, Option(sriVsVTypeDups), None).withAuditFields(businessDate)

      SriOutput(
        sriOutputWithAuditFields.sriOpen.orderedBy(orderColList),
        sriOutputWithAuditFields.sriNonOpen.orderedBy(orderColList),
        sriOutputWithAuditFields.vTypesDuplicates,
        sriOutputWithAuditFields.prevSriVsVtypesDuplicates,
        None
      )
    }
  }
}
