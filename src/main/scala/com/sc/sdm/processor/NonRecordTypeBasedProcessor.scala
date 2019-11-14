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

import com.sc.sdm.utils.Enums.RunType.RunType
import com.sc.sdm.utils.Enums.TableType.TableType
import com.sc.sdm.utils.Enums.{RunType, TableType}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.storage.StorageLevel
import org.joda.time.DateTime

/**
  * Holds implementation methods for handling Non-CDC Flatfile processing
  * The primary functions are :
  * 1. processSriForIncremental
  * 2. processSriForFullDump
  */
class NonRecordTypeBasedProcessor extends SriBaseProcessor {

  import SriBaseProcessor._

  /**
    * Entry point for processing Source Image for Non-CDC and Non-RecordType based filesets
    */
  def processSri(tableName: String, dataCols: List[String], primaryKeyCols: List[String], verifyTypesDf: DataFrame, previousSriDf: DataFrame, metaInfo: NonRecordTypeMetaInfo,
                 tableType: TableType, runType: RunType, businessDate: DateTime)(implicit spark: SparkSession): SriOutput = {
    runType match {
      case RunType.INCREMENTAL => processSriForIncremental(tableName, dataCols, primaryKeyCols, metaInfo.timestampColName, verifyTypesDf, previousSriDf, metaInfo, tableType, businessDate)
      case RunType.FULLDUMP => processSriForFullDump(tableName, dataCols, primaryKeyCols, metaInfo.timestampColName, verifyTypesDf, previousSriDf, metaInfo, tableType, businessDate)
    }
  }

  /**
    * Function that handles data that is a delta of the previous day.  This new data is
    * 1. Filtered for duplicates &
    * 2. Analyzed for primary key changes
    *
    * All intermediary transitions of data for a primary key is extracted out as history data (SRI-NON-OPEN).
    * All transitions of data for a primary key which is deleted will also move into SRI-NON-OPEN
    * B-Records (for CDC) is ignored but the count logged.
    * Only the latest record will get into SRI-OPEN
    */
  private def processSriForIncremental(tableName: String, dataCols: List[String], primaryKeyCols: List[String], sortKey: String, verifyTypesDfR: DataFrame, previousSriDfR: DataFrame, metaInfo: NonRecordTypeMetaInfo,
                                       tableType: TableType, businessDate: DateTime)(implicit spark: SparkSession): SriOutput = {

    import metaInfo._

    val auditColList = vTypesAuditCols.diff(List("vds", "filename")) ++ sriAuditCols ++ systemAuditCols
    val orderColList = auditColList ++ dataCols
    val groupByCols = if (primaryKeyCols.isEmpty) dataCols else primaryKeyCols

    val verifyTypesDf = verifyTypesDfR.orderedBy(orderColList).persist(StorageLevel.MEMORY_AND_DISK)
    val previousSriDf = previousSriDfR.orderedBy(orderColList).persist(StorageLevel.MEMORY_AND_DISK)

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
      val dateParser: (String => Long) = { sDate =>
        val formatter = new SimpleDateFormat(metaInfo.timestampColFormat)
        formatter.parse(sDate).getTime
      }

      val dateParserUDF = udf(dateParser)

      val (allDataDF, vTypeDups) = tableType match {
        case TableType.DELTA =>
          val (deDupVTypesDf, vTypeDupsInt) = findDuplicates(verifyTypesDf, dataCols, auditColList, ignorableColsForDeDuplication, orderColList, pickLatest = true)
          (previousSriDf.union(deDupVTypesDf), vTypeDupsInt)
        case TableType.TXN =>
          val (deDupVTypesDf, vTypeDupsInt) = findDuplicates(verifyTypesDf, dataCols, auditColList, ignorableColsForDeDuplication, orderColList, pickLatest = true)
          (deDupVTypesDf, vTypeDupsInt)
      }

      val (deDuplicates, sriVsVTypeDups) = findDuplicates(allDataDF, dataCols, auditColList, ignorableColsForDeDuplication, orderColList, pickLatest = false)

      val allDataWithTSNumber = deDuplicates.withColumn(SortKeyAsNumberColName, dateParserUDF(col(sortKey)))

      //SRI_OPEN
      val sortKeyLatestDf = getLatestDataDf(allDataWithTSNumber, groupByCols, SortKeyAsNumberColName)
        .drop(col(SortKeyAsNumberColName))
        .orderedBy(orderColList)

      val sriOpen = getLatestDataDf(sortKeyLatestDf.withColumn(RowIdAsNumber, rowIdUDF(col(RowId))), groupByCols, RowIdAsNumber).drop(col(RowIdAsNumber))

      //SRI_NONOPEN - except sri open and duplicates
      val sriNonOpen = allDataDF.join(
        sriOpen.select(col(RowId))
          .union(vTypeDups.select(col(RowId)))
          .union(sriVsVTypeDups.select(col(RowId)))
        , List(RowId), "leftanti")

      val sriOutputWithAuditFields = SriOutput(sriOpen, sriNonOpen, vTypeDups, Option(sriVsVTypeDups), None).withAuditFields(businessDate)

      SriOutput(
        sriOutputWithAuditFields.sriOpen.orderedBy(orderColList),
        sriOutputWithAuditFields.sriNonOpen.orderedBy(orderColList),
        sriOutputWithAuditFields.vTypesDuplicates.cache(),
        sriOutputWithAuditFields.prevSriVsVtypesDuplicates.map(_.cache()),
        None
      )
    }
  }


  /**
    * For cases when the source system sends the entire snapshot of a table as a "full dump", this function loads into the SRI-OPEN and
    * SRI-NON-OPEN.
    *
    * About 90% of the records would be duplicates of the existing SRI-OPEN. These "duplicates" are extracted and logged. The original record
    * with the earlier "start_date" and "start_time" are retained to maintain history
    */
  private def processSriForFullDump(tableName: String, dataCols: List[String], primaryKeyCols: List[String], sortKey: String, verifyTypesDfR: DataFrame, previousSriDfR: DataFrame,
                                    metaInfo: NonRecordTypeMetaInfo, tableType: TableType, businessDate: DateTime)(implicit spark: SparkSession): SriOutput = {

    import metaInfo._

    val auditColList = vTypesAuditCols.diff(List("vds", "filename")) ++ sriAuditCols ++ systemAuditCols
    val orderColList = auditColList ++ dataCols
    val groupByCols = if (primaryKeyCols.isEmpty) dataCols else primaryKeyCols

    val verifyTypesDf = verifyTypesDfR.orderedBy(orderColList)
    val previousSriDf = previousSriDfR.orderedBy(orderColList)

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

      val sriNonOpen = previousSriDf.join(broadcast(deletedPkRowIds), List(RowId))

      val (deDuplicates, sriVsVTypeDups) = findDuplicates(allDataDF, dataCols, auditColList, ignorableColsForDeDuplication, orderColList, pickLatest = false)

      val sriOpen = deDuplicates.join(broadcast(deletedPkRowIds), List(RowId), "leftanti")

      val sriOutputWithAuditFields = SriOutput(sriOpen, sriNonOpen, vTypeDups, Some(sriVsVTypeDups), None).withAuditFields(businessDate)

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