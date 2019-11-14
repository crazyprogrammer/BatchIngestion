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

import com.sc.sdm.models.OpsModels.{ProcessMetadataEvent, RowCount}
import com.sc.sdm.utils.SdmConstants
import com.sc.sdm.utils.SriUDFs._
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.storage.StorageLevel
import org.joda.time.DateTime

/**
  * Base Processor for both RecordType and Non-RecordType processors.
  */
trait SriBaseProcessor {

  import SriBaseProcessor._

  lazy val ignorableColsForDeDuplication = List("rowId", "filename", "vds", "del_flag")

  val rowIdToNumber: (String => Long) = { rowId =>
    StringUtils.substringBefore(rowId, "_").toLong
  }
  val rowIdUDF = udf(rowIdToNumber)

  /**
    * Given a DataFrame fetches the latest for a PrimaryKey (or a group of columns)
    */
  protected def getLatestDataDf(allDataDF: DataFrame, groupByCols: List[String], numberSortKey: String)(implicit spark: SparkSession): DataFrame = {

    //val latestCalculationDF = allDataDF.select((groupByCols ++ List(numberSortKey)).map(col): _*)
    val allDataDFInt = allDataDF.withColumn(AllColString, concat_ws(";", groupByCols.map(col): _*))
    val latestCalculationDF = allDataDFInt.select(AllColString, numberSortKey)

    val latestRecordByPK = latestCalculationDF
      .groupBy(AllColString)
      .max(numberSortKey)
      .withColumnRenamed(s"max($numberSortKey)", numberSortKey)

    val latestRecordByPKCp = spark.createDataFrame(latestRecordByPK.rdd, latestRecordByPK.schema)

    allDataDFInt
      .join(latestRecordByPKCp, List(AllColString, numberSortKey))
      .drop(AllColString)
      .persist(StorageLevel.MEMORY_AND_DISK)
  }

  /**
    * Given a Dataframe and the columns to be considered for de-duplication, this function returns two Dataframes
    * 1. Duplicates (along with the reference to the retained record)
    * 2. De-duplicates
    *
    * There's also an option to retain either the latest or the oldest record while de-deduplicating.
    */
  protected def findDuplicates(inputDf: DataFrame, tableColumns: List[String], auditColList: List[String], ignorableCols: List[String],
                               orderColList: List[String], pickLatest: Boolean)(implicit spark: SparkSession): (DataFrame, DataFrame) = {

    val dataColsWithPk = tableColumns.diff(ignorableCols)

    val dataDF = inputDf.withColumn(RowIdAsNumber, rowIdUDF(col(RowId))).withColumn(AllColString, concat_ws(";", dataColsWithPk.map(col): _*))

    val deDuplicatedDf =
      if (pickLatest) {
        dataDF.groupBy(col(AllColString)).max(RowIdAsNumber).withColumnRenamed("max(rowIdAsNumber)", RowIdAsNumber)
      }
      else {
        dataDF.groupBy(col(AllColString)).min(RowIdAsNumber).withColumnRenamed("min(rowIdAsNumber)", RowIdAsNumber)
      }

    //Fetch deDuplicates
    val deDuplicates = dataDF.join(deDuplicatedDf.select(col(RowIdAsNumber)), RowIdAsNumber).drop(RowIdAsNumber).persist(StorageLevel.MEMORY_AND_DISK)
    val exactDuplicates = dataDF.join(deDuplicates.select(RowId), List(RowId), "leftanti")

    //Avoiding https://issues.apache.org/jira/browse/SPARK-10925
    val exactDuplicatesCopy = spark.createDataFrame(exactDuplicates.rdd, exactDuplicates.schema)
    //Interesting - The duplicates include the original row as well. So, if there are 2 duplicates of 1, then the total number of rows for a PK would be 3 in this DF
    val justRowIdsWithDups = exactDuplicatesCopy
      .join(deDuplicates.withColumnRenamed(RowId, "DUPLICATE_ROWID"), List(AllColString))
      .drop(dataColsWithPk ++ auditColList.toSet.diff(Set(RowId)): _*)

    (deDuplicates.drop(AllColString), justRowIdsWithDups.drop(AllColString))
  }

  implicit class DataframeOps(df: DataFrame) {
    def orderedBy(colList: List[String]): DataFrame = {
      df.select(colList.map(col): _*)
    }
  }

  protected def filterRecordsByColumnValue(recordTypeColName: String)(verifyTypesDf: DataFrame, recordType: String): DataFrame =
    verifyTypesDf.filter(col(recordTypeColName).isin(recordType)) //Need to use it for rowcounts

  implicit class SriOutputOps(sriOutput: SriOutput) {

    /**
      * Enriches a Dataframe with SCD related audit columns
      */
    def withAuditFields(businessDt: DateTime): SriOutput = {

      val businessDateStr = SdmConstants.PartitionFormat.print(businessDt)
      val yesterday = SdmConstants.PartitionFormat.print(businessDt.minusDays(1))

      val sriOpen = sriOutput.sriOpen
        .withColumn("start_date_upd", retainIfNotNullOrSetUdf(col("start_date"), lit(businessDateStr)))
        .withColumn("start_time_upd", retainIfNotNullOrSetUdf(col("start_time"), lit(SdmConstants.nowAsString())))
        .withColumn("end_date_upd", lit(SdmConstants.EndOfDays))
        .withColumn("end_time_upd", lit(SdmConstants.EndOfTimes))
        .drop("start_date", "start_time", "end_date", "end_time")
        .withColumnRenamed("start_date_upd", "start_date")
        .withColumnRenamed("start_time_upd", "start_time")
        .withColumnRenamed("end_date_upd", "end_date")
        .withColumnRenamed("end_time_upd", "end_time")

      val sriNonOpen = sriOutput.sriNonOpen
        .withColumn("start_date_upd", retainIfNotNullOrSetUdf(col("start_date"), lit(businessDateStr)))
        .withColumn("start_time_upd", retainIfNotNullOrSetUdf(col("start_time"), lit(SdmConstants.nowAsString())))
        .withColumn("end_date_upd", setIfPresentOrFallbackToUdf(col("start_date"), lit(yesterday), lit(businessDateStr))) //If today's data, set today. Else, yesterday
        .withColumn("end_time_upd", lit(SdmConstants.nowAsString()))
        .drop("start_date", "start_time", "end_date", "end_time")
        .withColumnRenamed("start_date_upd", "start_date")
        .withColumnRenamed("start_time_upd", "start_time")
        .withColumnRenamed("end_date_upd", "end_date")
        .withColumnRenamed("end_time_upd", "end_time")

      sriOutput.copy(sriOpen = sriOpen, sriNonOpen = sriNonOpen)
    }

    def withOds(businessDt: DateTime): SriOutput = {

      val businessDateStr = SdmConstants.PartitionFormat.print(businessDt)

      val sriOpen = sriOutput.sriOpen.withColumn("ods", lit(businessDateStr))
      val sriNonOpen = sriOutput.sriNonOpen.withColumn("nds", lit(businessDateStr))
      val bRecordsOpt = sriOutput.bRecords.map(_.withColumn("ods", lit(businessDateStr)))
      val vTypesDuplicates = sriOutput.vTypesDuplicates.withColumn("dds", lit(businessDateStr))
      val prevSriVsVtypesDuplicatesOpt = sriOutput.prevSriVsVtypesDuplicates.map(_.withColumn("dds", lit(businessDateStr)))

      sriOutput.copy(sriOpen, sriNonOpen, vTypesDuplicates, prevSriVsVtypesDuplicatesOpt, bRecordsOpt)
    }
  }

}

object SriBaseProcessor {

  val SortKeyAsNumberColName = "sortKeyAsNumberColName"
  val RowIdAsNumber = "rowIdAsNumber"
  val RowId = "rowId"
  val AllColString = "ALL_COL_STRING"

}

/**
  * A sub-set of the Global config that is closely tied with Sri Processing.
  */
sealed trait SriMetaInfo {
  val timestampColName: String
  val timestampColFormat: String
  val vTypesAuditCols: List[String]
  val sriAuditCols: List[String]
  val systemAuditCols: List[String]
}

case class NonRecordTypeMetaInfo(timestampColName: String, timestampColFormat: String, vTypesAuditCols: List[String], sriAuditCols: List[String], systemAuditCols: List[String]) extends SriMetaInfo

case class RecordTypeMetaInfo(timestampColName: String, timestampColFormat: String, operationTypeColName: String, insertType: String, beforeType: String, afterType: String, deleteType: String,
                              vTypesAuditCols: List[String], sriAuditCols: List[String], systemAuditCols: List[String]) extends SriMetaInfo

/**
  * Holder of the result of the SRI processing
  * 1. SRI-OPEN
  * 2. SRI-NON OPEN
  * 3. Duplicates while looking at Verified types
  * 4. Duplicates that were a difference between Previous SRI and today's Verify types (applicable only for Full Dump)
  * 5. B-Records (Applicable only for CDC/other Record Type based systems which has a 'Before' image of the data)
  */
case class SriOutput(sriOpen: DataFrame, sriNonOpen: DataFrame, vTypesDuplicates: DataFrame, prevSriVsVtypesDuplicates: Option[DataFrame] = None, bRecords: Option[DataFrame] = None)

case class AuditOutput(rowCounts: Option[RowCount], opsEvents: List[ProcessMetadataEvent], businessDate: String)
