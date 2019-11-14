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

package com.sc.sdm.processor.prop

import java.util.UUID

import com.sc.sdm.processor._
import com.sc.sdm.processor.fixtures.{DelimitedTableRow, NonCDCTestFixtures, SdmSharedSparkContext}
import com.sc.sdm.utils.Enums.{RunType, TableType}
import org.joda.time.DateTime
import org.scalacheck.Prop._
import org.scalatest.FunSuite
import org.scalatest.prop.Checkers

/** Tests the Core logic for Non-Record Type/Non-CDC based master/delta tables for Incremental/Daily loads */
class NonRecordTypeMasterIncrementalTest extends FunSuite with SdmSharedSparkContext with Checkers with NonCDCTestFixtures {

  val dataCols = List("drReason", "drReasonE", "drReasonC", "drKind", "drDrrKind")
  val sriAuditCols = List("start_date", "start_time", "end_date", "end_time", "del_flag")
  val vTypesAuditCols = List[String]("rowId", "vds", "filename")
  val metaInfo = NonRecordTypeMetaInfo(timestampColName = "start_time", timestampColFormat = "yyyy-MM-dd HH:mm:ss", vTypesAuditCols, sriAuditCols, List[String]())

  val januaryFirst = new DateTime(2017, 1, 1, 10, 0)
  val december31st = new DateTime(2016, 12, 31, 10, 0)

  test("NonCDC Master Incremental Test") {

    spark.sparkContext.setLogLevel("ERROR")

    val todaysVerifyTypesListGen = DelimitedDataGenerators.createGeneratorWithoutRecordType(januaryFirst)

    val previousSriListGen = DelimitedDataGenerators.createGeneratorWithoutRecordType(december31st)
    check {
      forAllNoShrink(todaysVerifyTypesListGen, previousSriListGen) { (verifyTypesList, previousSriList) =>
        getAllAssertions(verifyTypesList, previousSriList)
      }
    }
  }

  /** Encapsulates all the assertions for this testcase
    *
    * @param verifyTypesList List of DelimitedTableRowWithRecordType that composes Verify types
    * @param previousSriList List of DelimitedTableRowWithRecordType that composes the Previous SRI
    * @return Consolidated assertion of all properties
    */
  def getAllAssertions(verifyTypesList: List[DelimitedTableRow], previousSriList: List[DelimitedTableRow]) = {

    implicit val sqlContext = spark.sqlContext

    val previousSriSingleInstancePerKey = previousSriList
      .groupBy(_.drReason) //have only one instance per primarykey
      .map(_._2.head)
      .toList

    val selfIntroducedDuplicates = if (verifyTypesList.nonEmpty) List(verifyTypesList.last.copy(rowId = s"${System.nanoTime()}_${UUID.randomUUID().toString}")) else List()

    val verifyTypesConsolidated = verifyTypesList ++ selfIntroducedDuplicates
    //logDatasetForDebugging(verifyTypesConsolidated, previousSriSingleInstancePerKey)

    import sqlContext.implicits._

    val todaysVerifyTypesDF = spark.sparkContext.parallelize(verifyTypesConsolidated).toDF
    val previousSriDF = spark.sparkContext.parallelize(previousSriSingleInstancePerKey).toDF.drop("vds") //.withColumn("ods", lit(dec31stStr))

    println("###################################### TODAY'S VERIFY TYPES ######################################")
    todaysVerifyTypesDF.show(100, false)
    println("######################################     PREVIOUS SRI     ######################################")
    previousSriDF.show(false)

    //TODO Duplicated across Testcase and assertions
    val januaryFirst = new DateTime(2017, 1, 1, 10, 0)
    val tableName = "DUMMY_TABLENAME"

    val processor = new NonRecordTypeBasedProcessor

    //Ref : Note that runType - fulldump/incremental doesnt make sense for transactional assertions
    val SriOutput(sriOpen, sriNonOpen, vTypesDuplicates, previousSriVsVTypes, bRecordsOpt) = processor.processSri(tableName, dataCols, List("drReason"), todaysVerifyTypesDF,
      previousSriDF, metaInfo, TableType.DELTA, RunType.INCREMENTAL, januaryFirst)(spark)

    println("######################################       SRI OPEN       ######################################")
    sriOpen.show(false)
    println("######################################     SRI NON OPEN     ######################################")
    sriNonOpen.show(false)
    println("######################################       DUPLICATES       ######################################")
    vTypesDuplicates.show(false)

    val sriOpenRowIds = sriOpen.select("rowId").collect().map(row => row.get(0).toString).toSet
    val sriNonOpenRowIds = sriNonOpen.select("rowId").collect().map(row => row.get(0).toString).toSet

    val duplicatesRowIds = vTypesDuplicates.select("rowId").collect().map(row => row.get(0).toString).toSet
    val previousSriVsVTypesRowIds = previousSriVsVTypes.map(df => df.select("rowId").collect().map(row => row.get(0).toString)).getOrElse(Array()).toSet

    //val updatedVerifyTypesSet = verifyTypesConsolidated.filterNot(_.recordType == "B").toSet
    val updatedVerifyTypesSet = verifyTypesConsolidated.toSet

    //val latestPerKeyInVTypes = verifyTypesConsolidated.groupBy(_.drReason).map(_._2.last).toSet
    val latestPerKeyInVTypes = verifyTypesConsolidated
      .groupBy(row => (row.drReason, row.drReasonE, row.drReasonC, row.drKind, row.drDrrKind))
      .map { case (_, listRow) => listRow.last }
      .groupBy(_.drReason)
      .mapValues(groupList => groupList.maxBy(delimRow => delimRow.rowId))
      .values.toSet


    val oldUpdatesDuringTheDay = updatedVerifyTypesSet.diff(latestPerKeyInVTypes)
    //Has to go into SRI NON_OPEN -  Updates for I records (+ Old As for the day)
    val prevSriOpenProductsInVTypes = previousSriSingleInstancePerKey.filter(row => updatedVerifyTypesSet.map(_.drReason).contains(row.drReason)).toSet
    //Has to stay in SRI OPEN - Previous Sri open for which no update has been received (+ Latest update on Verify types)
    val prevSriOpenProductsNotInVTypes = previousSriSingleInstancePerKey.filterNot(row => updatedVerifyTypesSet.map(_.drReason).contains(row.drReason)).toSet

    //SRI OPEN = latestInVerifyTypes + PrevSRI data which is not updated through verify types - D Records
    val sriOpenCountCheck = sriOpen.count() === (latestPerKeyInVTypes.size + prevSriOpenProductsNotInVTypes.size)

    //SRI NON_OPEN = prevSRI Data which is updated through today's VTypes + Changes during the day in Vtypes + D records - bRecords
    val sriNonOpenCountCheck = sriNonOpen.count() === prevSriOpenProductsInVTypes.size + oldUpdatesDuringTheDay.size - duplicatesRowIds.size - previousSriVsVTypesRowIds.size


    //Actual record checks
    val sriOpenRowIdCheck = sriOpenRowIds ===
      (prevSriOpenProductsNotInVTypes.map(_.rowId) ++ latestPerKeyInVTypes.map(_.rowId))

    val sriNonOpenRowIdCheck = sriNonOpenRowIds === (prevSriOpenProductsInVTypes.map(_.rowId) ++ oldUpdatesDuringTheDay.map(_.rowId)).diff(duplicatesRowIds ++ previousSriVsVTypesRowIds)

    println(s"sriOpenCountCheck: $sriOpenCountCheck   -> ${sriOpen.count()} === ${latestPerKeyInVTypes.size} + ${prevSriOpenProductsNotInVTypes.size}")
    println(s"sriNonOpenCountCheck: $sriNonOpenCountCheck   -> ${sriNonOpen.count()} === ${prevSriOpenProductsInVTypes.size} + ${oldUpdatesDuringTheDay.size} - ${duplicatesRowIds.size} - ${previousSriVsVTypesRowIds.size}")
    println(s"sriOpenRowIdCheck: $sriOpenRowIdCheck")
    println(s"sriNonOpenRowIdCheck: $sriNonOpenRowIdCheck")

    sriOpenCountCheck && sriNonOpenCountCheck && sriOpenRowIdCheck && sriNonOpenRowIdCheck
  }
}