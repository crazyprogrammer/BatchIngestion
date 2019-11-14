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
import com.sc.sdm.processor.fixtures.{DelimitedTableRowWithRecordType, NonCDCTestFixtures, SdmSharedSparkContext}
import com.sc.sdm.utils.Enums.{RunType, TableType}
import org.joda.time.DateTime
import org.scalacheck.Prop._
import org.scalatest.FunSuite
import org.scalatest.prop.Checkers

/** Tests the IABD (Insert, After, Before, Delete) or equivalent core logic for Record Type based delta tables for Full dump/snapshot loads */
class RecordTypeMasterFullDumpTest extends FunSuite with SdmSharedSparkContext with Checkers with NonCDCTestFixtures {

  val colList = List("recordType", "drReason", "drReasonE", "drReasonC", "drKind", "drDrrKind")
  val januaryFirst = new DateTime(2017, 1, 1, 10, 0)
  val december31st = new DateTime(2016, 12, 31, 10, 0)

  val vTypesAuditCols = List[String]("rowId", "vds", "filename")
  val sriAuditCols = List("start_date", "start_time", "end_date", "end_time", "del_flag")
  val systemAuditCols = List()

  val metaInfo = RecordTypeMetaInfo(timestampColName = "start_date", timestampColFormat = "yyyy-MM-dd HH:mm:ss", operationTypeColName = "recordType",
    insertType = "D", beforeType = "", afterType = "", deleteType = "X", vTypesAuditCols, sriAuditCols, systemAuditCols)

  test("NonCDC RecordType Master FullDump Test") {

    spark.sparkContext.setLogLevel("ERROR")

    val (todaysVerifyTypesListGen, deletablesGen) = DelimitedDataGenerators.createGeneratorWithRecordType(januaryFirst, metaInfo)

    val (previousSriListGen, _) = DelimitedDataGenerators.createGeneratorWithRecordType(december31st, metaInfo)
    check {
      forAllNoShrink(todaysVerifyTypesListGen, previousSriListGen, deletablesGen) { (verifyTypesList, previousSriList, deletables) =>
        getAllAssertions(verifyTypesList, previousSriList, deletables)
      }
    }
  }

  /** Encapsulates all the assertions for this testcase
    *
    * @param verifyTypesList List of DelimitedTableRowWithRecordType that composes Verify types
    * @param previousSriList List of DelimitedTableRowWithRecordType that composes the Previous SRI
    * @param mayBeDeletables List of DelimitedTableRowWithRecordType that ''may be'' a part of ''Delete'' records.  The reason why they are ''may be'' is because the Delete records are generated randomly, there is a possibility that
    *                        there may be ''Delete'' records for a primary key which aren't on PreviousSRI or Verify types
    * @return Consolidated assertion of all properties
    */
  def getAllAssertions(verifyTypesList: List[DelimitedTableRowWithRecordType], previousSriList: List[DelimitedTableRowWithRecordType], mayBeDeletables: List[DelimitedTableRowWithRecordType]) = {

    implicit val sqlContext = spark.sqlContext

    //FIXME Figure out a way to add delete to a product in the middle of the record set instead of adding D records at the end of Verify Types
    val singleInstancePerKeyList = previousSriList
      .groupBy(_.drReason) //have only one instance per primarykey
      .map(_._2.head)
      .toList

    val previousSriSingleInstancePerKey = if (singleInstancePerKeyList.nonEmpty) singleInstancePerKeyList ++ List(singleInstancePerKeyList.last.copy(rowId = s"${System.nanoTime()}_${UUID.randomUUID().toString}", drReason = "DELREC", start_date = dateFormat.print(december31st))) else List()

    //Simulating a previous SRI
    val verifyTypes = singleInstancePerKeyList.map(each => each.copy(rowId = s"${System.nanoTime()}_${UUID.randomUUID().toString}", start_date = dateFormat.print(januaryFirst)))

    val allData = previousSriSingleInstancePerKey.union(verifyTypes)
    val deletedPkList = previousSriSingleInstancePerKey.filterNot(row => verifyTypes.map(_.drReason).contains(row.drReason))
    val deletedPkRowsList = previousSriSingleInstancePerKey.filter(row => deletedPkList.map(_.drReason).contains(row.drReason))
    val sriOpenDataList = allData.filterNot(row => deletedPkRowsList.map(_.drReason).contains(row.drReason)).groupBy(_.drReason).map(_._2.head).toList

    //logDatasetForDebugging(verifyTypes, previousSriSingleInstancePerKey)

    import sqlContext.implicits._

    val todaysVerifyTypesDF = spark.sparkContext.parallelize(verifyTypes).toDF
    val previousSriDF = spark.sparkContext.parallelize(previousSriSingleInstancePerKey).toDF

    println("###################################### TODAY'S VERIFY TYPES ######################################")
    todaysVerifyTypesDF.show(100, false)
    println("######################################     PREVIOUS SRI     ######################################")
    previousSriDF.show(false)
    val tableName = "DUMMY_TABLENAME"

    val processor = new RecordTypeBasedProcessor

    //Ref : Note that runType - fulldump/incremental doesnt make sense for transactional assertions
    val SriOutput(sriOpen, sriNonOpen, duplicatesOpt, prevSriVsVtypesDuplicates, bRecordsOpt) =
      processor.processSri(tableName, colList, List("drReason"), todaysVerifyTypesDF, previousSriDF, metaInfo, TableType.DELTA, RunType.FULLDUMP, januaryFirst)(spark)


    println("######################################       SRI OPEN       ######################################")
    sriOpen.show(false)
    println("######################################     SRI NON OPEN     ######################################")
    sriNonOpen.show(false)

    val sriOpenRowIds = sriOpen.select("rowId").collect().map(row => row.get(0).toString).toSet
    val sriNonOpenRowIds = sriNonOpen.select("rowId").collect().map(row => row.get(0).toString).toSet
    val duplicatesRowIds = prevSriVsVtypesDuplicates match {
      case Some(df) =>
        println("###################################### DUPLICATE ######################################")
        df.show(false)
        df.select("rowId").collect().map(row => row.get(0).toString).toSet
      case None => Set[String]()
    }

    //val updatedVerifyTypesSet = verifyTypes.filterNot(_.recordType == "B").toSet
    val updatedVerifyTypesSet = verifyTypes.toSet

    val oldestPerKey = allData.groupBy(_.drReason).map(_._2.head).toSet

    //SRI OPEN = latestInVerifyTypes + PrevSRI data which is not updated through verify types - D Records
    val sriOpenCountCheck = sriOpen.count() === sriOpenDataList.size

    //SRI NON_OPEN = prevSRI Data which is updated through today's VTypes + Changes during the day in Vtypes + D records - bRecords
    val sriNonOpenCountCheck = sriNonOpen.count() === deletedPkRowsList.size

    //Actual record checks
    val sriOpenRowIdCheck = sriOpenRowIds === sriOpenDataList.map(_.rowId).toSet

    val sriNonOpenRowIdCheck = sriNonOpenRowIds === deletedPkRowsList.map(_.rowId).toSet

    val prevSriVsVtypesDuplicateCheck = prevSriVsVtypesDuplicates.map(_.count()).getOrElse(0) === duplicatesRowIds.size

    println(s"sriOpenCountCheck: $sriOpenCountCheck   -> ${sriOpen.count()} === ${sriOpenDataList.size}")
    println(s"sriNonOpenCountCheck: $sriNonOpenCountCheck   -> ${sriNonOpen.count()} === ${deletedPkRowsList.size}")
    println(s"sriOpenRowIdCheck: $sriOpenRowIdCheck")
    println(s"sriNonOpenRowIdCheck: $sriNonOpenRowIdCheck")
    println(s"Duplicate check :  $prevSriVsVtypesDuplicateCheck -> ${prevSriVsVtypesDuplicates.map(_.count()).getOrElse(0)} === ${duplicatesRowIds.size}")

    sriOpenCountCheck && sriNonOpenCountCheck && sriOpenRowIdCheck && sriNonOpenRowIdCheck && prevSriVsVtypesDuplicateCheck
  }

  def logDatasetForDebugging(updatedVerifyTypesList: List[DelimitedTableRowWithRecordType], previousSriList: List[DelimitedTableRowWithRecordType]) = {
    println("###################################### VERIFY TYPES (DEBUG) ######################################")
    updatedVerifyTypesList.foreach(printDelimitedTableRow)
    println()
    println("###################################### PREVIOUS SRI (DEBUG) ######################################")
    previousSriList.foreach(printDelimitedTableRow)
    println()
  }

  def printDelimitedTableRow(each: DelimitedTableRowWithRecordType) = {
    println(
      s"""DelimitedTableRowWithRecordType("${each.rowId}", "${each.filename}", "${each.recordType}", "${each.drReason}", "${each.drReasonE}", "${each.drReasonC}", "${each.drKind}", "${each.drDrrKind}", "${each.start_date}", "${each.start_time}", "${each.end_date}", "${each.end_time}", "${each.del_flag}"),""")
  }
}