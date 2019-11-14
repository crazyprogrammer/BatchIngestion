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

import com.sc.sdm.processor.fixtures.{CDCDebugLogger, CDCTableRow, CDCTestFixtures, SdmSharedSparkContext}
import com.sc.sdm.processor.{RecordTypeBasedProcessor, RecordTypeMetaInfo, SriOutput}
import com.sc.sdm.utils.Enums.{RunType, TableType}
import org.joda.time.{DateTime, Period}
import org.scalacheck.Prop._
import org.scalatest.FunSuite
import org.scalatest.prop.Checkers

/** Tests the IABD (Insert, After, Before, Delete) core logic for CDC based master tables */
class CDCMasterIncrementalIABDTest extends FunSuite with SdmSharedSparkContext with Checkers with CDCTestFixtures with CDCDebugLogger {

  val dataCols = List("productId", "currencyVal", "description", "price")
  val vTypesAuditCols = List("rowId", "filename", "vds")
  val sriAuditCols = List("start_date", "start_time", "end_date", "end_time", "del_flag")
  val systemAuditCols = List("c_journaltime", "c_transactionid", "c_operationtype", "c_userid")
  val januaryFirst = new DateTime(2017, 1, 1, 10, 0)
  val december31st = new DateTime(2016, 12, 31, 10, 0)

  override val recordTypeMeta = RecordTypeMetaInfo(timestampColName = "c_journaltime", timestampColFormat = "yyyy-MM-dd HH:mm:ss.SSSSSS",
    operationTypeColName = "c_operationtype", insertType = "I", beforeType = "B", afterType = "A", deleteType = "D", vTypesAuditCols, sriAuditCols, systemAuditCols)

  test("CDC Master Incremental IABD") {

    spark.sparkContext.setLogLevel("ERROR")

    val range = Period.hours(5)
    val operationTypesForVTypes = List("B") //List("B") would mean (B,A) pairs
    val operationTypesForPrevSri = List("I") //Only I are available in the previous SRI

    val productIds = List("PRODUCT1", "PRODUCT2", "PRODUCT3", "PRODUCT4", "PRODUCT5")

    val (todaysVerifyTypesListGen, deletablesGen, pkChangeGen) = CDCDataGenerators.createGenerator(januaryFirst, range, operationTypesForVTypes, productIds)

    val (previousSriListGen, _, _) = CDCDataGenerators.createGenerator(december31st, range, operationTypesForPrevSri, productIds)

    check {
      forAllNoShrink(todaysVerifyTypesListGen, previousSriListGen, deletablesGen, pkChangeGen) { (todaysVerifyTypesList, previousSriList, deletables, pkChange) =>
        getAllAssertions(dataCols, List("productId"), todaysVerifyTypesList, previousSriList, deletables, pkChange)
      }
    }
  }

  /** Encapsulates all the assertions for this testcase
    *
    * @param dataCols        All business columns for this table
    * @param pkCols          Primary Key columns for this table
    * @param verifyTypesList List of CDCTableRow that composes Verify types
    * @param previousSriList List of CDCTableRow that composes the Previous SRI
    * @param mayBeDeletables List of CDCTableRow that ''may be'' a part of ''D'' records.  The reason why they are ''may be'' is because the Delete records are generated randomly, there is a possibility that
    *                        there may be ''D'' records for a primary key which aren't on PreviousSRI or Verify types
    * @param mayBePkChanges  List of CDCTableRow that ''may be'' a part of records whose primary key has changed aka Records whose last entry is a 'B'.
    *                        Again, the reason why they are may be is because the PK changed records are generated randomly, there is a possibility that
    *                        there may be records for a primary key which aren't on PreviousSRI or Verify types.  Also, a check is made so that the PKV records aren't part of the
    *                        ''D'' records as well
    * @return Consolidated assertion of all properties
    */
  def getAllAssertions(dataCols: List[String], pkCols: List[String], verifyTypesList: List[CDCTableRow], previousSriList: List[CDCTableRow], mayBeDeletables: List[CDCTableRow], mayBePkChanges: List[CDCTableRow]) = {

    implicit val sqlContext = spark.sqlContext

    //select just one D record for a single product which is available in the verifyTypes/previousSri. Isolated D for a product not in VType/Prev SRI doesn't make sense
    val dRecords = mayBeDeletables.groupBy(_.productId).map(_._2.head)
      .filter(delProd => (verifyTypesList ++ previousSriList).map(_.productId).contains(delProd.productId)).toSet

    val pkVDeletables = mayBePkChanges.groupBy(_.productId).map(_._2.head)
      .filter(pkDProd => (verifyTypesList ++ previousSriList).map(_.productId).contains(pkDProd.productId)).toSet
      .filterNot(pkDProd => dRecords.map(_.productId).contains(pkDProd.productId))

    val deletables = dRecords ++ pkVDeletables

    val verifyTypesWithSpeciallyIntroducedTypes = verifyTypesList ++ deletables

    val previousSriSingleInstancePerKey = previousSriList
      .groupBy(_.productId) //have only one instance per primarykey
      .map(_._2.head)
      .toList

    //Enable this if you would like to log the case class for interactive debugging on spark-shell
    //logDatasetForDebugging(verifyTypesWithSpeciallyIntroducedTypes, previousSriSingleInstancePerKey)

    import sqlContext.implicits._
    val todaysVerifyTypesDF = spark.sparkContext.parallelize(verifyTypesWithSpeciallyIntroducedTypes).toDF
    val previousSriDF = spark.sparkContext.parallelize(previousSriSingleInstancePerKey).toDF

    println("###################################### TODAY'S VERIFY TYPES ######################################")
    todaysVerifyTypesDF.show(100, false)
    println("######################################     PREVIOUS SRI     ######################################")
    previousSriDF.show(100, false)


    //TODO Duplicated across Testcase and assertions
    val januaryFirst = new DateTime(2017, 1, 1, 10, 0)
    val tableName = "DUMMY_TABLENAME"

    val recordTypeBasedProcessor = new RecordTypeBasedProcessor

    val SriOutput(sriOpen, sriNonOpen, duplicatesOpt, _, bRecordsOpt) = recordTypeBasedProcessor.processSri(tableName, dataCols, pkCols, todaysVerifyTypesDF, previousSriDF,
      recordTypeMeta, TableType.DELTA, RunType.INCREMENTAL, januaryFirst)(spark)

    println("######################################       SRI OPEN       ######################################")
    sriOpen.show(100, false)
    println("######################################     SRI NON OPEN     ######################################")
    sriNonOpen.show(100, false)
    println("######################################       DUPLICATES       ######################################")
    duplicatesOpt.show(100, false)
    /*println("######################################       B-RECORDS       ######################################")
    bRecordsOpt.foreach(_.show(false))*/

    val sriOpenRowIds = sriOpen.select("rowId").collect().map(row => row.get(0).toString).toSet
    val sriNonOpenRowIds = sriNonOpen.select("rowId").collect().map(row => row.get(0).toString).toSet

    val duplicatesRowIds = duplicatesOpt.select("rowId").collect().map(row => row.get(0).toString).toSet

    val verifyTypesWithoutBRecords = verifyTypesWithSpeciallyIntroducedTypes.toSet
    val bRecords = verifyTypesWithSpeciallyIntroducedTypes.filter(_.c_operationtype == "B").toSet

    //val latestPerKeyInVTypes = verifyTypesWithSpeciallyIntroducedTypes.groupBy(_.productId).map(_._2.last).toSet
    val latestPerKeyInVTypes = verifyTypesWithoutBRecords
      .groupBy(row => (row.c_journaltime, row.c_transactionid, row.c_operationtype, row.c_userid, row.productId, row.currencyVal, row.price))
      .map { case (_, listRow) => listRow.head }
      .groupBy(_.productId)
      .mapValues(groupList => groupList.maxBy(delimRow => delimRow.rowId))
      .values.toSet

    //All the rows with the same primary key as the deleted row
    val rowsWithDeletablesPK = latestPerKeyInVTypes.filter(dRow => deletables.map(_.productId).contains(dRow.productId))

    val oldAsAndDsDuringTheDay = verifyTypesWithoutBRecords.diff(latestPerKeyInVTypes)
    //Has to go into SRI NON_OPEN -  Updates for I records (+ Old As for the day)
    val prevSriOpenProductsInVTypes = previousSriSingleInstancePerKey.filter(row => verifyTypesWithoutBRecords.map(_.productId).contains(row.productId)).toSet
    //Has to stay in SRI OPEN - Previous Sri open for which no update has been received (+ Latest update on Verify types)
    val prevSriOpenProductsNotInVTypes = previousSriSingleInstancePerKey.filterNot(row => verifyTypesWithoutBRecords.map(_.productId).contains(row.productId)).toSet

    //SRI OPEN = latestInVerifyTypes + PrevSRI data which is not updated through verify types - D Records
    val sriOpenCountCheck = (latestPerKeyInVTypes.size + prevSriOpenProductsNotInVTypes.size) - deletables.size === sriOpen.count()
    //SRI NON_OPEN = prevSRI Data which is updated through today's VTypes + Changes during the day in Vtypes + D records - bRecords
    val sriNonOpenCountCheck = prevSriOpenProductsInVTypes.size + oldAsAndDsDuringTheDay.size + deletables.size - bRecords.size + pkVDeletables.size === sriNonOpen.count()

    val sriOpenRowIdCheck = (prevSriOpenProductsNotInVTypes.map(_.rowId) ++ latestPerKeyInVTypes.map(_.rowId)).diff(rowsWithDeletablesPK.map(_.rowId)) === sriOpenRowIds
    val sriNonOpenRowIdCheck = prevSriOpenProductsInVTypes.map(_.rowId) ++ oldAsAndDsDuringTheDay.map(_.rowId) ++ rowsWithDeletablesPK.map(_.rowId) -- bRecords.map(_.rowId) ++ pkVDeletables.map(_.rowId) === sriNonOpenRowIds

    println(s"sriOpenCountCheck: $sriOpenCountCheck   -> ${sriOpen.count()} === ${latestPerKeyInVTypes.size} + ${prevSriOpenProductsNotInVTypes.size} - ${deletables.size}")
    println(s"sriNonOpenCountCheck: $sriNonOpenCountCheck   -> ${sriNonOpen.count()} === ${prevSriOpenProductsInVTypes.size} + ${oldAsAndDsDuringTheDay.size} + ${deletables.size} - ${duplicatesRowIds.size}")
    println(s"sriOpenRowIdCheck: $sriOpenRowIdCheck")
    println(s"sriNonOpenRowIdCheck: $sriNonOpenRowIdCheck")

    sriOpenCountCheck && sriNonOpenCountCheck && sriOpenRowIdCheck && sriNonOpenRowIdCheck
  }
}