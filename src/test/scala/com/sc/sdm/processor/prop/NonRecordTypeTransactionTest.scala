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

class NonRecordTypeTransactionTest extends FunSuite with SdmSharedSparkContext with Checkers with NonCDCTestFixtures {

  val dataCols = List("drReason", "drReasonE", "drReasonC", "drKind", "drDrrKind")
  val sriAuditCols = List("start_date", "start_time", "end_date", "end_time", "del_flag")
  val vTypesAuditCols = List[String]("rowId", "vds", "filename")
  val metaInfo = NonRecordTypeMetaInfo(timestampColName = "start_time", timestampColFormat = "yyyy-MM-dd HH:mm:ss", vTypesAuditCols, sriAuditCols, List[String]())

  val januaryFirst = new DateTime(2017, 1, 1, 10, 0)
  val december31st = new DateTime(2016, 12, 31, 10, 0)

  test("Non RecordType Transaction Test") {

    spark.sparkContext.setLogLevel("ERROR")

    val todaysVerifyTypesListGen = DelimitedDataGenerators.createGeneratorWithoutRecordType(januaryFirst)

    val previousSriListGen = DelimitedDataGenerators.createGeneratorWithoutRecordType(december31st)
    check {
      forAllNoShrink(todaysVerifyTypesListGen, previousSriListGen) { (verifyTypesList, unusedPrevSri) =>
        getAllAssertions(verifyTypesList, unusedPrevSri)
      }
    }
  }

  def getAllAssertions(verifyTypesList: List[DelimitedTableRow], unusedPrevSri: List[DelimitedTableRow]) = {

    implicit val sqlContext = spark.sqlContext

    val duplicates = verifyTypesList.last.copy(rowId = s"${System.nanoTime()}_${UUID.randomUUID().toString}")

    val verifyTypesWithDups = verifyTypesList ++ List(duplicates)

    import sqlContext.implicits._

    val todaysVerifyTypesDF = spark.sparkContext.parallelize(verifyTypesWithDups).toDF

    println("###################################### TODAY'S VERIFY TYPES ######################################")
    todaysVerifyTypesDF.show(100, false)

    val errorRecord = DelimitedTableRow("ERROR", "ERROR", "ERROR", "ERROR", "ERROR", "ERROR", "ERROR", "ERROR", "ERROR", "ERROR")
    val previousSriDFDUMMY = spark.sparkContext.parallelize(List(errorRecord)).toDF()
    val tableName = "DUMMY_TABLENAME"

    val processor = new NonRecordTypeBasedProcessor

    //TODO Duplicated across Testcase and assertions
    val SriOutput(sriOpen, sriNonOpen, duplicatesOpt, _, _) = processor.processSri(tableName, dataCols, List("drReason", "rowId"), todaysVerifyTypesDF,
      previousSriDFDUMMY, metaInfo, TableType.TXN, RunType.INCREMENTAL, januaryFirst)(spark)

    println("######################################       SRI OPEN       ######################################")
    sriOpen.show(false)
    println("######################################     SRI NON OPEN     ######################################")
    sriNonOpen.show(false)
    println("######################################       DUPLICATES       ######################################")
    duplicatesOpt.show(false)

    val sriOpenRowIds = sriOpen.select("rowId").collect().map(row => row.get(0).toString).toSet
    val sriNonOpenRowIds = sriNonOpen.select("rowId").collect().map(row => row.get(0).toString).toSet

    val duplicatesRowIds = duplicatesOpt.select("rowId").collect().map(row => row.get(0).toString).toSet

    //SRI OPEN = latestInVerifyTypes + PrevSRI data which is not updated through verify types - D Records
    val sriOpenCountCheck = sriOpen.count() === (verifyTypesWithDups.size - duplicatesRowIds.size)

    //SRI NON_OPEN = prevSRI Data which is updated through today's VTypes + Changes during the day in Vtypes + D records - bRecords
    val sriNonOpenCountCheck = sriNonOpen.count() == 0

    //Actual record checks
    val sriOpenRowIdCheck = sriOpenRowIds === verifyTypesWithDups.map(_.rowId).toSet.diff(duplicatesRowIds)

    val sriNonOpenRowIdCheck = sriNonOpenRowIds.isEmpty

    println(s"sriOpenCountCheck: $sriOpenCountCheck")
    println(s"sriNonOpenCountCheck: $sriNonOpenCountCheck")
    println(s"sriOpenRowIdCheck: $sriOpenRowIdCheck")
    println(s"sriNonOpenRowIdCheck: $sriNonOpenRowIdCheck")

    sriOpenCountCheck && sriNonOpenCountCheck && sriOpenRowIdCheck && sriNonOpenRowIdCheck
  }
}