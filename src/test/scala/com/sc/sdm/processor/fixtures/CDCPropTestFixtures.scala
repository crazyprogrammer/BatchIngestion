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

package com.sc.sdm.processor.fixtures

import com.fortysevendeg.scalacheck.datetime.GenDateTime.genDateTimeWithinRange
import com.fortysevendeg.scalacheck.datetime.instances.joda._
import com.sc.sdm.processor._
import org.joda.time.format.DateTimeFormat
import org.joda.time.{DateTime, Period}
import org.scalacheck.Gen
import org.scalacheck.Gen.uuid
import org.scalatest.Suite
import org.scalatest.prop.Checkers

/** Trait that all CDC Prop testing will mix-in. The important method is the `createGenerator` which generates random data for property based testing */
trait CDCTestFixtures {

  val dateFormat = DateTimeFormat.forPattern("yyyy-MM-dd")
  val timeFormat = DateTimeFormat.forPattern("HH:mm:ss")
  val javaDateTimeFormat = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss.SSSSSS")

  def recordTypeMeta: RecordTypeMetaInfo

  object CDCDataGenerators {

    /** Data generator for CDC
      *
      * based source systems
      *
      * Creates three generators for every call -
      * 1. VerifyTypes/Previous SRI
      * 2. 'D' records generator - just a D record
      * 3. 'B' records generator - just a B record
      */
    def createGenerator(businessDate: DateTime, range: Period, operationTypes: List[String], productIds: List[String]): (Gen[List[CDCTableRow]], Gen[List[CDCTableRow]], Gen[List[CDCTableRow]]) = {

      val timestampGen = genDateTimeWithinRange(businessDate, range)
      val timeStamp = timestampGen.sample.get
      var num = 0

      //Generates a single 'I' CDC row for operation type I or generates a B/A pair if the operationType is B
      val cdcTableRowGenerator = for {
        uuidOne <- uuid
        uuidTwo <- uuid

        localJournalTS = {
          num = num + 1
          timeStamp.plusSeconds(num)
        }

        vds = dateFormat.print(localJournalTS)

        journalTS = javaDateTimeFormat.print(localJournalTS)
        transactionId <- Gen.oneOf("TRAN1", "TRAN2", "TRAN3")
        operationType <- Gen.oneOf(operationTypes)
        userId <- Gen.oneOf("ARUN12", "SYSTEM")

        productId <- Gen.oneOf(productIds)
        currencyVal <- Gen.oneOf("USD", "SGD")
        description <- Gen.alphaUpperStr
        trimmedDesc = description.take(25)
        price <- Gen.choose[Float](1.0f, 1000.0f)
        filename <- Gen.oneOf("FNAME1", "FNAME2")

        cdcTableRow = if (operationType == "B" || operationType == "A") {
          val rowIdA = s"${localJournalTS.getMillis + 1}_${uuidOne.toString}"
          val rowIdB = s"${localJournalTS.getMillis}_${uuidTwo.toString}"
          List(
            CDCTableRow(rowIdB, filename, vds, journalTS, transactionId, "B", userId, productId, currencyVal, trimmedDesc, price - 1),
            CDCTableRow(rowIdA, filename, vds, journalTS, transactionId, "A", userId, productId, currencyVal, trimmedDesc, price)
          )
        }
        else {
          val rowId = s"${localJournalTS.getMillis}_${uuidOne.toString}"
          List(CDCTableRow(rowId, filename, vds, journalTS, transactionId, operationType, userId, productId, currencyVal, trimmedDesc, price))
        }

      } yield cdcTableRow


      //Generates 'D' records - to be appended at the end of the verify types
      val deletableProductsGen = for {
        uuid <- uuid
        prodId <- Gen.oneOf(productIds)
        localJournalTS = {
          num = num + 1
          timeStamp.plusMinutes(num)
        }

        delJTS = localJournalTS
        filename <- Gen.oneOf("FNAME1", "FNAME2")
      } yield CDCTableRow(rowId = s"${delJTS.getMillis}_${uuid.toString}", filename, vds = dateFormat.print(delJTS), javaDateTimeFormat.print(delJTS), "TRAN9", "D", "SYSTEM", prodId, "USD", "DELETE DESC", 0.0f)

      //Generates a standalone 'B' (not a B/A pair).  This is for Primary key change usecase
      val pkChangeGen = for {
        uuid <- uuid
        prodId <- Gen.oneOf(productIds)
        localJournalTS = {
          num = num + 3
          timeStamp.plusMinutes(num)
        }
        pkChangeJTS = localJournalTS
        filename <- Gen.oneOf("FNAME1", "FNAME2")
      } yield CDCTableRow(rowId = s"${pkChangeJTS.getMillis}_${uuid.toString}", filename, vds = dateFormat.print(pkChangeJTS), javaDateTimeFormat.print(pkChangeJTS), "TRAN7", "B", "SYSTEM", prodId, "SGD", "PKCHANGE", 0.0f)

      val cdcRowListGen = Gen.containerOfN[List, List[CDCTableRow]](10, cdcTableRowGenerator).flatMap(coll => coll.flatten.toList)
      val deletableProdGenList = Gen.containerOfN[List, CDCTableRow](5, deletableProductsGen)
      val pkChangeGenList = Gen.containerOfN[List, CDCTableRow](2, pkChangeGen)

      (cdcRowListGen, deletableProdGenList, pkChangeGenList)

    }
  }

}

trait CDCDebugLogger {

  self: Suite with SdmSharedSparkContext with Checkers with CDCTestFixtures =>

  /** Logs the VerifyTypes and PreviousSri as case classes that's easier to run on spark-shell */
  protected def logDatasetForDebugging(updatedVerifyTypesList: List[CDCTableRow], previousSriList: List[CDCTableRow]) = {
    println("###################################### VERIFY TYPES (DEBUG) ######################################")
    updatedVerifyTypesList.foreach(printCDCTableRow)
    println()
    println("###################################### PREVIOUS SRI (DEBUG) ######################################")
    previousSriList.foreach(printCDCTableRow)
    println()
  }

  /** Prints a quoted toString of the CDCTableRow case class */
  private def printCDCTableRow(each: CDCTableRow) = {
    println(
      s"""CDCTableRow("${each.rowId}","${each.vds}","${each.c_journaltime}","${each.c_transactionid}","${each.c_operationtype}","${each.c_userid}","${each.productId}","${each.currencyVal}","${each.description}",${each.price}f)""")
  }

}

/** Represents a single CDC table row. A list of these are generated using scalacheck generators and later converted to Dataframes */
case class CDCTableRow(rowId: String, filename: String, vds: String,
                       c_journaltime: String, c_transactionid: String, c_operationtype: String, c_userid: String,
                       productId: String, currencyVal: String, description: String, price: Float,
                       start_date: String = "", start_time: String = "", end_date: String = "", end_time: String = "", del_flag: String = "N")


object CDCTableRow {
  val javaDateTimeFormat = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss.SSSSSS")

  implicit def orderingRowId[A <: CDCTableRow]: Ordering[A] = Ordering.by(row => javaDateTimeFormat.parseDateTime(row.c_journaltime).getMillis)
}