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

import com.sc.sdm.processor.RecordTypeMetaInfo
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat
import org.scalacheck.Gen
import org.scalacheck.Gen.uuid

/** Trait that all CDC Prop testing will mix-in. The important method is the `createGenerator` which generates random data for property based testing */
trait NonCDCTestFixtures {

  val dateFormat = DateTimeFormat.forPattern("yyyy-MM-dd")
  val dateTimeFormat = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss")


  object DelimitedDataGenerators {

    /** Data generator for Non-CDC but RecordType indicator based source systems
      *
      * Creates three generators for every call -
      * 1. VerifyTypes/Previous SRI
      * 2. 'D' records generator - just a D record
      */
    def createGeneratorWithRecordType(businessDate: DateTime, metaInfo: RecordTypeMetaInfo): (Gen[List[DelimitedTableRowWithRecordType]], Gen[List[DelimitedTableRowWithRecordType]]) = {

      val delimRowGenerator = for {
        uuidOne <- uuid
        fileName = "DELIM_FNAME"

        recordId = metaInfo.insertType
        drReason <- Gen.oneOf("TXC", "TWB", "ISS", "SPS", "MIG")
        drReasonE <- Gen.oneOf("Cash Txns", "Customer is linked", "Cannot determine SOW", "Domestic PEP")
        drReasonC <- Gen.oneOf("博奕或", "火相關", "行業的")
        drKind <- Gen.oneOf("MOJO", "DOJO", "FOJO")
        drDrrKind <- Gen.oneOf("MEEP", "BEEP")

        rowId = s"${System.nanoTime()}_${uuidOne.toString}"
      } yield DelimitedTableRowWithRecordType(rowId, fileName, recordId, drReason, drReasonE, drReasonC, drKind, drDrrKind, start_date = dateFormat.print(businessDate), start_time = dateTimeFormat.print(businessDate))

      //Generates 'D' records - to be appended at the end of the verify types
      val deletableRowGen = for {
        normalRow <- delimRowGenerator
      } yield normalRow.copy(recordType = metaInfo.deleteType)


      val delimRowListGen = Gen.containerOfN[List, DelimitedTableRowWithRecordType](10, delimRowGenerator)
      val deletableRowListGen = Gen.containerOfN[List, DelimitedTableRowWithRecordType](5, deletableRowGen)

      (delimRowListGen, deletableRowListGen)

    }

    /** Data generator for Non-CDC and Non-RecordType indicator based source systems **/
    def createGeneratorWithoutRecordType(businessDate: DateTime) = {

      val delimRowGenerator = for {
        uuidOne <- uuid
        fileName = "DELIM_FNAME"

        drReason <- Gen.oneOf("TXC", "TWB", "ISS", "SPS", "MIG")
        drReasonE <- Gen.oneOf("Cash Txns", "Customer is linked", "Cannot determine SOW", "Domestic PEP")
        drReasonC <- Gen.oneOf("博奕或", "火相關", "行業的")
        drKind <- Gen.oneOf("MOJO", "DOJO", "FOJO")
        drDrrKind <- Gen.oneOf("MEEP", "BEEP")

        rowId = s"${System.nanoTime()}_${uuidOne.toString}"
      } yield DelimitedTableRow(rowId, fileName, drReason, drReasonE, drReasonC, drKind, drDrrKind, start_date = dateFormat.print(businessDate), start_time = dateTimeFormat.print(businessDate))

      val delimRowListGen = Gen.containerOfN[List, DelimitedTableRow](10, delimRowGenerator)

      delimRowListGen

    }
  }

}

/** Represents a single Non-CDC table row with recordtype column that behaves similar to CDC's operationType column.
  * The difference between CDCTableRow and DelimitedRow is that there's no timestamp column in the data (similar to journaltimestamp) which serves as the priimary sort key.
  * A list of these are generated using scalacheck generators and later converted to Dataframes
  */
case class DelimitedTableRowWithRecordType(rowId: String, filename: String, recordType: String, drReason: String, drReasonE: String, drReasonC: String,
                                           drKind: String, drDrrKind: String,
                                           start_date: String = "", start_time: String = "", end_date: String = "", end_time: String = "", del_flag: String = "N")

/** Represents a single Non-CDC table row.
  * The start_date (business date) which is not a part of the data will k
  * A list of these are generated using scalacheck generators and later converted to Dataframes
  */
case class DelimitedTableRow(rowId: String, filename: String, drReason: String, drReasonE: String, drReasonC: String,
                             drKind: String, drDrrKind: String,
                             start_date: String = "", start_time: String = "", end_date: String = "", end_time: String = "", del_flag: String = "N")


object NonCDCDelimitedTableRowWithRecordType {
  val javaDateTimeFormat = DateTimeFormat.forPattern("yyyy-MM-dd")

  implicit def orderingRowId[A <: DelimitedTableRowWithRecordType]: Ordering[A] = Ordering.by(row => javaDateTimeFormat.parseDateTime(row.start_date).getMillis)
}
