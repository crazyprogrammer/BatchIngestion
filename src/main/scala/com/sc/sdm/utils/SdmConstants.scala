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

package com.sc.sdm.utils

import org.joda.time.DateTime
import org.joda.time.format.{DateTimeFormat, DateTimeFormatter}

/** Consolidation of all the common constants and minor utility functions **/

object SdmConstants {

  val PartitionFormatString = "yyyy-MM-dd"
  val TableLevelEod = "table_level_eod"

  val BusinessDateFormat = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss")
  val PartitionFormat = DateTimeFormat.forPattern("yyyy-MM-dd")
  val yyyyMMddHHmmssFormat = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss")

  // Table Config Constants
  val PrefixConfigEdmHdpIfTable = "edmhdpif.table."
  val SuffixConfigColSchema = ".col_schema"
  val SuffixConfigKeyCols = ".keycols"
  val SuffixConfigSourceType = ".sourcetype"
  val SuffixConfigReconQuery = ".query"
  val SuffixConfigDeleteValue = ".deletevalue"
  val SuffixConfigDeleteIndex = ".deleteindex"
  val SuffixConfigDeleteFieldName = ".deletefield"
  val SuffixConfigOperationTypeCol = ".operationtypecol"
  val SuffixConfigInsertValue = ".insertvalue"
  val SuffixConfigBeforeValue = ".beforevalue"
  val SuffixConfigAfterValue = ".aftervalue"
  val SuffixConfigRunType = ".runtype"
  val EndOfDays = "9999-12-31"
  val EndOfTimes = "9999-12-31 00:00:00"

  //FIXME Find a better home for the following two functions
  /** Given a pattern, returns a DateTimeFormatter corresponding to that format. **/
  lazy val formatter: String => DateTimeFormatter = { str: String => DateTimeFormat forPattern str }

  /** Returns the current time as a String */
  def nowAsString(): String = yyyyMMddHHmmssFormat.print(DateTime.now())

}
