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

object Enums {

  /**
    * Defines whether a table is of type Delta or Transaction
    */
  object TableType extends Enumeration {
    type TableType = Value

    val DELTA, TXN = Value

    def fromString(tableType: String): Option[Value] = values.find(value => value.toString.equalsIgnoreCase(tableType))
  }

  /**
    * Defines whether the files of a table are to be treated as Fulldump/Snapshot or incremental data
    */
  object RunType extends Enumeration {
    type RunType = Value
    val FULLDUMP, INCREMENTAL = Value

    def fromString(runType: String): Option[Value] = values.find(value => value.toString.equalsIgnoreCase(runType))
  }

  /**
    * Defines whether the source application has CDC generated files, FIXEDWIDTH files or plain DELIMITED files
    */
  object SourceType extends Enumeration {
    type SourceType = Value
    val CDC, BATCH_FIXEDWIDTH, BATCH_DELIMITED = Value

    def fromString(sourceType: String): Option[Value] = values.find(value => value.toString.equalsIgnoreCase(sourceType))
  }

}