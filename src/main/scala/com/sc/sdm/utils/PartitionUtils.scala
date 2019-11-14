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

import org.joda.time._

object PartitionUtils {

  import SdmConstants._

  /**
    * Given a start partition and a time range with a limit to go back to, returns all the previous partitions
    *
    * @param partition      Start partition
    * @param format         Format of the partition - Generally it is yyyy-MM-dd
    * @param unit           Unit of time - minutes, hours, days or months
    * @param value          Quantity of time that is to added/subtracted
    * @param iterationLimit The maximum number of times that this operation has to be applied (aka) the maximum time that we would want to go back or look forward
    * @return [[List]] of partitions
    */
  def allPartitions(partition: String, format: String, unit: String, value: Int, iterationLimit: Int): List[String] = {
    if (iterationLimit <= 0) Nil
    else {
      getPreviousBusinessDatePartition(partition, format, unit, value) ::
        allPartitions(getPreviousBusinessDatePartition(partition, format, unit, value), format, unit, value, iterationLimit - 1)
    }
  }

  /** Given a partition returns the next partition based on the frequency in which the partitions are created **/
  def getPreviousBusinessDatePartition(basePartition: String, partitionFormat: String, unit: String, value: Int): String = {
    val parsedDate: DateTime = PartitionFormat parseDateTime basePartition
    PartitionFormat print minusDateTime(unit, value, parsedDate)
  }

  /** Given a partition returns the next partition based on the frequency in which the partitions are created **/
  def getNextBusinessDatePartition(basePartition: String, partitionFormat: String, unit: String, value: Int): String = {
    val parsedDate: DateTime = PartitionFormat parseDateTime basePartition
    PartitionFormat print addDateTime(unit, value, parsedDate)
  }

  /**
    * Performs subtraction operation on [[DateTime]] given a unit and the value
    *
    * @param unit     Unit of deduction - minutes, hours, days or months
    * @param value    Actual quantity of time that has to be subtracted
    * @param baseDate [[DateTime]] on which the operation has to be performed
    * @return Immutable instance of the modified [[DateTime]]
    */
  private def minusDateTime(unit: String, value: Int, baseDate: DateTime): DateTime = unit match {
    case "minutes" => baseDate.minusMinutes(value)
    case "hours" => baseDate.minusHours(value)
    case "days" => baseDate.minusDays(value)
    case "months" => baseDate.minusMonths(value)
    case _ => throw new IllegalArgumentException(s"value $unit was expected to be in minutes or hours or days or months")
  }

  /**
    * Performs addition operation on [[DateTime]] given a unit and the value
    *
    * @param unit     Unit of deduction - minutes, hours, days or months
    * @param value    Actual quantity of time that has to be subtracted
    * @param baseDate [[DateTime]] on which the operation has to be performed
    * @return Immutable instance of the modified [[DateTime]]
    */
  private def addDateTime(unit: String, value: Int, baseDate: DateTime): DateTime = unit match {
    case "minutes" => baseDate.plusMinutes(value)
    case "hours" => baseDate.plusHours(value)
    case "days" => baseDate.plusDays(value)
    case "months" => baseDate.plusMonths(value)
    case _ => throw new IllegalArgumentException(s"value $unit was expected to be in minutes or hours or days or months")
  }
}

