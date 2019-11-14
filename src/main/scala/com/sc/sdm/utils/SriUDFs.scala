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

import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.functions.udf

/** Collection of User Defined functions that are used with the Spark [[org.apache.spark.sql.DataFrame]] */
object SriUDFs {

  /** Returns a default value if the original column value is null */
  val retainIfNotNullOrSet: ((String, String) => String) = { (actualValue, fallValue) =>
    if (StringUtils.isBlank(actualValue)) fallValue
    else actualValue
  }

  val retainIfNotNullOrSetUdf = udf(retainIfNotNullOrSet)

  /** Updates a column value with a new value if the column value is NOT null */
  val setIfPresentOrFallbackTo: ((String, String, String) => String) = { (actualValue, ifPresentValue, ifAbsentValue) =>
    if (StringUtils.isNotBlank(actualValue)) ifPresentValue
    else ifAbsentValue
  }

  val setIfPresentOrFallbackToUdf = udf(setIfPresentOrFallbackTo)

  /** The delete_flag must be 1 (or set to True) if a row is a 'D' record. Else, the delete_flag must be 0. */
  val getDeleteFlag: ((String, String) => String) = { (currentValue, deleteValue) =>
    if (StringUtils.equalsIgnoreCase(currentValue, deleteValue)) "1"
    else "0"
  }

  val getDeleteFlagUdf = udf(getDeleteFlag)
}
