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
package com.sc.sdm

import java.sql.{Date => SqlDate, Timestamp => SqlTimestamp}
import java.text.SimpleDateFormat

import com.sc.sdm.models.SriParams
import com.sc.sdm.utils.XmlParser.{ColumnConfig, TableConfig}
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, udf}
import org.apache.spark.sql.types.{DataTypes, DateType, TimestampType}

package object processor {

  implicit class DataframeOps(df: DataFrame) {
    /**
      * NiFi 1.2 doesn't support Avro's Logical types. Decimal, Date and Timestamp values are stored as string in Storage
      * and recovered as original datatypes in Sri.
      *
      * @param sriParams
      * @param tableConfig
      * @return
      */
    def transformExceptionalTypes(sriParams: SriParams, tableConfig: TableConfig): DataFrame = {
      val dateParser: (String => SqlDate) = { sDate =>
        if (StringUtils.isBlank(sDate) || sDate == "null") null
        else {
          val formatter = new SimpleDateFormat(sriParams.dateColFormat)
          new SqlDate(formatter.parse(sDate).getTime)
        }
      }
      val dateParserUDF = udf(dateParser)

      val timestampParser: (String => SqlTimestamp) = { sDate =>
        if (StringUtils.isBlank(sDate) || sDate == "null") null
        else {
          val formatter = new SimpleDateFormat(sriParams.timestampColFormat)
          new SqlTimestamp(formatter.parse(sDate).getTime)
        }
      }
      val timestampParserUDF = udf(timestampParser)

      var mutableDf = df
      tableConfig.getColumnConfig().foreach {
        case ColumnConfig(name, typee, _, _, _) if StringUtils.startsWithIgnoreCase(typee, "DECIMAL") =>
          val (scale, precision) = getScaleAndPrecision(typee)
          mutableDf = mutableDf.withColumn(name + "_sricasted", col(name).cast(DataTypes.createDecimalType(scale, precision))).drop(name).withColumnRenamed(name + "_sricasted", name)

        case ColumnConfig(name, typee, _, _, _) if StringUtils.equalsIgnoreCase(typee, "DATE") =>
          mutableDf = mutableDf.withColumn(name + "_sricasted", dateParserUDF(col(name)).cast(DateType)).drop(name).withColumnRenamed(name + "_sricasted", name)

        case ColumnConfig(name, typee, _, _, _) if StringUtils.equalsIgnoreCase(typee, "TIMESTAMP") =>
          mutableDf = mutableDf.withColumn(name + "_sricasted", timestampParserUDF(col(name)).cast(TimestampType)).drop(name).withColumnRenamed(name + "_sricasted", name)
        case _ =>
      }

      mutableDf
    }


    private def getScaleAndPrecision(dataType: String) = {
      if (dataType.contains(",")) {
        val precisionScaleArray = StringUtils.substringBetween(dataType, "(", ")").split(",")
        (precisionScaleArray(0).toInt, precisionScaleArray(1).toInt)
      }
      else {
        val precisionStr = StringUtils.substringBetween(dataType, "(", ")")
        (precisionStr.toInt, 0)
      }
    }
  }

}
