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

import com.sc.sdm.utils.XmlParser.{ColumnConfig, TableConfig}
import org.apache.log4j.Logger
import org.apache.spark.sql.types._

import scala.util.matching.Regex

/**
  * Collection of all Utility methods that parse and convert the datatypes defined in the [[TableConfig]] into corresponding Spark Sql types.
  * TableConfig columns supports a variety of datatypes that are derived from the source databases.
  *
  * The reason why there is one for Avro and one for ORC is because of the inability of NiFi 1.2 to support logical types - Datetime, Decimal and Date.
  * These datatypes are stored as String in Avro and later recovered as their corresponding types.
  */
object TypeUtils {
  val logger = Logger.getLogger(TypeUtils.getClass)

  /** A [[PartialFunction]] that returns true if the input datatype is one of the accepted variants of String */
  private val _isString: PartialFunction[String, Boolean] = {
    case str: String if str.startsWith("VARCHAR") => true
    case str: String if str.startsWith("NVARCHAR") => true
    case str: String if str.startsWith("STRING") => true
    case str: String if str.startsWith("CHAR") => true
  }

  /** A [[PartialFunction]] that returns true if the input datatype is Decimal */
  private val _isDecimal: PartialFunction[String, Boolean] = {
    case str: String if str.startsWith("DECIMAL") => true
  }

  /** A [[PartialFunction]] that returns true if the input datatype is Double */
  private val _isDouble: PartialFunction[String, Boolean] = {
    case str: String if str.startsWith("DOUBLE") => true
  }

  /** A [[PartialFunction]] that returns true if the input datatype is Float */
  private val _isFloat: PartialFunction[String, Boolean] = {
    case str: String if str.startsWith("FLOAT") => true
  }

  /** A [[PartialFunction]] that returns true if the input datatype is one of the accepted variants of Integer */
  private val _isIntegral: PartialFunction[String, Boolean] = {
    case str: String if str.startsWith("SMALLINT") => true
    case str: String if str.startsWith("TINYINT") => true
    case str: String if str.startsWith("INT") => true
  }

  /** A [[PartialFunction]] that returns true if the input datatype is one of the accepted variants of Long */
  private val _isLong: PartialFunction[String, Boolean] = {
    case str: String if str.startsWith("BIGINT") => true
    case str: String if str.startsWith("LONG") => true
  }

  /** A [[PartialFunction]] that returns true if the input datatype is Boolean */
  private val _isBoolean: PartialFunction[String, Boolean] = {
    case str: String if str.startsWith("BOOLEAN") => true
  }

  /** A [[PartialFunction]] that returns true if the input datatype is Timestamp */
  private val _isTimeStamp: PartialFunction[String, Boolean] = {
    case str: String if str.startsWith("TIMESTAMP") => true
  }

  /** A [[PartialFunction]] that returns true if the input datatype is Date*/
  private val _isDate: PartialFunction[String, Boolean] = {
    case str: String if str.startsWith("DATE") => true
  }

  /** Types that aren't mapped by any of the above Partial functions would fail with a [[MatchError]] */
  private val _nopes: PartialFunction[String, Boolean] = {
    case _ => false
  }

  /** Regular expression that parses the scale and precision of a Decimal */
  private val scalePrecPattern: Regex = ".*\\((.*),(.*)\\)".r

  private val parseScaleAndPrecision: String => (String, String) = {
    case scalePrecPattern(scale, prec) => (scale, prec)
  }

  private val isString = _isString.orElse(_nopes)
  private val isDecimal = _isDecimal.orElse(_nopes)
  private val isDouble = _isDouble.orElse(_nopes)
  private val isFloat = _isFloat.orElse(_nopes)
  private val isLong = _isLong.orElse(_nopes)
  private val isBoolean = _isBoolean.orElse(_nopes)
  private val isTimeStamp = _isTimeStamp.orElse(_nopes)
  private val isDate = _isDate.orElse(_nopes)
  private val isIntegral = _isIntegral.orElse(_nopes)

  /**
    * Maps the columns in a table config to their corresponding Spark SQL Type for ORC
    */
  private val transformColumnConfigOrc: ColumnConfig => StructField = {
    case ColumnConfig(name, typee, _, _, _) if isString(typee) => StructField(name, StringType)
    case ColumnConfig(name, typee, _, _, _) if isDate(typee) => StructField(name, DateType)
    case ColumnConfig(name, typee, _, _, _) if isTimeStamp(typee) => StructField(name, TimestampType)
    case ColumnConfig(name, typee, _, _, _) if isDecimal(typee) =>
      StructField(name, DataTypes.createDecimalType(parseScaleAndPrecision(typee)._1.toInt, parseScaleAndPrecision(typee)._2.toInt))
    case ColumnConfig(name, typee, _, _, _) if isFloat(typee) => StructField(name, FloatType)
    case ColumnConfig(name, typee, _, _, _) if isIntegral(typee) => StructField(name, IntegerType)
    case ColumnConfig(name, typee, _, _, _) if isBoolean(typee) => StructField(name, BooleanType)
    case ColumnConfig(name, typee, _, _, _) if isDouble(typee) => StructField(name, DoubleType)
    case ColumnConfig(name, typee, _, _, _) if isLong(typee) => StructField(name, LongType)
  }

  /**
    * Maps the columns in a table config to their corresponding Spark SQL Type for ORC
    */
  private val transformColumnConfigAvro: ColumnConfig => StructField = {
    case ColumnConfig(name, typee, _, _, _) if isString(typee) => StructField(name, StringType)
    case ColumnConfig(name, typee, _, _, _) if isDate(typee) => StructField(name, StringType)
    case ColumnConfig(name, typee, _, _, _) if isTimeStamp(typee) => StructField(name, StringType)
    case ColumnConfig(name, typee, _, _, _) if isDecimal(typee) => StructField(name, StringType)
    case ColumnConfig(name, typee, _, _, _) if isFloat(typee) => StructField(name, FloatType)
    case ColumnConfig(name, typee, _, _, _) if isIntegral(typee) => StructField(name, IntegerType)
    case ColumnConfig(name, typee, _, _, _) if isBoolean(typee) => StructField(name, BooleanType)
    case ColumnConfig(name, typee, _, _, _) if isDouble(typee) => StructField(name, DoubleType)
    case ColumnConfig(name, typee, _, _, _) if isLong(typee) => StructField(name, LongType)
  }

  /** Given a [[TableConfig]] returns a Sequence of [[org.apache.spark.sql.types.StructField]] representing the columns in that table for VerifyTypes/Storage
    */
  val transformStructFieldsAvro: TableConfig => Seq[StructField] = {
    case tblConfig@TableConfig(name, source, keyCOls, colSchema, reconQuery, deleteIndex, deleteValue, _, _, _, _, _) =>
      XmlParser.fetchColumnConfig(tblConfig).map(transformColumnConfigAvro)
  }

  /** Given a [[TableConfig]] returns a Sequence of [[org.apache.spark.sql.types.StructField]] representing the columns in that table for Sri (ORC)
    */
  val transformStructFields: TableConfig => Seq[StructField] = {
    case tblConfig@TableConfig(name, source, keyCOls, colSchema, reconQuery, deleteIndex, deleteValue, _, _, _, _, _) =>
      XmlParser.fetchColumnConfig(tblConfig).map(transformColumnConfigOrc)
  }
}
