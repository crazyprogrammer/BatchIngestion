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

import com.sc.sdm.utils.SdmConstants._
import org.apache.commons.lang3.StringUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem

import scala.collection.JavaConverters._
import scala.collection.mutable

object XmlParser {

  def fetchColumnConfig(tableConfig: TableConfig): List[ColumnConfig] = getColumnConfigs(tableConfig.colSchema, "\\^")

  def getColumnConfigs(colsSchemas: String, delimiter: String) = {
    if (StringUtils.isBlank(colsSchemas)) List()
    else {
      colsSchemas.split(delimiter).foldRight(Nil: List[ColumnConfig]) {
        (colSchema: String, list: List[ColumnConfig]) =>
          getColumnConfigFromColSchema(colSchema) :: list
      }
    }
  }

  private def getColumnConfigFromColSchema(colSchema: String) = {
    val splitValues: Array[String] = colSchema.split(" ")
    val width = if (splitValues.length > 2 && splitValues(2).equals("WIDTH")) splitValues(3) else null
    val nullable: Boolean = {
      if (null != width) if (splitValues.length > 4 && splitValues(4).equals("NOT")) false else true
      else if (splitValues.length > 2 && splitValues(2).equals("NOT")) false else true
    }
    val comment = if (splitValues(splitValues.length - 2).equals("COMMENT")) splitValues(splitValues.length - 1) else null
    ColumnConfig(splitValues(0), splitValues(1), nullable, width, comment)
  }

  def fetchKeyColsFromTable(tableConfig: TableConfig): List[String] = {
    if (StringUtils.isNotBlank(tableConfig.keyCols)) {
      tableConfig.keyCols.split(",").map(_.trim).toList
    }
    else List()
  }

  def fetchColNamesFromTable(tableConfig: TableConfig): List[String] = {
    fetchColumnConfig(tableConfig).map(_.name)
  }

  def parseXmlConfig(tableXml: String, fs: FileSystem): List[TableConfig] = {
    val conf: Configuration = SriUtils.getParamConf(fs, tableXml)

    val patternR = (SdmConstants.PrefixConfigEdmHdpIfTable + "(.*)" + SdmConstants.SuffixConfigColSchema).r
    val parseTableName: String => String = {
      case patternR(name) => name
    }

    val tables: mutable.Map[String, String] = conf.getValByRegex(patternR.toString()).asScala
    tables.keys.foldRight(Nil: List[TableConfig]) {
      (key: String, tableList: List[TableConfig]) =>
        val tableName: String = parseTableName(key)
        val sourceType: String = conf.get(SdmConstants.PrefixConfigEdmHdpIfTable + tableName + SdmConstants.SuffixConfigSourceType)
        val keyCols: String = conf.get(SdmConstants.PrefixConfigEdmHdpIfTable + tableName + SdmConstants.SuffixConfigKeyCols)
        val colSchema: String = conf.get(SdmConstants.PrefixConfigEdmHdpIfTable + tableName + SdmConstants.SuffixConfigColSchema)
        val reconQuery: String = conf.get(SdmConstants.PrefixConfigEdmHdpIfTable + tableName + SdmConstants.SuffixConfigReconQuery)
        val deleteValue: String = conf.get(SdmConstants.PrefixConfigEdmHdpIfTable + tableName + SdmConstants.SuffixConfigDeleteValue)
        val deleteField: String = conf.get(SdmConstants.PrefixConfigEdmHdpIfTable + tableName + SdmConstants.SuffixConfigDeleteFieldName, "")

        val operationTypeCol: String = conf.get(SdmConstants.PrefixConfigEdmHdpIfTable + tableName + SdmConstants.SuffixConfigOperationTypeCol)
        val insertValue: String = conf.get(SdmConstants.PrefixConfigEdmHdpIfTable + tableName + SdmConstants.SuffixConfigInsertValue)
        val beforeValue: String = conf.get(SdmConstants.PrefixConfigEdmHdpIfTable + tableName + SdmConstants.SuffixConfigBeforeValue)
        val afterValue: String = conf.get(SdmConstants.PrefixConfigEdmHdpIfTable + tableName + SdmConstants.SuffixConfigAfterValue)
        val runType: Option[String] = Option(conf.get(SdmConstants.PrefixConfigEdmHdpIfTable + tableName + SdmConstants.SuffixConfigRunType))

        val deleteIndex: String = if (deleteField.nonEmpty) {
          (colSchema.split("\\^").map(_.split(" ")(0)).indexOf(deleteField) + 1).toString
        }
        else {
          (conf.getInt(SdmConstants.PrefixConfigEdmHdpIfTable + tableName + SdmConstants.SuffixConfigDeleteIndex, -2) + 1).toString
        }
        TableConfig(tableName, sourceType, keyCols, colSchema, reconQuery, deleteIndex, deleteValue, operationTypeCol, insertValue, beforeValue, afterValue, runType) :: tableList
    }
  }

  /** Constructs a List of [[AllTabColumn]] for a source-country given a [[TableConfig]] */
  def getAllTabColumns(currentTime: String, source: String, country: String, tableConfig: TableConfig, version: Int = 1): List[AllTabColumn] = {
    var keyColumns: List[String] = List()
    if (null != tableConfig.keyCols) {
      keyColumns = tableConfig.keyCols.split(",").toList
    }
    val columnConfig = fetchColumnConfig(tableConfig)
    columnConfig.zipWithIndex.map { case (column, index) =>
      val nullString = if (column.nullable) "Y" else "N"
      val isKeyCol = if (keyColumns.contains(column.name)) "Y" else "N"
      if (column.typee.contains("(")) {
        val dataType = column.typee.substring(0, column.typee.indexOf("("))
        val dataLength = column.typee.substring(column.typee.indexOf("(") + 1, column.typee.indexOf(")")).split(",")(0).toInt
        AllTabColumn(tableConfig.name.replace(source + "_" + country + "_", ""), column.name, country, source, dataType,
          dataLength, dataLength, isKeyCol,
          nullString, index, version, version, column.width,
          "1", currentTime, EndOfTimes, currentTime, currentTime, "")
      }
      else {
        AllTabColumn(tableConfig.name.replace(source + "_" + country + "_", ""), column.name, country, source, column.typee,
          0, 0, isKeyCol,
          nullString, index, version, version, column.width,
          "1", currentTime, EndOfTimes, currentTime, currentTime, "")
      }
    }
  }

  /** Represents an entry of versioned TableConfig in a flat format */
  case class AllTabColumn(tableName: String, columnName: String, country: String, source: String, dataType: String, dataLength: Int,
                          precision: Int, isPrimaryKey: String, nullable: String, order: Int, currentVersion: Int, version: Int, width: String,
                          valid: String, validFrom: String, validTo: String, creationDate: String, lastUpdateDate: String, Comments: String)

  //FIXME Re-arrange fields inside this case class
  /** Scala binding for the Table config XML */
  case class TableConfig(private val _name: String, sourceType: String, keyCols: String, colSchema: String, reconQuery: String, deleteIndex: String, deleteValue: String,
                         operationTypeCol: String, insertValue: String, beforeValue: String, afterValue: String, runType: Option[String]) {

    val name = _name.toLowerCase

    def isDeltaSource: Boolean = {
      sourceType.equals("delta")
    }

    def isTxnSource: Boolean = {
      sourceType.equals("txn")
    }

    def getColumnConfigForIndex(index: Int): ColumnConfig = {
      fetchColumnConfig(this)(index)
    }

    def getColumnConfig(): List[ColumnConfig] = {
      fetchColumnConfig(this)
    }
  }

  /** Scala binding for all the attributes of a column */
  case class ColumnConfig(name: String, typee: String, nullable: Boolean, width: String, comment: String)


}
