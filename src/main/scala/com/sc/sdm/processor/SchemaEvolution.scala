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

package com.sc.sdm.processor


import java.util.Date

import com.sc.sdm.models.SriParams
import com.sc.sdm.utils.SchemaUtils.{AllTables, ConfigHistory}
import com.sc.sdm.utils.SdmConstants._
import com.sc.sdm.utils.XmlParser.{AllTabColumn, TableConfig}
import com.sc.sdm.utils._
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.log4j.Logger
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.joda.time.Days

import scala.collection.mutable.ListBuffer
import scala.util.Try

/**
  * Entry point of the Schema Evolution process. The primary function of this class is to compare the old and the new schema
  * and retrofit data, if needed, for a specific window.
  * Handles Addition and Deletion of columns in the beginning, middle and end. Does not support renaming of columns.
  *
  */
object SchemaEvolution {

  val avroFormat = "com.databricks.spark.avro"
  val orcFormat = "orc"

  val logger: Logger = Logger.getLogger(SchemaEvolution.getClass)

  def main(args: Array[String]): Unit = {

    if (args.length != 3 && args.length != 5) {
      logger.error("Usage : SchemaEvolution <new table config xml hdfs path> <parameter xml hdfs path> <hdfs user> <startMarker(optional)> <endMarker(optional)> ")
      sys.error("Usage : SchemaEvolution <new table config xml hdfs path> <parameter xml hdfs path> <hdfs user> <startMarker(optional)> <endMarker(optional)> ")
      System.exit(1)
    }

    val newTableConfigXML = args(0)
    val paramPath = args(1)

    val fs: FileSystem = FileSystemUtils.fetchFileSystem
    val sriParams: SriParams = SriParams(SriUtils.getParamConf(fs, paramPath))
    val newTableConfigs = XmlParser.parseXmlConfig(newTableConfigXML, fs)
    val oldTableConfigs = XmlParser.parseXmlConfig(sriParams.tableConfigXml, fs).map(x => {
      (x.name, x)
    }).toMap

    updateSchema(findModifiedTables(fs, newTableConfigs, oldTableConfigs), getPartitions(args), sriParams, newTableConfigs, oldTableConfigs)

    //Replace old config file with new one
    fs.rename(new Path(sriParams.tableConfigXml), new Path(sriParams.tableConfigXml + "_bkp_" + new Date().getTime))
    fs.rename(new Path(newTableConfigXML), new Path(sriParams.tableConfigXml))
  }

  /**
    * Modifies the Hive table structure and retrofits the data across Sri open, NonOpen and storage for all the modified tables
    *
    * @param modifiedTables     List of [[TableConfig]] for all modified tables
    * @param allPartitions      List of affected partitions
    * @param sriParams          Environment parameters in the form of [[SriParams]]
    * @param newTableConfigs    List of new [[TableConfig]] generated from NiFi
    * @param oldTableConfigsMap List of old [[TableConfig]] that was used before the Schema evolution
    */
  private def updateSchema(modifiedTables: List[TableConfig], allPartitions: List[String], sriParams: SriParams, newTableConfigs: List[TableConfig], oldTableConfigsMap: Map[String, TableConfig]) = {

    val spark = SparkSession
      .builder()
      .appName("Schema Evolution")
      .enableHiveSupport()
      .getOrCreate()

    val isCdcSource = sriParams.isCdcSource
    val hdfsBaseDir = sriParams.hdfsBaseDir
    val hdfsTmpDir = sriParams.hdfsTmpDir

    logger.info ("Running Schema evolution for tables : " + modifiedTables.map(_.name) + s" across following partitions : $allPartitions")

    modifiedTables.foreach {
      newTableConfig =>
        val tableName = newTableConfig.name

        logger.info (s"Schema evolution in progress for : $tableName")

        val sriOpenSchema = sriParams.sriOpenSchema
        val sriNonOpenSchema = sriParams.sriNonOpenSchema
        val verifyTypeSchema = sriParams.verifyTypesSchema

        //Retrofit data for Sri Open
        evolveSchemaForData(spark, tableName, allPartitions, hdfsBaseDir, hdfsTmpDir,
          SriUtils.fetchSriStructure(oldTableConfigsMap(tableName), sriParams.sriOpenPartitionColumn, isCdcSource),
          SriUtils.fetchSriStructure(newTableConfig, sriParams.sriOpenPartitionColumn, isCdcSource),
          sriOpenSchema, sriParams.sriOpenPartitionColumn, orcFormat)

        //Retrofit data for Sri Open
        evolveSchemaForData(spark, tableName, allPartitions, hdfsBaseDir, hdfsTmpDir,
          SriUtils.fetchSriStructure(oldTableConfigsMap(tableName), sriParams.sriNonOpenPartitionColumn, isCdcSource),
          SriUtils.fetchSriStructure(newTableConfig, sriParams.sriNonOpenPartitionColumn, isCdcSource),
          sriNonOpenSchema, sriParams.sriNonOpenPartitionColumn, orcFormat)

        //Retrofit data for Storage
        evolveSchemaForData(spark, tableName, allPartitions, hdfsBaseDir, hdfsTmpDir,
          SriUtils.fetchVerifyStructure(oldTableConfigsMap(tableName), sriParams.verifyTypesPartitionColumn, isCdcSource),
          SriUtils.fetchVerifyStructure(newTableConfig, sriParams.verifyTypesPartitionColumn, isCdcSource),
          verifyTypeSchema, sriParams.verifyTypesPartitionColumn, avroFormat)

        val hiveQueries = SchemaUtils.getBusinessTablesSchema(newTableConfig, sriParams)
        hiveQueries.foreach(spark.sql)

        SriUtils.moveHdfsFiles(hdfsTmpDir + "/" + sriOpenSchema + "/" + tableName, hdfsBaseDir + "/" + sriOpenSchema + "/" + tableName, spark)
        SriUtils.moveHdfsFiles(hdfsTmpDir + "/" + sriNonOpenSchema + "/" + tableName, hdfsBaseDir + "/" + sriNonOpenSchema + "/" + tableName, spark)
        SriUtils.moveHdfsFiles(hdfsTmpDir + "/" + verifyTypeSchema + "/" + tableName, hdfsBaseDir + "/" + verifyTypeSchema + "/" + tableName, spark)

        logger.info (s"Schema evolution compeleted for : $tableName")
    }
    populateCommonMetadata(modifiedTables, newTableConfigs, sriParams, spark)
  }

  /** Updates the metadata tables with the updated version of the schema - [[AllTables]] [[AllTabColumn]] and [[ConfigHistory]] */
  private def populateCommonMetadata(modifiedTables: List[TableConfig], newTableConfigs: List[TableConfig], sriParams: SriParams, spark: SparkSession): Unit = {

    val df1 = spark.read.format("orc").load(s"${sriParams.hdfsBaseDir}${sriParams.metadataSchema}/scb_all_tables/${sriParams.source}_${sriParams.country}_scb_all_tables").cache
    val tableNameWdVersion: Map[String, Int] = df1.groupBy("tableName").max("version").collect.map(row => row.getAs[String](0) -> row.getAs[Int](1)).toMap

    val configs = ListBuffer[ConfigHistory]()
    val allTables = ListBuffer[AllTables]()
    val allTabColumns = ListBuffer[AllTabColumn]()

    newTableConfigs.foreach { tableConfig =>

      val tableName: String = tableConfig.name.replace(s"${sriParams.source}_${sriParams.country}_", "")
      val latestVersion = tableNameWdVersion.getOrElse(tableName, 1) + 1
      val currentTime = nowAsString()
      val allTabColumn = XmlParser.getAllTabColumns(currentTime, sriParams.source, sriParams.country, tableConfig, latestVersion)
      val scbAllTables = List(AllTables(sriParams.country, tableName, sriParams.source, tableConfig.sourceType, "0p", "", "", "", "", latestVersion, latestVersion,
        "Y", currentTime, EndOfTimes, currentTime, currentTime, "Y"))
      val configHistory = List(ConfigHistory("", tableName, sriParams.sourcingType.toString, allTabColumn.map(_.columnName).mkString(","), tableConfig.colSchema, currentTime))
      configs ++= configHistory

      if (modifiedTables.contains(tableConfig)) {
        allTables ++= scbAllTables
        allTabColumns ++= allTabColumn
      }
    }

    spark.createDataFrame(allTables).write.mode("append").format("orc").save(s"hdfs://${sriParams.hdfsBaseDir}${sriParams.metadataSchema}/scb_all_tables/${sriParams.source}_${sriParams.country}_scb_all_tables")
    spark.createDataFrame(allTabColumns).write.mode("append").format("orc").save(s"hdfs://${sriParams.hdfsBaseDir}${sriParams.metadataSchema}/scb_all_tab_columns/${sriParams.source}_${sriParams.country}_scb_all_tab_columns")
    spark.createDataFrame(configs).write.mode("append").format("orc").save(s"hdfs://${sriParams.hdfsBaseDir}${sriParams.opsSchema}/${sriParams.source}_${sriParams.country}_config_history/${sriParams.source}_${sriParams.country}_config_history")
  }

  /** Loads tha table data as a [[DataFrame]], applies the latest schema on top of it and returns the updated one **/
  private def evolveSchemaForData(spark: SparkSession, tableName: String, allPartitions: List[String], hdfsBaseDir: String, hdfsTmpDir: String,
                                  sourceTableSchema: StructType, targetTableSchema: StructType, schemaName: String, partitionColumnName: String, format: String) = {
    val sourceDataFrame = Try(getSourceDataFrame(spark, (hdfsBaseDir + "/" + schemaName + "/" + tableName), partitionColumnName, allPartitions, format))
      .getOrElse(spark.createDataFrame(spark.sparkContext.emptyRDD[Row], sourceTableSchema))
    retrofitData(sourceDataFrame, targetTableSchema).write.format(format).partitionBy(partitionColumnName).save(hdfsTmpDir + "/" + schemaName + "/" + tableName)
  }

  private def getSourceDataFrame(spark: SparkSession, path: String, partitionColumnName: String, allPartitions: List[String], format: String): DataFrame = {
    val sourceDataFrame = spark.read.format(format).load(path)
    if (!allPartitions.isEmpty) {
      sourceDataFrame.filter(col(partitionColumnName).isin(allPartitions: _*))
    } else {
      sourceDataFrame
    }
  }

  /** Creates a new DataFrame with the updated schema **/
  def retrofitData(df: DataFrame, newTableSchema: StructType): DataFrame = {

    logger.info (s"Source Dataframe Structure: ${df.schema}")
    logger.info (s"Target Dataframe Structure: $newTableSchema")

    val sourceDFFields = df.schema.fields.map(_.name)
    val (availableColumns, newColumns) = newTableSchema.fields.partition(x => sourceDFFields.contains(x.name))
    var targetDF = df.select(availableColumns.map(x => col(x.name)): _ *)
    newColumns.foreach(newCol => targetDF = targetDF.withColumn(newCol.name, lit(null).cast(newCol.dataType)))

    targetDF.select(newTableSchema.fields.map(x => col(x.name)): _ *)
  }

  /** Identifies the tables whose schema has changed **/
  private def findModifiedTables(fs: FileSystem, newTableConfigs: List[TableConfig], oldTableConfigsMap: Map[String, TableConfig]): List[TableConfig] = {

    newTableConfigs.filterNot {
      tableConfig =>
        //Null pointer for table not available
        val oldColumnList = oldTableConfigsMap(tableConfig.name).getColumnConfig()
        val newColumnList = tableConfig.getColumnConfig()
        newColumnList.equals(oldColumnList)
    }
  }

  /** Given a start and end date, identifies the partitions to be retrofitted */
  private def getPartitions(args: Array[String]): List[String] = {
    if (args.length == 3) {
      return List()
    }

    val startDay = PartitionFormat.parseDateTime(args(3))
    val endDay = PartitionFormat.parseDateTime(args(4))

    (for (f <- 0 to Days.daysBetween(startDay, endDay).getDays) yield startDay.plusDays(f)).flatMap(x => {
      val verifyTypePartition = PartitionFormat.print(x)
      List(verifyTypePartition, verifyTypePartition + "_early", verifyTypePartition + "_late", PartitionFormat.print(x))
    }).toList
  }
}