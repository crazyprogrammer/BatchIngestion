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

import java.util.Date

import com.sc.sdm.models.SriParams
import com.sc.sdm.utils.XmlParser.{TableConfig, _}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.SQLContext

import scala.collection.mutable.ListBuffer

/**
  * Collection of functions that create Hive tables for VerifyTypes, Sri Open, Sri Non Open and operational tables.
  */
object SchemaUtils {

  /** Constructs schema and creates the Hive tables for all the table definitions in the table config.xml */
  def getBusinessTablesSchema(tableConfig: TableConfig, sriParams: SriParams): List[String] = {
    val tableName: String = tableConfig.name
    val sriOpenHdfsLocation: String = s"""${sriParams.hdfsBaseDir}${sriParams.sriOpenSchema}/$tableName"""
    val sriNonOpenHdfsLocation: String = s"""${sriParams.hdfsBaseDir}${sriParams.sriNonOpenSchema}/$tableName"""
    val verifyTypesHdfsLocation: String = s"""${sriParams.hdfsBaseDir}${sriParams.verifyTypesSchema}/$tableName"""
    val hiveColSchemaOrc: String = createHiveSchema(tableConfig)
    val hiveColSchemaAvro: String = createHiveSchema(tableConfig, isAvro = true)

    val sriOpenTable = getSriTableSchema(sriParams.sriOpenSchema, tableName, hiveColSchemaOrc,
      sriParams.sriOpenPartitionColumn, sriOpenHdfsLocation, sriParams.isCdcSource)

    val sriNonOpenTable = getSriTableSchema(sriParams.sriNonOpenSchema, tableName, hiveColSchemaOrc,
      sriParams.sriNonOpenPartitionColumn, sriNonOpenHdfsLocation, sriParams.isCdcSource)

    val verifyTypesTable = getVerifyTypesTableSchema(sriParams.verifyTypesSchema, tableName, hiveColSchemaAvro,
      sriParams.verifyTypesPartitionColumn, verifyTypesHdfsLocation, sriParams.isCdcSource)

    sriOpenTable ++ sriNonOpenTable ++ verifyTypesTable
  }

  /** Constructs the Hive table definition for an Sri Open/NonOpen table **/
  def getSriTableSchema(database: String, tableName: String, hiveColSchema: String, partitionColumn: String, hdfsLocation: String, cdcSource: Boolean): List[String] = {
    val cdcColumn = if (cdcSource) cdcColSchema else ""
    List(
      s"create schema if not exists $database",
      s"drop table if exists $database.$tableName",
      s"""
         |
         | create external table if not exists $database.$tableName(
         | ROWID string
         |, s_startdt string, s_starttime string, s_enddt string, s_endtime string, s_deleted_flag string
         | $cdcColumn$hiveColSchema
         |)
         | partitioned by($partitionColumn string)
         | stored as ORC
         | location '$hdfsLocation'""".stripMargin)
  }


  /** Constructs the Hive table definition for a Verify type/Storage table **/
  def getVerifyTypesTableSchema(database: String, tableName: String, hiveColSchema: String, partitionColumn: String,
                                hdfsLocation: String, cdcSource: Boolean): List[String] = {
    val cdcColumn = if (cdcSource) {
      cdcColSchema
    } else {
      ""
    }

    List(
      s"create schema if not exists $database",
      s"drop table if exists $database.$tableName",
      s"""create external table if not exists $database.$tableName(
         |   ROWID string, FILENAME string
         |   $cdcColumn$hiveColSchema
         |   )
         |partitioned by ($partitionColumn string)
         |stored as AVRO
         |location '$hdfsLocation'""".stripMargin)
  }

  /** Given a table config, constructs the column definition part of the Hive create table DDL **/
  def createHiveSchema(tableConfig: TableConfig, isAvro: Boolean = false): String = {
    val columns: Array[String] = tableConfig.colSchema.split("\\^")
    columns.map {
      col =>
        val strings = col.split(" ")
        val drop = strings.exists(_.equalsIgnoreCase("DROP"))

        drop match {
          case true => ""
          case false if !isAvro => "," + strings(0) + " " + strings(1)
          case false if isAvro => "," + strings(0) + " " + toAvroType(strings(1))
        }

    }.foldRight("") { (x, y) => x + y }
  }

  /** Constructs schema and creates the following Operational tables
    * 1. Warn Type Table
    *
    * 2. All Country Table
    *
    * 3. All Tab Columns
    *
    * 4. All Tables
    *
    * 5. XForm Failure
    *
    * 6. Data Dictionary
    *
    * 7. Row History Table
    *
    * 8. Row Counts Table
    *
    * 9. Recon Output Table
    *
    * 10. Edm Recon Table
    *
    * 11. Ops Data Table
    *
    * 12. Eod Table
    *
    * 13. Invalid Types Table
    *
    * 14. Config Table
    *
    * 15. Process Metadata Table
    *
    * 16. Duplicates Table
    */
  def getOpsSchema(sriParams: SriParams): List[String] = {

    val rowHistoryTable = getRowHistoryTable(sriParams.source, sriParams.country, sriParams.opsSchema, sriParams.hdfsBaseDir)
    val rowCountsTable = getRowCountsTable(sriParams.source, sriParams.country, sriParams.opsSchema, sriParams.hdfsBaseDir)
    val reconOutputTable = getReconOutputTable(sriParams.source, sriParams.country, sriParams.opsSchema, sriParams.reconOutputTableName, sriParams.hdfsBaseDir)
    val edmReconTable = createReconTable(sriParams.opsSchema, sriParams.source, sriParams.country, sriParams.hdfsBaseDir)
    val opsDataTable = getOpsDataTable(sriParams.source, sriParams.country, sriParams.opsSchema, sriParams.hdfsBaseDir)
    val eodTable = createEodTable(sriParams.opsSchema, sriParams.eodTableName, sriParams.hdfsBaseDir + sriParams.opsSchema + "/" + sriParams.eodTableName)
    val invalidTypesTable = getInvalidTypesTable(sriParams.source, sriParams.country, sriParams.verifyTypesSchema, sriParams.hdfsBaseDir, sriParams.verifyTypesPartitionColumn)
    val warnTypeTable = getWarnTypesTable(sriParams.source, sriParams.country, sriParams.verifyTypesSchema, sriParams.hdfsBaseDir, sriParams.verifyTypesPartitionColumn)
    val xFormFailure = getXformFailureTable(sriParams.source, sriParams.country, sriParams.verifyTypesSchema, sriParams.hdfsBaseDir, sriParams.verifyTypesPartitionColumn)
    val dataDictionary = createDataDictionary(sriParams.opsSchema, sriParams.hdfsBaseDir + sriParams.opsSchema + "/"
      + s"${sriParams.source}_${sriParams.country}_data_dictionary", s"${sriParams.source}_${sriParams.country}_data_dictionary")
    val configTable = createConfigHistoryTableSchema(sriParams.opsSchema, sriParams.source, sriParams.country, sriParams.hdfsBaseDir)
    val processMetadataTable = createProcessMetaData(sriParams.opsSchema, sriParams.source, sriParams.hdfsBaseDir)

    val duplicatesTable = createDuplicatesTables(sriParams.opsSchema, sriParams.source, sriParams.country, sriParams.hdfsBaseDir)
    val allCountryTable = createAllCountryTable(sriParams.metadataSchema, sriParams.hdfsBaseDir)
    val allTabColumns = createAllTabColumns(sriParams.metadataSchema, sriParams.hdfsBaseDir)
    val allTables = createAllTables(sriParams.metadataSchema, sriParams.hdfsBaseDir)

    warnTypeTable ++ allCountryTable ++ allTabColumns ++ allTables ++ xFormFailure ++
      dataDictionary ++ rowHistoryTable ++ rowCountsTable ++ reconOutputTable ++ edmReconTable ++ opsDataTable ++ eodTable ++ invalidTypesTable ++ configTable ++ processMetadataTable ++ duplicatesTable
  }

  /** Constructs the Hive table definition for config history **/
  private def createConfigHistoryTableSchema(opsSchemaName: String, source: String, country: String, hdfsLocation: String): List[String] = {
    List(
      s"create schema if not exists $opsSchemaName",
      s"drop table if exists $opsSchemaName.${source}_${country}_config_history",
      s"""create external table if not exists $opsSchemaName.${source}_${country}_config_history(
         |filename string COMMENT 'source config file path',
         |tablename string COMMENT 'source table',
         |sourcetype string COMMENT 'delta or transaction',
         |keycols string COMMENT 'source table key columns - comma-separated',
         |schema string COMMENT 'schema as a string',
         |asof string COMMENT 'date timestamp when the config extraction was executed')
         |stored as ORC
         |location '$hdfsLocation$opsSchemaName/${source}_${country}_config_history'""".stripMargin)
  }

  /** Constructs the Hive table definition for process metadata **/
  private def createProcessMetaData(opsSchemaName: String, source: String, hdfsLocation: String): List[String] = {
    List(
      s"create schema if not exists $opsSchemaName",
      s"drop table if exists $opsSchemaName.${source}_process_metadata",
      s""" CREATE EXTERNAL TABLE if not exists $opsSchemaName.${source}_process_metadata (
         |  meta_time timestamp,
         |  partition_name string,
         |  table_name string,
         |  status string,
         |  description string,
         |  execution_time string,
         |  action_name string,
         |  file_name string,
         |  run_type string,
         |  batch_date string,
         |  json_attribute string,
         |  source_name string,
         |  country_name string,
         |  module_name string
         |)PARTITIONED BY (partition_date string )
         |  STORED AS ORC
         |  LOCATION  '$hdfsLocation/$opsSchemaName/${source}_process_metadata'""".stripMargin)
  }

  private def createDuplicatesTables(opsSchemaName: String, source: String, country: String, hdfsLocation: String): List[String] = {
    List(
      s"create schema if not exists $opsSchemaName",
      s"drop table if exists $opsSchemaName.${source}_${country}_duplicates",
      s"""CREATE EXTERNAL TABLE if not exists $opsSchemaName.${source}_${country}_duplicates(
         |   ROWID string,
         |   DUPLICATE_ROWID string,
         |   STEPNAME string,
         |   TABLENAME string,
         |   asof string
         |)
         |partitioned by (dds string)
         |STORED as ORC
         |location '$hdfsLocation$opsSchemaName/${source}_${country}_duplicates'""".stripMargin)
  }

  /** Constructs the Hive table definition for recon table **/
  private def createReconTable(opsSchemaName: String, source: String, country: String, hdfsLocation: String): List[String] = {
    List(
      s"create schema if not exists $opsSchemaName",
      s"drop table if exists $opsSchemaName.${source}_${country}_recon",
      s"""CREATE EXTERNAL TABLE if not exists $opsSchemaName.${source}_${country}_Recon(
         |     filename STRING,
         |       tableName STRING,
         |       checksum_position STRING,
         |       checksum_value STRING,
         |       file_record_count STRING,
         |       tbusiness_date STRING,
         |       run_date STRING,
         |       file_date STRING,
         |       dummy_file STRING,
         |       checksum_delimiter STRING,
         |       noofchecksum String,
         |       file_type String,
         |       meta_time String)
         |       PARTITIONED BY (edmp_partitiondate String)
         |STORED as ORC
         |location '$hdfsLocation$opsSchemaName/${source}_${country}_recon'""".stripMargin)
  }

  /** Constructs the Hive table definition for all tables **/
  private def createAllTables(commonMetadataSchema: String, hdfsRootDir: String): List[String] = {
    List(
      s"create schema if not exists $commonMetadataSchema",
      s"drop table if exists $commonMetadataSchema.scb_all_tables",
      s""" create external table if not exists $commonMetadataSchema.scb_all_tables(
         |    country_name string ,table_name string, source string ,
         |    source_type string , threshold_val string, user string,
         |    server_id string,
         |    port string,
         |    other_info string,
         |    current_version int,
         |    version int,
         |    valid string,
         |    valid_from string,
         |    valid_to string,
         |    creation_date string,
         |    last_update_date string,
         |    new_line_char string
         |)
         |stored as ORC
         |location '$hdfsRootDir$commonMetadataSchema/scb_all_tables'""".stripMargin)
  }

  /** Constructs the Hive table definition for All table columns **/
  private def createAllTabColumns(commonMetadataSchema: String, hdfsRootDir: String): List[String] = {
    List(
      s"create schema if not exists $commonMetadataSchema",
      s"drop table if exists $commonMetadataSchema.scb_all_tab_columns",
      s"""create external table if not exists $commonMetadataSchema.scb_all_tab_columns(
         |    table_name string ,
         |    column_name string,
         |    country_name string ,
         |    source string ,
         |    data_type string,
         |    data_length int,
         |    data_precision int,
         |    primary_column_indicator string,
         |    nullable string,
         |    column_order int,
         |    current_version int,
         |    version int,
         |    width string,
         |    valid string,
         |    valid_from string,
         |    valid_to string,
         |    creation_date string,
         |    last_update_date string,
         |    comment string
         |)
         |stored as ORC
         |location '$hdfsRootDir$commonMetadataSchema/scb_all_tab_columns'""".stripMargin)
  }

  /** Constructs the Hive table definition for all country  **/
  private def createAllCountryTable(commonMetadataSchema: String, hdfsLocation: String): List[String] = {
    List(
      s"create schema if not exists $commonMetadataSchema",
      s"drop table if exists $commonMetadataSchema.scb_all_countries",
      s"""create external table if not exists $commonMetadataSchema.scb_all_countries(
         |    source string ,
         |    country_name string,
         |    data_path string ,
         |    version string ,
         |    source_group string,
         |    threshold_val string,
         |    cut_off_hour int,
         |    retention_period_hdfsfile int,
         |    retention_period_edge_node int,
         |    retention_period_ops int,
         |    retention_period_backup int,
         |    retention_period_eod_data_file string,
         |    time_zone_value string
         |)
         |stored as ORC
         |location '$hdfsLocation$commonMetadataSchema/scb_all_countries'""".stripMargin)

  }

  /** Constructs the Hive table definition for data dictionary **/
  private def createDataDictionary(opsSchema: String, hdfsLocation: String, name: String): List[String] = {
    List(
      s"create schema if not exists $opsSchema",
      s"drop table if exists $opsSchema.$name",
      s"""
         |
        | create external table  if not exists $opsSchema.$name(
         | tablename string,
         | colname string,
         | coltype string,
         | colnull string,
         | commentstr string,
         | asof string
         |)
         | stored as ORC
         | location '$hdfsLocation'""".stripMargin)

  }

  /** Constructs the Hive table definition for EOD table **/
  private def createEodTable(database: String, tableName: String, hdfsLocation: String): List[String] = {
    List(
      s"create schema if not exists $database",
      s"drop table if exists $database.$tableName",
      s"""
         | create external table if not exists $database.$tableName(
         | source string, country string, markerTime string, previousBusinessDate string
         |)
         | partitioned by(businessDate string)
         | stored as ORC
         | location '$hdfsLocation'""".stripMargin)
  }

  /** Constructs the Hive table definition for row counts **/
  private def getRowCountsTable(source: String, country: String, opsSchemaName: String, hdfsRootDir: String): List[String] = {
    val rowCountsTable: String = s"${source}_${country}_rowcounts"
    List(
      s"create schema if not exists $opsSchemaName",
      s"drop table if exists $opsSchemaName.$rowCountsTable",
      s"""create external table   if not exists $opsSchemaName.$rowCountsTable(
         | schemaname string COMMENT 'schema (or database) ',
         | tablename string COMMENT 'physical table that is the source of rowcount computation',
         | filename string COMMENT 'source file name',
         | rowcount bigint ,
         | functionaltable string COMMENT 'logical table whose data is embedded in the physical table and that forms the granularity of rowcounts',
         | asof string COMMENT 'date timestamp at which the row count entry was made',
         | step_name string COMMENT 'processing step that populated this row',
         | attempt_id string COMMENT 'job attempt that executed this step' )
         | partitioned by(rcds string)
         | stored as ORC
         | location '$hdfsRootDir$opsSchemaName/$rowCountsTable'""".stripMargin)
  }

  /** Constructs the Hive table definition for Ops metadata **/
  private def getOpsDataTable(source: String, country: String, opsSchemaName: String, hdfsRootDir: String): List[String] = {
    val opsTableName: String = s"""${source}_${country}_ops_data"""
    List(
      s"create schema if not exists $opsSchemaName",
      s"drop table if exists $opsSchemaName.$opsTableName",
      s"""
         |  create external table if not exists $opsSchemaName.$opsTableName(
         |   event_time string COMMENT 'time at which an event occurred',
         |   event_message string COMMENT 'message describing a given event'
         |   )
         |partitioned by (event_date string)
         |stored as ORC
         |location '$hdfsRootDir$opsSchemaName/$opsTableName'""".stripMargin)

  }

  /** Constructs the Hive table definition for Row history table **/
  private def getRowHistoryTable(source: String, country: String, opsSchemaName: String, hdfsRootDir: String): List[String] = {
    val rowHistoryTable: String = s"""${source}_${country}_rowhistory"""
    List(
      s"create schema if not exists $opsSchemaName",
      s"drop table if exists $opsSchemaName.$rowHistoryTable",
      s"""
         |CREATE EXTERNAL TABLE $opsSchemaName.$rowHistoryTable(
         |   ROWID string,
         |   FILENAME string
         |)
         | partitioned by (vds string)
         |STORED as AVRO
         |location '$hdfsRootDir$opsSchemaName/$rowHistoryTable'""".stripMargin)
  }

  /** Constructs the Hive table definition for Invalid types **/
  private def getInvalidTypesTable(source: String, country: String, storageSchemaName: String, hdfsRootDir: String, partitionColumn: String): List[String] = {

    val invalidTypesTable: String = s"""${source}_${country}_invalidtypes"""
    List(
      s"create schema if not exists $storageSchemaName",
      s"drop table if exists $storageSchemaName.$invalidTypesTable",
      s"""
         |create external table if not exists $storageSchemaName.$invalidTypesTable(
         |      source string,country string,tablename string,rowid string,data string,errormsg string, partitioncolumn string, ts string
         |  )
         |  partitioned by ($partitionColumn string)
         |  stored as AVRO
         |  location '$hdfsRootDir$storageSchemaName/$invalidTypesTable'""".stripMargin)
  }

  /** Constructs the Hive table definition for Warn types **/
  private def getWarnTypesTable(source: String, country: String, storageSchemaName: String, hdfsRootDir: String, partitionColumn: String): List[String] = {

    val warnTypesTable: String = s"${source}_${country}_warntypes"
    List(
      s"create schema if not exists $storageSchemaName",
      s"drop table if exists $storageSchemaName.$warnTypesTable",
      s"""create external table if not exists $storageSchemaName.$warnTypesTable(
         |      source string,country string,tablename string,rowid string,data string,errormsg string, partitioncolumn string, ts string
         |  )
         |  partitioned by ($partitionColumn string)
         |  stored as AVRO
         |  location '$hdfsRootDir$storageSchemaName/$warnTypesTable'""".stripMargin)

  }

  /** Constructs the Hive table definition for transformation failure table **/
  private def getXformFailureTable(source: String, country: String, storageSchemaName: String, hdfsRootDir: String, partitionColumn: String): List[String] = {

    val xformFailureTable: String = s"${source}_${country}_xform_failure"
    List(
      s"create schema if not exists $storageSchemaName",
      s"drop table if exists $storageSchemaName.$xformFailureTable",
      s"""create external table if not exists $storageSchemaName.$xformFailureTable(
         |      source string,country string,tablename string,rowid string,data string,errormsg string, partitioncolumn string, ts string
         |  )
         |  partitioned by ($partitionColumn string)
         |  stored as AVRO
         |  location '$hdfsRootDir$storageSchemaName/$xformFailureTable'""".stripMargin)

  }

  /** Constructs the Hive table definition for Recon output table **/
  private def getReconOutputTable(source: String, country: String, opsSchemaName: String, reconOutputTableName: String, hdfsRootDir: String): List[String] = {
    List(
      s"create schema if not exists $opsSchemaName",
      s"""
         create external table if not exists $opsSchemaName.$reconOutputTableName(
         |   source_name string,
         |   country_name string,
         |   source_type string,
         |   table_name string,
         |   source_count decimal(32,0),
         | 	 target_count decimal(32,0),
         |   chksum_field_name string,
         |   source_chksum float,
         |   target_chksum float,
         |   count_recon_status string,
         |   chksum_recon_status string,
         |   insert_time timestamp
         |   )
         |partitioned by (batch_date string)
         |stored as ORC
         |location '$hdfsRootDir$opsSchemaName/$reconOutputTableName'""".stripMargin)
  }

  /** Constructs data for the following metadata tables and stores them. This is called for the first installation and for subsequent SchemaEvolution
    *
    * 1. DataDictionary
    *
    * 2. Config History
    *
    * 3. All tables
    *
    * 4. All Table columns
    *
    * 5. All country
    *
    */
  def populateCommonTables(tableConfigs: Seq[TableConfig], sqlContext: SQLContext, sriParams: SriParams, fetchFileSystem: FileSystem): Unit = {
    val dictionary = ListBuffer[DataDictionary]()
    val configs = ListBuffer[ConfigHistory]()
    val allTables = ListBuffer[AllTables]()
    val allTabColumns = ListBuffer[AllTabColumn]()
    val allCountry = List(AllCountry(sriParams.source, sriParams.country, "NULL", "1", "NULL", "NULL", 0, 0, 0, 0, 0, "", ""))
    val currentTime = SdmConstants.nowAsString()

    tableConfigs.foreach {
      tableConfig =>
        val colConfig = fetchColumnConfig(tableConfig)
        val columns = colConfig.map(_.name)
        val dataDictionary = colConfig.map(eachCol => DataDictionary(tableConfig.name, eachCol.name, eachCol.typee, eachCol.nullable.toString.capitalize, "", new Date().toString))
        val allTabColumn = getAllTabColumns(currentTime, sriParams.source, sriParams.country, tableConfig)

        val configHistory = List(ConfigHistory("", tableConfig.name.replace(s"${sriParams.source}_${sriParams.country}_", ""), sriParams.sourcingType.toString,
          columns.mkString(","), tableConfig.colSchema, currentTime))

        val scbAllTables = List(AllTables(sriParams.country, tableConfig.name.replace(s"${sriParams.source}_${sriParams.country}_", ""), sriParams.source,
          tableConfig.sourceType, "0p", "", "", "", "", 1, 1, "Y", currentTime, "2099-12-31 00:00:00", currentTime, currentTime, "Y"))

        dictionary ++= dataDictionary
        configs ++= configHistory
        allTables ++= scbAllTables
        allTabColumns ++= allTabColumn
    }

    fetchFileSystem.delete(new Path(s"hdfs://${sriParams.hdfsBaseDir}${sriParams.opsSchema}/${sriParams.source}_${sriParams.country}_data_dictionary/${sriParams.source}_${sriParams.country}_data_dictionary"), true)
    fetchFileSystem.delete(new Path(s"hdfs://${sriParams.hdfsBaseDir}${sriParams.metadataSchema}/scb_all_tables/${sriParams.source}_${sriParams.country}_scb_all_tables"), true)
    fetchFileSystem.delete(new Path(s"hdfs://${sriParams.hdfsBaseDir}${sriParams.metadataSchema}/scb_all_countries/${sriParams.source}_${sriParams.country}_scb_all_countries"), true)
    fetchFileSystem.delete(new Path(s"hdfs://${sriParams.hdfsBaseDir}${sriParams.opsSchema}/${sriParams.source}_${sriParams.country}_config_history/${sriParams.source}_${sriParams.country}_config_history"), true)
    fetchFileSystem.delete(new Path(s"hdfs://${sriParams.hdfsBaseDir}${sriParams.metadataSchema}/scb_all_tab_columns/${sriParams.source}_${sriParams.country}_scb_all_tab_columns"), true)
    sqlContext.createDataFrame(allCountry).write.format("orc").save(s"hdfs://${sriParams.hdfsBaseDir}${sriParams.metadataSchema}/scb_all_countries/${sriParams.source}_${sriParams.country}_scb_all_countries")
    sqlContext.createDataFrame(dictionary).write.format("orc").save(s"hdfs://${sriParams.hdfsBaseDir}${sriParams.opsSchema}/${sriParams.source}_${sriParams.country}_data_dictionary/${sriParams.source}_${sriParams.country}_data_dictionary")
    sqlContext.createDataFrame(allTables).write.format("orc").save(s"hdfs://${sriParams.hdfsBaseDir}${sriParams.metadataSchema}/scb_all_tables/${sriParams.source}_${sriParams.country}_scb_all_tables")
    sqlContext.createDataFrame(configs).write.format("orc").save(s"hdfs://${sriParams.hdfsBaseDir}${sriParams.opsSchema}/${sriParams.source}_${sriParams.country}_config_history/${sriParams.source}_${sriParams.country}_config_history")
    sqlContext.createDataFrame(allTabColumns).write.format("orc").save(s"hdfs://${sriParams.hdfsBaseDir}${sriParams.metadataSchema}/scb_all_tab_columns/${sriParams.source}_${sriParams.country}_scb_all_tab_columns")

  }


  /** Mapping of table config datatypes to Avro datatypes */
  private val toAvroType: String => String = {
    case "BOOLEAN" | "SHORT" => "boolean"
    case "INT" => "int"
    case "LONG" | "BIGINT" => "long"
    case "FLOAT" => "float"
    case typee@x if List("DOUBLE").exists(typee.contains) => "double"
    case typee@x if List("DECIMAL", "CHAR", "VARCHAR", "STRING", "DATE", "TIMESTAMP").exists(typee.contains) => "string"
    case typee => throw new IllegalArgumentException(s"Invalid type: $typee")
  }

  /** CDC Column schema in the same format as the column schema in the Table config xml. **/
  //FIXME Source this from the systemColSchema
  private val cdcColSchema = ",c_journaltime STRING,c_transactionid string,c_operationtype string,c_userid string"


  /** Object representation of all tables stored for a country stored in the AllTables table */
  case class AllTables(country: String, tableName: String, source: String, sourceType: String, threshold: String, user: String, serverId: String, port: String, info: String, currentVersion: Int
                               , version: Int, valid: String, validFrom: String, validTo: String, createDate: String, lastUpdateDate: String, newLineChar: String)

  /** Object representation of the table config stored in Config history table */
  case class ConfigHistory(fileName: String, tableName: String, sourceType: String, keyColumns: String, schema: String, asOf: String)

  /** Object representation of an entry of source and country made into the AllCountry table */
  private case class AllCountry(source: String, country: String, dataPath: String, version: String, sourceGroup: String, thresholdVal: String, cutOffHour: Int,
                                retPeriodHdfsfile: Int, retPeriodEdgeNode: Int, retPeriodOps: Int, retPeriodBackup: Int, retPeriodEodDataFile: String, timeZoneValue: String)

  /** Object representation table metadata instance stored in the data dictionary table */
  private case class DataDictionary(tablename: String, colname: String, coltype: String, colnull: String, commentstr: String, asof: String)

}