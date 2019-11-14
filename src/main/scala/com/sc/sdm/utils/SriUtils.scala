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

import java.lang.Math.{abs, min}

import com.sc.sdm.models.SriParams
import com.sc.sdm.utils.FileSystemUtils.fetchFileSystem
import com.sc.sdm.utils.PartitionUtils._
import com.sc.sdm.utils.SdmConstants._
import com.sc.sdm.utils.XmlParser.TableConfig
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, FileSystem, Path, PathFilter}
import org.apache.log4j.Logger
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.joda.time.Days

import scala.util.Try

//TODO Most of the functions of this class have to refactored and removed to be sourced from configuration file
/**
  * Holder of Utility methods for the Sri Layer
  */
object SriUtils {
  val logger: Logger = Logger.getLogger(SriUtils.getClass)

  //TODO Change the implementation to source from the Audit columns instead of hard coded columns in this class
  /**
    * Returns the Spark [[StructType]] of a given table for SRI layer
    *
    * @param tableConfig     Table configuration of the table for which the structure is required
    * @param partitionColumn Name of the partition column
    * @param cdcSource       Indication whether the source system is CDC based or not
    * @return Table structure as a [[StructType]]
    */
  def fetchSriStructure(tableConfig: TableConfig, partitionColumn: String, cdcSource: Boolean): StructType = {
    StructType(Seq(StructField("rowid", StringType)) ++
      getSriTimeColumnsSchema ++
      (if (cdcSource) cdcColumnSchema else Seq()) ++
      TypeUtils.transformStructFields(tableConfig) ++ Seq(StructField(partitionColumn, StringType)))
  }

  /**
    * Returns the Spark [[StructType]] of a given table for Storage/Verify types layer
    *
    * @param tableConfig     Table configuration of the table for which the structure is required
    * @param partitionColumn Name of the partition column
    * @param cdcSource       Indication whether the source system is CDC based or not
    * @return Table structure as a [[StructType]]
    */
  def fetchVerifyStructure(tableConfig: TableConfig, partitionColumn: String, cdcSource: Boolean): StructType = {
    StructType(Seq(StructField("rowid", StringType), StructField("filename", StringType)) ++
      Seq(StructField(partitionColumn, StringType)) ++
      (if (cdcSource) {
        cdcColumnSchema
      } else {
        Seq()
      })
      ++
      TypeUtils.transformStructFieldsAvro(tableConfig)
        .map(x => StructField(x.name.toLowerCase, x.dataType, x.nullable, x.metadata)))
  }

  //TODO Find out the purpose of this method and why Append write mode doesn't work for EOD table

  /**
    * Moves the HDFS files from source to destination. Also adds the hive partition for that table
    *
    * @param sourcePathString    Source HDFS path
    * @param destPathString      Destination HDFS path
    * @param spark               Spark Session
    * @param fileNamePrefix      If the target file name should be renamed, this parameter can be used. Default is an empty string
    * @param deletePreviousFiles Flag whether the previous files in the target directory must be deleted before copying these files
    */
  def moveHdfsFiles(sourcePathString: String, destPathString: String, spark: SparkSession, fileNamePrefix: String = "", deletePreviousFiles: Boolean = true): Unit = {
    logger.info("Moving Hdfs files from " + sourcePathString + " to " + destPathString)
    val sourcePath = new Path(sourcePathString)
    val destinationPath = new Path(destPathString)

    val fs: FileSystem = FileSystemUtils.fetchFileSystem
    logger.info(" default FS is " + fs.getConf.get("fs.defaultFS"))
    val database = sourcePath.getParent.getName
    val tableName = destinationPath.getName

    fs.deleteOnExit(sourcePath)

    if (fs.exists(sourcePath)) {
      fs.listStatus(sourcePath).filter(_.isDirectory).foreach {
        level1 =>
          val partitionDir: String = level1.getPath.getName
          val dstPartition: Path = new Path(destinationPath.toString + "/" + partitionDir + "/")
          if (fs.exists(dstPartition) && deletePreviousFiles) {
            fs.delete(dstPartition, true)
          }
          val fileList: Array[FileStatus] = fs.listStatus(level1.getPath).filterNot(_.getPath.getName.startsWith("_")).filterNot(_.getPath.getName.startsWith("."))
          if (fileList.nonEmpty) {
            val split: Array[String] = partitionDir.split("=")
            val partitionName = split(0) + "='" + split(1) + "'"
            createHivePartition(database, tableName, partitionName)(spark)
          }

          fileList.foreach {
            name =>
              val path1: String = sourcePath.toString + "/" + level1.getPath.getName + "/" + name.getPath.getName
              val path2: String = destinationPath.toString + "/" + level1.getPath.getName + "/" + fileNamePrefix + name.getPath.getName
              logger.info("Moving path : " + path1 + "; to " + path2)
              val path: Path = new Path(path2)
              fs.mkdirs(path.getParent)
              if (!fs.rename(new Path(path1), path)) throw new IllegalStateException(s"Application failed renaming from $sourcePath to $destinationPath")
          }
      }
    }
  }

  /**
    * Given a database, tablename and partition column, creates and executes the `ALTER TABLE ADD PARTITION` for that table partition
    *
    * @param database      Database of the tablename
    * @param tableName     Table name on top of which the partition is to be created
    * @param partitionName Name of the partition to be created
    * @param spark         SparkSession
    * @return
    */
  def createHivePartition(database: String, tableName: String, partitionName: String)(implicit spark: SparkSession) = {
    val sqlQuery: String = s"alter table $database." + tableName + " add if not exists partition(" + partitionName + ")"
    logger.info("Executing Partition query (database: " + database + ") - " + sqlQuery)
    spark.synchronized {
      spark.sql(sqlQuery)
    }
  }

  /**
    * Recursively checks if any ORC file exists in the input path
    *
    * @param path Input path
    * @return
    */
  def doesPathExist(path: String): Boolean = {

    val fs: FileSystem = fetchFileSystem
    fs.exists(new Path(path).getParent) && fs.listStatus(new Path(path).getParent, new PathFilter {
      override def accept(path: Path): Boolean = {
        path.getName.matches("(.*)orc$")
      }
    }).nonEmpty
  }

  /** Returns the Previous existing partition for a table */
  def getPreviousExistingSriPartition(sriParams: SriParams, businessDate: String, tableName: String, limit: Int): String = {
    val previousBusinessDay: String = getPreviousBusinessDatePartition(businessDate, PartitionFormatString, "days", 1)
    val path: String = sriParams.getSriOpenPartitionPath(tableName, previousBusinessDay) + "*.orc"
    if (doesPathExist(path) || limit == 0) {
      previousBusinessDay
    }
    else {
      getPreviousExistingSriPartition(sriParams, previousBusinessDay, tableName, limit - 1)
    }
  }

  /** Returns the Next existing partition for a table if exists */
  def getNextExistingSriPartition(sriParams: SriParams, businessDate: String, tableName: String, limit: Int): Option[String] = {
    val nextBusinessDay: String = getNextBusinessDatePartition(businessDate, PartitionFormatString, "days", 1)
    val path: String = sriParams.getSriOpenPartitionPath(tableName, nextBusinessDay) + "*.orc"

    if (doesPathExist(path)) Some(nextBusinessDay)
    else if (limit == 0) None
    else {
      getNextExistingSriPartition(sriParams, nextBusinessDay, tableName, limit - 1)
    }
  }

  /**
    * Prepares a list of HDFS locations which contain storage inputs for the given batch
    */
  private def getExistingVerifyTypesPartitions(sriParams: SriParams, fs: FileSystem, tableName: String, allPartitions: List[String]): List[String] = {

    def doesVerifyTypesInputExist(fs: FileSystem, verifyTypesPath: String, tableName: String): Boolean = {
      val inputPath = new Path(verifyTypesPath)
      if (fs.exists(inputPath) && fs.isDirectory(inputPath)) {
        fs.listStatus(inputPath).nonEmpty
      } else {
        fs.exists(inputPath)
      }
    }

    allPartitions
      .dropRight(1)
      .map(sriParams.getVerifyTypesPartitionPath(tableName, _))
      .filter(path => doesVerifyTypesInputExist(fs, path, tableName))
  }

  /**
    * Given a HDFS path to a Hadoop configuration file, returns a [[Configuration]] object
    *
    * @param fs
    * @param xmlPath
    * @return
    */
  def getParamConf(fs: FileSystem, xmlPath: String): Configuration = {
    val xmlHdfs = new Path(xmlPath)
    val paramConf: Configuration = new Configuration()
    paramConf.addResource(fs.open(xmlHdfs), xmlPath)
    paramConf
  }

  /**
    * Given a [[TableConfig]] and [[SriParams]], returns a [[PreviousPartitionInfo]] object that encapsulates the Previous SRI location and the Previous Business date
    *
    * @param sriParams
    * @param fs
    * @param tableConfig
    * @param inputFormat
    * @return instance of [[PreviousPartitionInfo]]
    */
  def getInputFiles(sriParams: SriParams, fs: FileSystem, tableConfig: TableConfig)(implicit inputFormat: String = "orc"): PreviousPartitionInfo = {
    val tableName = tableConfig.name
    val previousPartition = getPreviousExistingSriPartition(sriParams, sriParams.businessDate, tableName, 10)
    logger.info("Detected previous business date as " + previousPartition + " for table " + tableConfig.name)
    val previousInStorageFormat = previousPartition
    val maxDaysToIterate = min(250, abs(Days.daysBetween(PartitionFormat.parseDateTime(sriParams.batchPartition), PartitionFormat.parseDateTime(previousInStorageFormat)).getDays))

    val allPartitions = sriParams.batchPartition :: PartitionUtils.allPartitions(sriParams.batchPartition, PartitionFormatString, "days", 1, maxDaysToIterate)
    logger.info(s"$tableName seeking from " + sriParams.getVerifyTypesPartitionPath(tableName, "X") + " from list" + allPartitions.mkString("|"))
    val verifyTypesDirList = getExistingVerifyTypesPartitions(sriParams, fs, tableConfig.name, allPartitions)

    val sriOpenPrevPartitionPath: String = sriParams.getSriOpenPartitionPath(tableName, previousPartition)
    logger.info(" looking for storage files in " + verifyTypesDirList.mkString(","))
    PreviousPartitionInfo(sriOpenPrevPartitionPath, allPartitions.last)
  }

  /**
    * Parses the submit time and extracts partition information from it
    *
    * @param submitTime
    * @return
    */
  def getBusinessDayPartition(submitTime: String, timestampColFormat: String): Option[String] = {
    val businessDay = Try(formatter(timestampColFormat) parseDateTime submitTime).toOption
    businessDay.map(PartitionFormat.print)
  }

  /**
    * Given a [[org.apache.spark.sql.DataFrame]], the table it corresponds to and the partition, this function persists the data to HDFS and creates a Hive partition on top of the table

    * @param dataFrame DataFrame that needs to be written
    * @param path HDFS Target path
    * @param database Name of the Hive Database
    * @param tableName Name of the Hive Table
    * @param partitionName Partition column name of the table
    * @param partitionValue Partition value
    * @param saveMode Whether the existing files in the partition needs to be overwritten or appended to
    * @param spark Spark session
    * @return
    */
  def persistAsHiveTable(dataFrame: DataFrame, path: String, database: String, tableName: String, partitionName: String, partitionValue: String, saveMode: SaveMode = SaveMode.Overwrite)(implicit spark: SparkSession) = {
    val partition = s"$partitionName=$partitionValue"
    val partitionQuoted = s"""$partitionName='$partitionValue'"""
    dataFrame
      .write
      .option("orc.compress", "SNAPPY")
      .option("orc.stripe.size", "104857")
      .option("orc.compress.size", "8000")
      .format("orc")
      .mode(saveMode)
      .save(s"$path/$partition")

    createHivePartition(database, tableName, partitionQuoted)
  }

  /** Returns the CDC columns in the form of Seq [[org.apache.spark.sql.types.StructField]]. */
  private def cdcColumnSchema: Seq[StructField] = {
    Seq(StructField("c_journaltime", StringType),
      StructField("c_transactionid", StringType),
      StructField("c_operationtype", StringType),
      StructField("c_userid", StringType))
  }

  /** Returns the additional columns (SCD columns) that are added to the SRI in the form of Seq [[org.apache.spark.sql.types.StructField]]. */
  private def getSriTimeColumnsSchema = {
    Seq(
      StructField("start_date", StringType),
      StructField("start_time", StringType),
      StructField("end_date", StringType),
      StructField("end_time", StringType),
      StructField("del_flag", StringType)
    )
  }

  /** A simple wrapper over the information about the Previous partition */
  case class PreviousPartitionInfo(sriOpenPrevPartitionPath: String, previousBusinessDate: String)

}
