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

package com.sc.sdm.models

import java.util.UUID

import com.sc.sdm.utils.Enums.SourceType.SourceType
import com.sc.sdm.utils.Enums.{RunType, SourceType}
import com.sc.sdm.utils.SdmConstants._
import org.apache.hadoop.conf.Configuration
import org.joda.time.DateTime

import scala.util.{Failure, Success, Try}

//FIXME Source this from pureconfig
/** Scala binding for param.xml **/
case class SriParams(source: String, country: String, hdfsBaseDir: String,
                     sriOpenSchema: String, sriNonOpenSchema: String, verifyTypesSchema: String, opsSchema: String,
                     sriOpenPartitionColumn: String, sriNonOpenPartitionColumn: String, verifyTypesPartitionColumn: String,
                     hdfsTmpDir: String, tableConfigXml: String, reconTableName: String, reconTableNameColumn: Int, reconCountColumn: String,
                     fullDump: Boolean, businessDate: String, batchPartition: String,
                     eodMarker: String, sourcingType: SourceType,
                     eodTableName: String, reconOutputTableName: String, metadataSchema: String, recordTypeBatch: String,
                     dateColFormat: String,
                     timestampCol: String, timestampColFormat: String,
                     vTypesAuditColSchema: String, sriAuditColSchema: String, systemAuditColSchema: String,
                     parallelExecution: Boolean = true, isTableLevelEod: Boolean = false,
                     reconSourceQuery: String = "") {

  def getSriOpenPartitionPath(tableName: String, partition: String): String = {
    getSriOpenPath(tableName) + sriOpenPartitionColumn + "=" + partition + "/"
  }


  def getSriOpenPath(tableName: String): String = {
    hdfsBaseDir + sriOpenSchema + "/" + tableName + "/"
  }

  def getSriOpenTmpPath(tableName: String): String = {
    hdfsTmpDir + "/" + sriOpenSchema + "/" + tableName + "/"
  }

  def getSriNonOpenPartitionPath(tableName: String, partition: String): String = {
    getSriNonOpenPath(tableName) + sriNonOpenPartitionColumn + "=" + partition + "/"
  }

  def getSriNonOpenPath(tableName: String): String = {
    hdfsBaseDir + sriNonOpenSchema + "/" + tableName + "/"
  }

  def getSriNonOpenTmpPath(tableName: String): String = {
    hdfsTmpDir + "/" + sriNonOpenSchema + "/" + tableName + "/"
  }

  def getVerifyTypesPartitionPath(tableName: String, partition: String): String = {
    getVerifyTypesPath(tableName) + verifyTypesPartitionColumn + "=" + partition + "/"
  }

  def getVerifyTypesPath(tableName: String): String = {
    hdfsBaseDir + verifyTypesSchema + "/" + tableName + "/"
  }

  def getVerifyTypesTmpPath(tableName: String): String = {
    hdfsTmpDir + "/" + verifyTypesSchema + "/" + tableName + "/"
  }

  def isCdcSource: Boolean = {
    sourcingType == SourceType.CDC
  }

  def isRecordTypeBatch: Boolean = {
    isBatchSource && recordTypeBatch.equalsIgnoreCase("Y")
  }

  def isBatchSource: Boolean = {
    (sourcingType == SourceType.BATCH_FIXEDWIDTH) || (sourcingType == SourceType.BATCH_DELIMITED)
  }

  def businessDt: DateTime = {
    Try(BusinessDateFormat.parseDateTime(businessDate)) match {
      case Success(bd) => bd
      case Failure(throwable) =>
        //Allow it to blow up if both formats doesn't work
        PartitionFormat.parseDateTime(businessDate)
    }
  }
}

/** Factory for SriParams **/
object SriParams {
  def apply(paramConf: Configuration): SriParams = {
    apply(paramConf, "", "", "", "")
  }

  def apply(paramConf: Configuration, partition: String, businessDate: String, runType: String, eodMarker: String): SriParams = {

    val country: String = paramConf.get("edmhdpif.config.country")
    val source: String = paramConf.get("edmhdpif.config.source")
    val hdfsDataParentDir = paramConf.get("edmhdpif.config.hdfs.data.parent.dir") + "/"
    val sriOpenSchema = paramConf.get("edmhdpif.config.sri.open.schema")
    val sriNonOpenSchema = paramConf.get("edmhdpif.config.sri.nonopen.schema")
    val verifyTypesSchema = paramConf.get("edmhdpif.config.storage.schema")
    val opsSchema: String = paramConf.get("edmhdpif.config.ops.schema")

    val sriOpenPartitionName = paramConf.get("edmhdpif.config.sri.open.partition.name", "ods")
    val sriNonOpenPartitionName = paramConf.get("edmhdpif.config.sri.nonopen.partition.name", "nds")
    val verifyTypesPartitionName = paramConf.get("edmhdpif.config.storage.partition.name", "vds")

    val dateColFormat: String = paramConf.get("edmhdpif.config.datecolformat")
    val timestampCol: String = paramConf.get("edmhdpif.config.timestampcol")
    val timestampColFormat: String = paramConf.get("edmhdpif.config.timestampcolformat")

    val hdfsTmpDir = paramConf.get("edmhdpif.config.hdfs.tmpdir", "/tmp") + "/" + UUID.randomUUID.toString
    val tableConfigXml = paramConf.get("edmhdpif.config.hdfs.tableconfig.xml.path")

    val reconTable = paramConf.get("edmhdpif.config.recon.tablename", "")
    val reconTableNameColNum = paramConf.getInt("edmhdpif.config.recon.tablename.colnum", -1)
    val reconTableCountColNum = paramConf.get("edmhdpif.config.recon.tablecount.colnum", "-1")

    val recordTypeBatch = paramConf.get("edmhdpif.config.batch.recordtype", "N").trim

    val vTypesAuditColSchema = paramConf.get("edmhdpif.config.vtypes.auditcolschema", "")
    val sriAuditColSchema = paramConf.get("edmhdpif.config.sri.auditcolschema", "")
    val systemAuditColSchema = paramConf.get("edmhdpif.config.system.auditcolschema", "")

    val fullDump = if (runType.trim.compareToIgnoreCase("fulldump") == 0) true else false
    //FIXME convert the above line to type
    val runTypeEnum = RunType.fromString(runType)

    val sourcingType = SourceType.fromString(paramConf.get("edmhdpif.config.sourcing.type")).getOrElse(SourceType.CDC)

    val eodTableName = paramConf.get("edmhdpif.config.eod.table.name", "eod_table")
    val parallelExecution = paramConf.get("edmhdpif.config.parallel.execution", "true").toBoolean

    val isTableLevelEod = paramConf.get("edmhdpif.config.eod.marker.strategy", "global_eod").equals(TableLevelEod)

    val reconSourceQuery = paramConf.get("edmhdpif.config.recon.source.query", "")

    val reconOutputTableName = paramConf.get("edmhdpif.config.recon.output.tablename", "recon_output")
    val metadataTableName = paramConf.get("edmhdpif.config.commonmetadata.schema", s"${source}_common_metadata")
    new SriParams(source, country, hdfsDataParentDir, sriOpenSchema, sriNonOpenSchema, verifyTypesSchema, opsSchema,
      sriOpenPartitionName, sriNonOpenPartitionName, verifyTypesPartitionName,
      hdfsTmpDir, tableConfigXml, reconTable, reconTableNameColNum, reconTableCountColNum, fullDump, businessDate, partition, eodMarker,
        sourcingType, eodTableName, reconOutputTableName, metadataTableName, recordTypeBatch,
      dateColFormat,
      timestampCol, timestampColFormat,
      vTypesAuditColSchema, sriAuditColSchema, systemAuditColSchema, parallelExecution, isTableLevelEod, reconSourceQuery
    )
  }
}