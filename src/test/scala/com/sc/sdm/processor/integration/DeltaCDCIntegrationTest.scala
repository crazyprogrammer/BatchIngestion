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

package com.sc.sdm.processor.integration

import java.io.File
import java.util.UUID

import com.sc.sdm.models.SriParams
import com.sc.sdm.processor.BootstrapProcessor
import com.sc.sdm.processor.fixtures.IntegrationTestDataHelpers.ReconRecord
import com.sc.sdm.processor.fixtures.{IntegrationTestDataHelpers, SdmSharedSparkContext}
import com.sc.sdm.utils.Enums.SourceType
import com.sc.sdm.utils.XmlParser.TableConfig
import com.sc.sdm.utils.{FileSystemUtils, ReconUtils, SchemaUtils, SriUtils}
import org.apache.commons.io.FileUtils
import org.apache.commons.lang3.StringUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.{FlatSpec, Matchers}

/** Tests the Delta source type behavior for CDC/Record Type based tables */
class DeltaCDCIntegrationTest extends FlatSpec with Matchers with SdmSharedSparkContext {

  def prepareReconTableData(spark: SparkSession, sriParams: SriParams, tableName: String): Unit = {
    val effectiveReconLoc = sriParams.getSriOpenPartitionPath(sriParams.reconTableName, sriParams.businessDate)

    spark.createDataFrame(Seq(ReconRecord(tableName, 4)))
      .write.format("orc")
      .save(effectiveReconLoc)
  }

  "DeltaCdcDataSriTest" should "createSriOpen_NonOpen" in {
    val id = StringUtils.substringAfterLast(UUID.randomUUID().toString, "-")
    val base = "target/batchIngestion/" + id

    val tableName = "gps_all_deltacdcdatasritest"


    val eodMarker = "2017-01-02 00:00:00"
    val businessDay = "2017-01-01 00:00:00"

    val partition: String = SriUtils.getBusinessDayPartition(businessDay, "yyyy-MM-dd HH:mm:ss").get

    val vTypesAuditColSchema = "rowId STRING NOT NULL ^filename STRING NOT NULL ^vds STRING NOT NULL"
    val sriAuditColSchema = "start_date STRING NOT NULL ^start_time STRING NOT NULL ^end_date STRING NOT NULL ^end_time STRING NOT NULL ^del_flag STRING NOT NULL"
    val systemAuditColSchema = "c_journaltime STRING NOT NULL ^c_transactionid STRING NOT NULL ^c_operationtype STRING NOT NULL ^c_userid STRING NOT NULL"

    val sriParams: SriParams = IntegrationTestDataHelpers.createSriParams(id, partition, partition, eodMarker, SourceType.CDC, base, vTypesAuditColSchema, sriAuditColSchema, systemAuditColSchema,
      "c_journaltimestamp", "yyyy-MM-dd HH:mm:ss:SSSSSS", tableName.split("_")(2), reconTableName = "RECON_TABLE")
      .copy(recordTypeBatch = "Y", timestampCol = "c_journaltime", timestampColFormat = "yyyy-MM-dd HH:mm:ss.SSSSSS")

    IntegrationTestDataHelpers.prepareSampleCDCData(sriParams, spark, base, tableName)
    prepareReconTableData(spark, sriParams, tableName)
    val tableDictionary: List[TableConfig] = IntegrationTestDataHelpers.cdcTableConfig(tableName)
      .map { x =>
        import x._
        TableConfig(name, sourceType, keyCols.toLowerCase, colSchema,
          s"select count(*) from ${sriParams.sriOpenSchema}.$tableName", deleteIndex, deleteValue, "c_operationtype", "I", "B", "A", Some("incremental"))
      }

    val fs: FileSystem = FileSystemUtils.fetchFileSystem
    SchemaUtils.getOpsSchema(sriParams).filterNot(_.startsWith("drop")).foreach(spark.sql)
    SchemaUtils.getBusinessTablesSchema(tableDictionary.head, sriParams).filterNot(_.startsWith("drop")).foreach(spark.sql)
    val pConf = new Configuration()

    BootstrapProcessor.processSri(sriParams, fs, tableDictionary, new Configuration())(spark)
    ReconUtils.recon(sriParams, tableDictionary, fs, pConf)(spark)

    val sqlSriOpen1: DataFrame = spark.sql("select count(*) from " + sriParams.sriOpenSchema + "." + tableName + " where ods='2017-01-01'")
    assert(sqlSriOpen1.take(1).map(_.getLong(0)).head == 4, "Open count mis-match")

    val sqlSriNonOpen1: DataFrame = spark.sql(s"select count(*) from ${sriParams.sriNonOpenSchema}." + tableName + " where nds='2017-01-01'")
    assert(sqlSriNonOpen1.take(1).map(_.getLong(0)).head == 6, " Non open count mis-match")

    assert(spark.sql(s"select rowcount from ${sriParams.opsSchema}.gps_all_rowcounts where rcds='2017-01-01' and step_name='SRI OPEN'").take(1).map(_.getLong(0)).head == 4)
    assert(spark.sql(s"select rowcount from ${sriParams.opsSchema}.gps_all_rowcounts where rcds='2017-01-01' and step_name='SRI NONOPEN'").take(1).map(_.getLong(0)).head == 6)
    assert(spark.sql(s"select rowcount from ${sriParams.opsSchema}.gps_all_rowcounts where rcds='2017-01-01' and step_name='BRecordsSri'").take(1).map(_.getLong(0)).head == 3)
    assert(spark.sql(s"select rowcount from ${sriParams.opsSchema}.gps_all_rowcounts where rcds='2017-01-01' and step_name='Duplicates'").take(1).map(_.getLong(0)).head == 1)


    FileUtils.deleteDirectory(new File(sriParams.hdfsTmpDir))

    val eodMarker2 = "2017-01-03 00:00:00"
    val businessDay2 = "2017-01-02 00:00:00"

    val partition2: String = SriUtils.getBusinessDayPartition(businessDay2, "yyyy-MM-dd HH:mm:ss").get

    val sriParams2: SriParams = IntegrationTestDataHelpers.createSriParams(id, partition2, partition2, eodMarker2, SourceType.CDC, base, vTypesAuditColSchema, sriAuditColSchema, systemAuditColSchema,
      "c_journaltimestamp", "yyyy-MM-dd HH:mm:ss:SSSSSS", tableName.split("_")(2), reconTableName = "RECON_TABLE")
      .copy(recordTypeBatch = "Y", timestampCol = "c_journaltime", timestampColFormat = "yyyy-MM-dd HH:mm:ss.SSSSSS")

    BootstrapProcessor.processSri(sriParams2, fs, tableDictionary, new Configuration())(spark)

    val sqlSriOpen2: DataFrame = spark.sql(s"select count(*) from ${sriParams.sriOpenSchema}." + tableName + " where ods='2017-01-02'")
    assert(sqlSriOpen2.take(1).map(_.getLong(0)).head == 8)

    val sqlSriNonOpen2: DataFrame = spark.sql(s"select count(*) from ${sriParams.sriNonOpenSchema}." + tableName + " where nds='2017-01-02'")
    assert(sqlSriNonOpen2.take(1).map(_.getLong(0)).head == 4)

    assert(spark.sql(s"select rowcount from ${sriParams.opsSchema}.gps_all_rowcounts where rcds='2017-01-02' and step_name='SRI OPEN'").take(1).map(_.getLong(0)).head == 8)
    assert(spark.sql(s"select rowcount from ${sriParams.opsSchema}.gps_all_rowcounts where rcds='2017-01-02' and step_name='SRI NONOPEN'").take(1).map(_.getLong(0)).head == 4)
    assert(spark.sql(s"select rowcount from ${sriParams.opsSchema}.gps_all_rowcounts where rcds='2017-01-02' and step_name='BRecordsSri'").take(1).map(_.getLong(0)).head == 3)

    spark.sql(s" select * from ${sriParams.opsSchema}.${sriParams.reconOutputTableName}").foreach(x => println(x.toSeq.map(_.toString).mkString("|")))
    spark.sql(s" select count_recon_status from ${sriParams.opsSchema}.${sriParams.reconOutputTableName}").head().getString(0) should be("Success")

    //Assert previous day's data again to ensure that we don't overwrite the partition somehow
    assert(spark.sql(s"select rowcount from ${sriParams.opsSchema}.gps_all_rowcounts where rcds='2017-01-01' and step_name='SRI OPEN'").take(1).map(_.getLong(0)).head == 4)
    assert(spark.sql(s"select rowcount from ${sriParams.opsSchema}.gps_all_rowcounts where rcds='2017-01-01' and step_name='SRI NONOPEN'").take(1).map(_.getLong(0)).head == 6)
    assert(spark.sql(s"select rowcount from ${sriParams.opsSchema}.gps_all_rowcounts where rcds='2017-01-01' and step_name='BRecordsSri'").take(1).map(_.getLong(0)).head == 3)
    assert(spark.sql(s"select rowcount from ${sriParams.opsSchema}.gps_all_rowcounts where rcds='2017-01-01' and step_name='Duplicates'").take(1).map(_.getLong(0)).head == 1)
  }

}
