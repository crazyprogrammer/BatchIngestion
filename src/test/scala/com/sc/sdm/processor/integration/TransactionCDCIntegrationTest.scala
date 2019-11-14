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

import java.util.UUID

import com.sc.sdm.models.SriParams
import com.sc.sdm.processor.BootstrapProcessor
import com.sc.sdm.processor.fixtures.{IntegrationTestDataHelpers, SdmSharedSparkContext}
import com.sc.sdm.utils.Enums.SourceType
import com.sc.sdm.utils.XmlParser.TableConfig
import com.sc.sdm.utils.{FileSystemUtils, SchemaUtils, SriUtils}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.spark.sql.DataFrame
import org.scalatest.{FlatSpec, Matchers}

/** Tests the Transaction source type behavior for CDC/Record Type based tables */

class TransactionCDCIntegrationTest extends FlatSpec with Matchers with SdmSharedSparkContext {
  "TxnCdcDataSriTest" should "createSriOpen_NonOpen" in {
      val id = UUID.randomUUID().toString.substring(1, 5)
      val base = "target/batchIngestion/" + id
      val tableName = "gps_all_txncdcdatasritest"

      val eodMarker = "2017-01-02 00:00:00"
      val businessDay = "2017-01-01 00:00:00"
      val partition: String = SriUtils.getBusinessDayPartition(businessDay, "yyyy-MM-dd HH:mm:ss").get
      val vTypesAuditColSchema = "rowId STRING NOT NULL ^filename STRING NOT NULL ^vds STRING NOT NULL"
      val sriAuditColSchema = "start_date STRING NOT NULL ^start_time STRING NOT NULL ^end_date STRING NOT NULL ^end_time STRING NOT NULL ^del_flag STRING NOT NULL"
      val systemAuditColSchema = "c_journaltime STRING NOT NULL ^c_transactionid STRING NOT NULL ^c_operationtype STRING NOT NULL ^c_userid STRING NOT NULL"


      val sriParams: SriParams = IntegrationTestDataHelpers.createSriParams(id, partition, partition, eodMarker, SourceType.CDC, base, vTypesAuditColSchema, sriAuditColSchema, systemAuditColSchema,
        "c_journaltimestamp", "yyyy-MM-dd HH:mm:ss:SSSSSS", tableName.split("_")(2), reconTableName = "RECON_TABLE")
        .copy(recordTypeBatch = "Y", timestampCol = "c_journaltime", timestampColFormat = "yyyy-MM-dd HH:mm:ss.SSSSSS")

      val tableDictionary: List[TableConfig] = IntegrationTestDataHelpers.cdcTableConfig(tableName)
        .map { x =>
          import x._
          TableConfig(name, "txn", keyCols.toLowerCase, colSchema,
            s"select count(*) from ${sriParams.sriOpenSchema}.$tableName", deleteIndex, deleteValue, "c_operationtype", "I", "B", "A", Some("incremental"))
        }


      IntegrationTestDataHelpers.prepareSampleCDCData(sriParams, spark, base, tableName)

      val fs: FileSystem = FileSystemUtils.fetchFileSystem
      SchemaUtils.getOpsSchema(sriParams).filterNot(_.startsWith("drop")).foreach(spark.sql)
      SchemaUtils.getBusinessTablesSchema(tableDictionary.head, sriParams).filterNot(_.startsWith("drop")).foreach(spark.sql)

      BootstrapProcessor.processSri(sriParams, fs, tableDictionary, new Configuration())(spark)


      val sqlSriOpen1: DataFrame = spark.sql("select count(*) from " + sriParams.sriOpenSchema + "." + tableName + " where ods='2017-01-01'")
      sqlSriOpen1.take(1).map(_.getLong(0)).head should be(4)


      val eodMarker2 = "2017-01-03 00:00:00"
      val businessDay2 = "2017-01-02 00:00:00"

      val partition2: String = SriUtils.getBusinessDayPartition(businessDay2, "yyyy-MM-dd HH:mm:ss").get

      val sriParams2: SriParams = IntegrationTestDataHelpers.createSriParams(id, partition2, partition2, eodMarker2, SourceType.CDC, base, vTypesAuditColSchema, sriAuditColSchema, systemAuditColSchema,
        "c_journaltimestamp", "yyyy-MM-dd HH:mm:ss:SSSSSS", tableName.split("_")(2), reconTableName = "RECON_TABLE")
        .copy(recordTypeBatch = "Y", timestampCol = "c_journaltime", timestampColFormat = "yyyy-MM-dd HH:mm:ss.SSSSSS")

      BootstrapProcessor.processSri(sriParams2, fs, tableDictionary, new Configuration())(spark)

      val sqlSriOpen2: DataFrame = spark.sql("select count(*) from " + sriParams.sriOpenSchema + "." + tableName + " where ods='2017-01-02'")
      sqlSriOpen2.take(1).map(_.getLong(0)).head should be(4)

      //Assert previous day as well
      spark.sql("select count(*) from " + sriParams.sriOpenSchema + "." + tableName + " where ods='2017-01-01'").take(1).map(_.getLong(0)).head should be(4)
  }

}
