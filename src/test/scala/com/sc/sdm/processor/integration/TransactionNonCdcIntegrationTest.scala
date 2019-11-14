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

/** Tests the Transaction source type behavior for NonRecord Type/Flat file based system tables */
class TransactionNonCdcIntegrationTest extends FlatSpec with Matchers with SdmSharedSparkContext {

  "TxnNonCdcDataSriTest" should "createSriOpen_NonOpen" in {
      val id = UUID.randomUUID().toString.substring(1, 5)
      val base = "target/batchIngestion/" + id
      val tableName = "gps_all_txnnoncdcdatasritest"
      IntegrationTestDataHelpers.prepareSampleNonCDCData(id, spark, base, tableName)
      val tableDictionary: List[TableConfig] = IntegrationTestDataHelpers.nonCdcTableConfigTxn(tableName).map(x => TableConfig(x.name, x.sourceType, x.keyCols, x.colSchema, x.reconQuery, x.deleteIndex, x.deleteValue, "", "", "", "", None))
      val eodMarker = "2017-02-01 10:10:00"
      val businessDay = "2017-01-01 02:10:00"

      val partition: String = SriUtils.getBusinessDayPartition(businessDay, "yyyy-MM-dd HH:mm:ss").get

      val vTypesAuditColSchema = "rowId STRING NOT NULL ^filename STRING NOT NULL ^vds STRING NOT NULL"
      val sriAuditColSchema = "start_date STRING NOT NULL ^start_time STRING NOT NULL ^end_date STRING NOT NULL ^end_time STRING NOT NULL ^del_flag STRING NOT NULL"
      val systemAuditColSchema = ""

      val sriParams: SriParams = IntegrationTestDataHelpers.createSriParams(id, partition, partition, eodMarker, SourceType.BATCH_DELIMITED, base, vTypesAuditColSchema, sriAuditColSchema, systemAuditColSchema, "start_date", "yyyy-MM-dd", tableName.split("_")(2))
      val fs: FileSystem = FileSystemUtils.fetchFileSystem
      SchemaUtils.getOpsSchema(sriParams).filterNot(_.startsWith("drop")).foreach(spark.sql)
      SchemaUtils.getBusinessTablesSchema(tableDictionary.head, sriParams).filterNot(_.startsWith("drop")).foreach(spark.sql)

      BootstrapProcessor.processSri(sriParams, fs, tableDictionary, new Configuration())(spark)
      val sqlSriOpen1: DataFrame = spark.sql("select count(*) from " + sriParams.sriOpenSchema + "." + tableName + " where ods='2017-01-01'")
      assert(sqlSriOpen1.take(1).map(_.getLong(0)).head == 7)
      assert(spark.sql(s"select * from ${sriParams.opsSchema}.gps_all_rowcounts where  rcds='2017-01-01' and step_name='Duplicates'").take(1).map(_.getLong(3)).head == 4)

      val eodMarker2 = "2017-02-02 10:10:00"
      val businessDay2 = "2017-01-02 02:10:00"

      val partition2: String = SriUtils.getBusinessDayPartition(businessDay2, "yyyy-MM-dd HH:mm:ss").get

      val sriParams2: SriParams = IntegrationTestDataHelpers.createSriParams(id, partition2, partition2, eodMarker2, SourceType.BATCH_DELIMITED, base, vTypesAuditColSchema, sriAuditColSchema, systemAuditColSchema, "start_date", "yyyy-MM-dd", tableName.split("_")(2))

      BootstrapProcessor.processSri(sriParams2, fs, tableDictionary, new Configuration())(spark)
      val sqlSriOpen2: DataFrame = spark.sql("select count(*) from " + sriParams.sriOpenSchema + "." + tableName + " where ods='2017-01-02'")
      assert(sqlSriOpen2.take(1).map(_.getLong(0)).head == 7)
      assert(spark.sql(s"select * from ${sriParams.opsSchema}.gps_all_rowcounts where  rcds='2017-01-02' and step_name='Duplicates'").take(1).map(_.getLong(3)).head == 0)

      //Assert previous days as well
      val sqlSriOpenPrev: DataFrame = spark.sql("select count(*) from " + sriParams.sriOpenSchema + "." + tableName + " where ods='2017-01-01'")
      assert(sqlSriOpenPrev.take(1).map(_.getLong(0)).head == 7)
      assert(spark.sql(s"select * from ${sriParams.opsSchema}.gps_all_rowcounts where  rcds='2017-01-01' and step_name='Duplicates'").take(1).map(_.getLong(3)).head == 4)
  }


}