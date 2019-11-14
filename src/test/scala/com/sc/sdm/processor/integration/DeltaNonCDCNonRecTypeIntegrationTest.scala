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
import org.apache.commons.lang3.StringUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.spark.sql.DataFrame
import org.scalatest.{FlatSpec, Matchers}

/** Tests the Delta/Master source type behavior for Non Record Type/File based system tables */
class DeltaNonCDCNonRecTypeIntegrationTest extends FlatSpec with Matchers with SdmSharedSparkContext {
  "DeltaNonCdc New DataSriTest" should "createSriOpen_NonOpen" in {
      val id = StringUtils.substringAfterLast(UUID.randomUUID().toString, "-")
      val base = "target/batchIngestion/" + id
      val tableName = "gps_all_deltanoncdcdatasritest"
      IntegrationTestDataHelpers.prepareSampleNonCDCData(id, spark, base, tableName)
      val tableDictionary: List[TableConfig] = IntegrationTestDataHelpers.nonCdcTableConfig(tableName)
      val eodMarker = "2017-01-01 02:10:00"
      val businessDay = "2017-01-01 02:10:00"

      spark.sparkContext.setLogLevel("ERROR")

      val partition: String = SriUtils.getBusinessDayPartition(businessDay, "yyyy-MM-dd HH:mm:ss").get

      val vTypesAuditColSchema = "rowId STRING NOT NULL ^filename STRING NOT NULL ^vds STRING NOT NULL"
      val sriAuditColSchema = "start_date STRING NOT NULL ^start_time STRING NOT NULL ^end_date STRING NOT NULL ^end_time STRING NOT NULL ^del_flag STRING NOT NULL"
      val systemAuditColSchema = ""

      val sriParams: SriParams = IntegrationTestDataHelpers.createSriParams(id, partition, partition, eodMarker, SourceType.BATCH_DELIMITED,
          base, vTypesAuditColSchema, sriAuditColSchema, systemAuditColSchema, tableName.split("_")(2),
          "start_date", "yyyy-MM-dd")
        .copy(recordTypeBatch = "N", timestampCol = "start_date", timestampColFormat = "yyyy-MM-dd")

      val fs: FileSystem = FileSystemUtils.fetchFileSystem
      SchemaUtils.getOpsSchema(sriParams).foreach(spark.sql)
      SchemaUtils.getBusinessTablesSchema(tableDictionary.head, sriParams).foreach(spark.sql)

      BootstrapProcessor.processSri(sriParams, fs, tableDictionary, new Configuration())(spark)

      val sqlSriOpen1: DataFrame = spark.sql(s"select count(*) from ${sriParams.sriOpenSchema}." + tableName + " where ods='2017-01-01'")
      assert(sqlSriOpen1.take(1).map(_.getLong(0)).head == 4)

      val sqlSriNonOpen1: DataFrame = spark.sql(s"select count(*) from ${sriParams.sriNonOpenSchema}." + tableName + " where nds='2017-01-01'")
      assert(sqlSriNonOpen1.take(1).map(_.getLong(0)).head == 3)
      assert(spark.sql(s"select rowcount from ${sriParams.opsSchema}.gps_all_rowcounts where rcds='2017-01-01' and step_name='Duplicates'").take(1).map(_.getLong(0)).head == 2)

      val eodMarker2 = "2017-01-02 10:10:00"
      val businessDay2 = "2017-01-02 10:10:00"

      val partition2: String = SriUtils.getBusinessDayPartition(businessDay2, "yyyy-MM-dd HH:mm:ss").get
      val sriParams2: SriParams = IntegrationTestDataHelpers.createSriParams(id, partition2, partition2, eodMarker2, SourceType.BATCH_DELIMITED, base, vTypesAuditColSchema, sriAuditColSchema, systemAuditColSchema, "start_date", "yyyy-MM-dd", tableName.split("_")(2))
        .copy(recordTypeBatch = "N", timestampCol = "start_date", timestampColFormat = "yyyy-MM-dd")
      BootstrapProcessor.processSri(sriParams2, fs, tableDictionary, new Configuration())(spark)

      val sqlSriOpen2: DataFrame = spark.sql("select count(*) from " + sriParams.sriOpenSchema + "." + tableName + " where ods='2017-01-02'")
      assert(sqlSriOpen2.take(1).map(_.getLong(0)).head == 7)
      val sqlSriNonOpen2: DataFrame = spark.sql("select count(*) from " + sriParams.sriNonOpenSchema + "." + tableName + " where nds='2017-01-02'")
      assert(sqlSriNonOpen2.take(1).map(_.getLong(0)).head == 4)


      //Assert previous day as well
      assert(spark.sql(s"select count(*) from ${sriParams.sriOpenSchema}." + tableName + " where ods='2017-01-01'").take(1).map(_.getLong(0)).head == 4)
      assert(spark.sql(s"select count(*) from ${sriParams.sriNonOpenSchema}." + tableName + " where nds='2017-01-01'").take(1).map(_.getLong(0)).head == 3)
      assert(spark.sql(s"select rowcount from ${sriParams.opsSchema}.gps_all_rowcounts where rcds='2017-01-01' and step_name='Duplicates'").take(1).map(_.getLong(0)).head == 2)

    /*val eodMarker3 = "2017-02-03 10:10:00"
    val businessDay3 = "2017-01-03 02:10:00"

    val (partition3: String, businessDate3: String) = SriUtils.getBusinessDayPartition(businessDay3).get
    val sriParams3: SriParams = SriParams(id, partition3, businessDate3, eodMarker3, "batch", base, tableName.split("_")(2))
    BootstrapProcessor.processSriForBatch(sriParams3, fs, tableDictionary, new Configuration())(sc, spark, "orc")*/

    /*val sqlSriOpen3: DataFrame = spark.sql("select count(*) from " + sriParams.sriOpenSchema + "." + tableName + " where ods='2017-01-03'")
    assert(sqlSriOpen3.take(1).map(_.getLong(0)).head == 5)*/

  }
}
