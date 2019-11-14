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

package com.sc.sdm.processor.fixtures

import java.io.File
import java.util.UUID

import com.sc.sdm.models.SriParams
import com.sc.sdm.utils.Enums.SourceType
import com.sc.sdm.utils.Enums.SourceType.SourceType
import com.sc.sdm.utils.FileSystemUtils
import com.sc.sdm.utils.XmlParser.TableConfig
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.SparkSession

/** Fixtures holder for Integration tests */
object IntegrationTestDataHelpers {

  System.setProperty("hive.exec.staging", "tmp/hive/staging")
  System.setProperty("hive.exec.scratchdir", "tmp/hive/staging")

  val nonCdcTableConfig: String => List[TableConfig] = { x =>
    List(TableConfig(x, "delta", "SYSTEMID",
      "SYSTEMID VARCHAR(2) NOT NULL DEFAULT 'AB'^CURRENCYVAL VARCHAR(4)^OPTYPE VARCHAR(1)^DROPPEDCOL VARCHAR(8) ^RTCOL FLOAT", null, "4", "D", "OPTYPE", "I", "B", "A", None))
  }

  val nonCdcTableConfigTxn: String => List[TableConfig] = { x =>
    List(TableConfig(x, "txn", "SYSTEMID",
      "SYSTEMID VARCHAR(2) NOT NULL DEFAULT 'AB'^CURRENCYVAL VARCHAR(4)^OPTYPE VARCHAR(1)^DROPPEDCOL VARCHAR(8) ^RTCOL FLOAT", null, "4", "D", "OPTYPE", "I", "B", "A", None))
  }

  val cdcTableConfig: String => List[TableConfig] = { x =>
    List(TableConfig(x, "delta", "SYSTEMID",
      "SYSTEMID VARCHAR(2) NOT NULL DEFAULT 'AB'^CURRENCYVAL VARCHAR(4)^CURRENTDATE VARCHAR(10)^DROPPEDCOL CHAR(8) ^RTCOL INT ^DECIML_COL DECIMAL(38,3) ^DBL_COL DOUBLE", null, "", "D", "OPTYPE", "I", "B", "A", None))
  }

  /** Data builder for NonCDC/File based Integration testcases */
  def prepareSampleNonCDCData(id: String, spark: SparkSession, base: String, tableName: String): SriParams = {

    val tableDictionary: List[TableConfig] = nonCdcTableConfig(tableName)

    val sriAuditColSchema = "start_date STRING NOT NULL ^start_time STRING NOT NULL ^end_date STRING NOT NULL ^end_time STRING NOT NULL ^del_flag STRING NOT NULL"
    val vTypesAuditColSchema = "rowId STRING NOT NULL"

    val sriParams: SriParams = createSriParams(id, "", "", "", SourceType.BATCH_DELIMITED, base, sriAuditColSchema, vTypesAuditColSchema, "", "start_date", "yyyy-MM-dd")

    val verifyTypesBaseDir: String = sriParams.getVerifyTypesPath(tableDictionary.head.name)

    val tableData: Seq[ITNonCDCRecord] = Seq(
      ITNonCDCRecord(s"${System.nanoTime()}_${UUID.randomUUID().toString}", "FNAME", "2017-01-01", "01", "CVAL", "I", "12345678", 2.3f),
      ITNonCDCRecord(s"${System.nanoTime()}_${UUID.randomUUID().toString}", "FNAME", "2017-01-01", "02", "CVAL", "I", "12345678", 2.3f),
      ITNonCDCRecord(s"${System.nanoTime()}_${UUID.randomUUID().toString}", "FNAME", "2017-01-01", "03", "CVAL", "I", "12345678", 2.3f),
      ITNonCDCRecord(s"${System.nanoTime()}_${UUID.randomUUID().toString}", "FNAME", "2017-01-01", "03", "CVAL", "I", "12345678", 2.3f),
      ITNonCDCRecord(s"${System.nanoTime()}_${UUID.randomUUID().toString}", "FNAME", "2017-01-01", "04", "CVAL", "I", "12345678", 2.3f),
      ITNonCDCRecord(s"${System.nanoTime()}_${UUID.randomUUID().toString}", "FNAME", "2017-01-01", "01", "CVL",  "A", "12345678", 2.3f),
      ITNonCDCRecord(s"${System.nanoTime()}_${UUID.randomUUID().toString}", "FNAME", "2017-01-01", "02", "CVAL", "A", "1234567",  2.3f),
      ITNonCDCRecord(s"${System.nanoTime()}_${UUID.randomUUID().toString}", "FNAME", "2017-01-01", "04", "CVAL", "D", "12345678", 2.3f),
      ITNonCDCRecord(s"${System.nanoTime()}_${UUID.randomUUID().toString}", "FNAME", "2017-01-01", "03", "CVAL", "I", "12345678", 2.3f)
    ) ++ Seq(
      ITNonCDCRecord(s"${System.nanoTime()}_${UUID.randomUUID().toString}", "FNAME", "2017-01-02", "11", "CVAL", "I", "12345678", 2.3f),
      ITNonCDCRecord(s"${System.nanoTime()}_${UUID.randomUUID().toString}", "FNAME", "2017-01-02", "12", "CVAL", "I", "12345678", 2.3f),
      ITNonCDCRecord(s"${System.nanoTime()}_${UUID.randomUUID().toString}", "FNAME", "2017-01-02", "13", "CVAL", "I", "12345678", 2.3f),
      ITNonCDCRecord(s"${System.nanoTime()}_${UUID.randomUUID().toString}", "FNAME", "2017-01-02", "13", "CVAL", "D", "12345678", 2.3f),
      ITNonCDCRecord(s"${System.nanoTime()}_${UUID.randomUUID().toString}", "FNAME", "2017-01-02", "01", "CVL",  "D", "12345678", 2.3f),
      ITNonCDCRecord(s"${System.nanoTime()}_${UUID.randomUUID().toString}", "FNAME", "2017-01-02", "11", "CVL",  "A", "12345678", 2.3f),
      ITNonCDCRecord(s"${System.nanoTime()}_${UUID.randomUUID().toString}", "FNAME", "2017-01-02", "12", "CVAL", "A", "1234567",  2.3f),
      ITNonCDCRecord(s"${System.nanoTime()}_${UUID.randomUUID().toString}", "FNAME", "2017-01-02", "12", "CVAL", "A", "1234567",  2.3f),
      ITNonCDCRecord(s"${System.nanoTime()}_${UUID.randomUUID().toString}", "FNAME", "2017-01-02", "03", "CVAL", "I", "12345678", 2.3f) //self introduced duplicate.
    )

    import spark.sqlContext.implicits._
    spark.sparkContext.parallelize(tableData).toDF().write.format("com.databricks.spark.avro").partitionBy("vds").save(verifyTypesBaseDir)

    val fs: FileSystem = FileSystemUtils.fetchFileSystem

    fs.listStatus(new Path(verifyTypesBaseDir)).foreach {
      partition =>
        fs.listStatus(partition.getPath).filter(_.getPath.getName.startsWith("part-r-")).foreach {
          file =>
            val newPath: String = partition.getPath.toString + "/" + tableName.split('_').drop(2).mkString("_") + ".D0" + file.getPath.getName
            val oldPath: String = partition.getPath.toString + "/" + file.getPath.getName

            fs.rename(new Path(oldPath), new Path(newPath))
        }
    }
    sriParams
  }

  /** SriParams factory method for Integration testcases */
  def createSriParams(id: String, partition: String, businessDate: String,
                      eodMarker: String, sourcingType: SourceType, base: String = "target/batchIngestion",
                      vTypesAuditColSchema: String, sriAuditColSchema: String, systemAuditColSchema: String, timestampCol: String, timestampColFormat: String, schmPrefix: String = "", reconTableName: String = "") = {

    new SriParams("gps", "all", new File(s"$base/roothdfs/").getAbsoluteFile.toString.replaceAll("\\\\", "/") +
      "/", "gps_sri_open" + id,
      "gps_sri_nonopen" + id, "gps_storage" + id, "gps_ops" + id, "ods", "nds", "vds",
      s"$base/tmp" + UUID.randomUUID().toString.substring(1, 5) +
        "/fw", "sampleconfig/gps_all_tables_config.xml", reconTableName, 0, "1",
      false, businessDate, partition, eodMarker, sourcingType, "eod_table", "recon_output", "metadatatable", "N", "YYYY-mm-dd HH:mm:ss",
      timestampCol, timestampColFormat, vTypesAuditColSchema, sriAuditColSchema, systemAuditColSchema)
  }

  /** Data builder for CDC based Integration testcases */
  def prepareSampleCDCData: (SriParams, SparkSession, String, String) => SriParams = {
    (sriParams: SriParams, spark: SparkSession, base: String, tableName: String) =>
      val tableDictionary: List[TableConfig] = cdcTableConfig(tableName)

      val verifyTypesBaseDir: String = sriParams.getVerifyTypesPath(tableDictionary.head.name)

      val table1P1: Seq[ITCDCRecord] = Seq(
        ITCDCRecord(s"${System.nanoTime()}_${UUID.randomUUID().toString}", "FNAME", "2017-01-01", "2017-01-01 00:01:15.000000", "trid", "I", "user", "01", "CVAL", "2015-01-01", "12345678", 12, 1212.23, 1212.23),
        ITCDCRecord(s"${System.nanoTime()}_${UUID.randomUUID().toString}", "FNAME", "2017-01-01", "2017-01-01 00:01:15.000000", "trid", "I", "user", "02", "CVAL", "2015-01-01", "12345678", 13, 1212.23, 1212.23),
        ITCDCRecord(s"${System.nanoTime()}_${UUID.randomUUID().toString}", "FNAME", "2017-01-01", "2017-01-01 00:02:15.000000", "trid", "I", "user", "03", "CVAL", "2015-01-01", "12345678", 14, 1212.23, 1212.23),
        ITCDCRecord(s"${System.nanoTime()}_${UUID.randomUUID().toString}", "FNAME", "2017-01-01", "2017-01-01 00:03:15.000000", "trid", "I", "user", "04", "CVAL", "2015-01-01", "12345678", 15, 1212.23, 1212.23),
        ITCDCRecord(s"${System.nanoTime()}_${UUID.randomUUID().toString}", "FNAME", "2017-01-01", "2017-01-01 00:03:15.000000", "trid", "I", "user", "04", "CVAL", "2015-01-01", "12345678", 15, 1212.23, 1212.23),
        ITCDCRecord(s"${System.nanoTime()}_${UUID.randomUUID().toString}", "FNAME", "2017-01-01", "2017-01-01 00:03:16.000000", "trid", "I", "user", "04", "CVAL", "2015-01-01", "12345678", 15, 1212.23, 1212.23),
        ITCDCRecord(s"${System.nanoTime()}_${UUID.randomUUID().toString}", "FNAME", "2017-01-01", "2017-01-01 00:03:15.000000", "trid", "I", "user", "44", "CVAL", "2015-01-01", "12345678", 0, 1212.23, 1212.23)
      ) ++ Seq(
        ITCDCRecord(s"${System.nanoTime()}_${UUID.randomUUID().toString}", "FNAME", "2017-01-01", "2017-01-01 01:01:15.000000", "trid", "B", "user", "01", "CVAL", "2015-01-01", "12345678", 0, null, 1212.23),
        ITCDCRecord(s"${System.nanoTime()}_${UUID.randomUUID().toString}", "FNAME", "2017-01-01", "2017-01-01 01:01:15.000000", "trid", "A", "user", "01", "CVL", "2015-01-01", "12345678", 19, 1212.23, 1212.23),
        ITCDCRecord(s"${System.nanoTime()}_${UUID.randomUUID().toString}", "FNAME", "2017-01-01", "2017-01-01 01:02:15.000000", "trid", "B", "user", "03", "CVAL", "2015-01-01", "12345678", 21, 1212.23, 1212.23),
        ITCDCRecord(s"${System.nanoTime()}_${UUID.randomUUID().toString}", "FNAME", "2017-01-01", "2017-01-01 01:02:15.000000", "trid", "A", "user", "03", "CAL", "2015-01-01", "12345678", 22, 1212.23, 1212.23),
        ITCDCRecord(s"${System.nanoTime()}_${UUID.randomUUID().toString}", "FNAME", "2017-01-01", "2017-01-01 01:03:15.000000", "trid", "B", "user", "02", "CVAL", "2015-01-01", "12345678", 23, 1212.23, 1212.23),
        ITCDCRecord(s"${System.nanoTime()}_${UUID.randomUUID().toString}", "FNAME", "2017-01-01", "2017-01-01 01:03:15.000000", "trid", "A", "user", "02", "CVAL", "2015-01-01", "1234567", 24, 1212.23, 1212.23),
        ITCDCRecord(s"${System.nanoTime()}_${UUID.randomUUID().toString}", "FNAME", "2017-01-01", "2017-01-01 00:03:15.000000", "trid", "D", "user", "44", "CVAL", "2015-01-01", "12345678", 25, 1212.23, 1212.23)
      ) ++ Seq(
        ITCDCRecord(s"${System.nanoTime()}_${UUID.randomUUID().toString}", "FNAME", "2017-01-02", "2017-01-02 00:00:15.000000", "trid", "I", "user", "11", "CVAL", "2015-01-01", "12345678", 26, 1212.23, 1212.23),
        ITCDCRecord(s"${System.nanoTime()}_${UUID.randomUUID().toString}", "FNAME", "2017-01-02", "2017-01-02 00:01:15.000000", "trid", "I", "user", "12", "CVAL", "2015-01-01", "12345678", 27, 1212.23, 1212.23),
        ITCDCRecord(s"${System.nanoTime()}_${UUID.randomUUID().toString}", "FNAME", "2017-01-02", "2017-01-02 00:02:15.000000", "trid", "I", "user", "13", "CVAL", "2015-01-01", "12345678", 28, 1212.23, 1212.23),
        ITCDCRecord(s"${System.nanoTime()}_${UUID.randomUUID().toString}", "FNAME", "2017-01-02", "2017-01-02 00:03:15.000000", "trid", "I", "user", "14", "CVAL", "2015-01-01", "12345678", 29, 1212.23, 1212.23)
      ) ++ Seq(
        ITCDCRecord(s"${System.nanoTime()}_${UUID.randomUUID().toString}", "FNAME", "2017-01-02", "2017-01-02 01:00:15.000000", "trid", "B", "user", "11", "CVAL", "2015-01-01", "12345678", 30, 1212.23, 1212.23),
        ITCDCRecord(s"${System.nanoTime()}_${UUID.randomUUID().toString}", "FNAME", "2017-01-02", "2017-01-02 01:01:15.000000", "trid", "A", "user", "11", "CVL", "2015-01-01", "12345678", 31, 1212.23, 1212.23),
        ITCDCRecord(s"${System.nanoTime()}_${UUID.randomUUID().toString}", "FNAME", "2017-01-02", "2017-01-02 01:02:15.000000", "trid", "B", "user", "13", "CVAL", "2015-01-01", "12345678", 32, 1212.23, 1212.23),
        ITCDCRecord(s"${System.nanoTime()}_${UUID.randomUUID().toString}", "FNAME", "2017-01-02", "2017-01-02 01:03:15.000000", "trid", "A", "user", "13", "CAL", "2015-01-01", "12345678", 33, 1212.23, 1212.23),
        ITCDCRecord(s"${System.nanoTime()}_${UUID.randomUUID().toString}", "FNAME", "2017-01-02", "2017-01-02 01:03:15.000000", "trid", "B", "user", "12", "CVAL", "2015-01-01", "12345678", 34, 1212.23, 1212.23),
        ITCDCRecord(s"${System.nanoTime()}_${UUID.randomUUID().toString}", "FNAME", "2017-01-02", "2017-01-02 01:03:15.000000", "trid", "A", "user", "12", "CVAL", "2015-01-01", "1234567", 35, 1212.23, 1212.23),
        ITCDCRecord(s"${System.nanoTime()}_${UUID.randomUUID().toString}", "FNAME", "2017-01-02", "2017-01-02 01:03:15.000000", "trid", "A", "user", "11", "CVAL", "2015-01-01", "1234567", 36, 1212.23, 1212.23), //sample for PKV test
        ITCDCRecord(s"${System.nanoTime()}_${UUID.randomUUID().toString}", "FNAME", "2017-01-02", "2017-01-03 11:03:15.000000", "trid", "I", "user", "15", "CVAL", "2015-01-01", "12345697", 36, 1212.23, 1212.23) //sample for early event
      )

      import spark.sqlContext.implicits._
      spark.sparkContext.parallelize(table1P1).toDF().write.format("com.databricks.spark.avro").partitionBy("vds").save(verifyTypesBaseDir)

      val fs: FileSystem = FileSystemUtils.fetchFileSystem


      fs.listStatus(new Path(verifyTypesBaseDir)).foreach {
        partition =>
          fs.listStatus(partition.getPath).filter(_.getPath.getName.startsWith("part-r-")).foreach {
            file =>
              val newPath: String = partition.getPath.toString + "/" + tableName.split('_').drop(2).mkString("_") + ".D0" + file.getPath.getName
              val oldPath: String = partition.getPath.toString + "/" + file.getPath.getName

              fs.rename(new Path(oldPath), new Path(newPath))
          }
      }

      sriParams
  }

  /** Represents a source sent Recon table record */
  case class ReconRecord(tableName: String, count: BigDecimal)

  case class ITCDCRecord(rowId: String, filename: String, vds: String, c_journaltime: String, c_transactionid: String, c_operationtype: String,
                         c_userid: String, SYSTEMID: String, CURRENCYVAL: String, CURRENTDATE: String,
                         DROPPEDCOL: String, RTCOL: Int, DECIML_COL: java.lang.Double, DBL_COL: Double)

  case class ITNonCDCRecord(rowId: String, filename: String, vds: String, SYSTEMID: String, CURRENCYVAL: String, OPTYPE: String, DROPPEDCOL: String, RTCOL: Float)

}



