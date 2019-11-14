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

import com.sc.sdm.models.SriParams
import com.sc.sdm.processor.fixtures.IntegrationTestDataHelpers
import com.sc.sdm.utils.Enums.SourceType
import com.sc.sdm.utils.XmlParser.TableConfig
import org.scalatest.{FlatSpec, Matchers}

class SchemaUtilsTest extends FlatSpec with Matchers {

  "createCdcTable" should "return Schema with Cdc columns" in {
    val tableConfig: TableConfig = TableConfig("gps_all_table1", "delta", "SYSTEMID",
      "SYSTEMID VARCHAR(2) NOT NULL DEFAULT 'AB'^CURRENCYVAL VARCHAR(4) DROP^CURRENTDATE VARCHAR(10)^DROPPEDCOL VARCHAR(8)", null, "", "", "", "", "", "", None)

    val sriAuditColSchema = "start_date STRING NOT NULL ^start_time STRING NOT NULL ^end_date STRING NOT NULL ^end_time STRING NOT NULL ^del_flag STRING NOT NULL"
    val vTypesAuditColSchema = "rowId STRING NOT NULL ^filename STRING NOT NULL ^vds STRING NOT NULL"

    val sriParams: SriParams = IntegrationTestDataHelpers.createSriParams("", "", "", "", SourceType.CDC, "target/batchIngestion", vTypesAuditColSchema, sriAuditColSchema, "", "", "")

    val tableList = SchemaUtils.getBusinessTablesSchema(tableConfig, sriParams)
    assert(tableList.size == 9)

    val hdfsRoot = sriParams.hdfsBaseDir

    val margin: String =
      s"""create schema if not exists gps_sri_open;
         |drop table if exists gps_sri_open.gps_all_table1;
         |create external table if not exists gps_sri_open.gps_all_table1(
         |   ROWID string
         |   ,s_startdt string,s_starttime string,s_enddt string,s_endtime string,s_deleted_flag string
         |   ,c_journaltime string,c_transactionid string,c_operationtype string,c_userid string,SYSTEMID VARCHAR(2),CURRENTDATE VARCHAR(10),DROPPEDCOL VARCHAR(8)
         |   )
         |partitioned by (ods string)
         |stored as ORC
         |location '${hdfsRoot}gps_sri_open/gps_all_table1';create schema if not exists gps_sri_nonopen;
         |use gps_sri_nonopen;
         |drop table if exists gps_sri_nonopen.gps_all_table1;
         |create external table if not exists gps_sri_nonopen.gps_all_table1(
         |   ROWID string
         |   ,s_startdt string,s_starttime string,s_enddt string,s_endtime string,s_deleted_flag string
         |   ,c_journaltime string,c_transactionid string,c_operationtype string,c_userid string,SYSTEMID VARCHAR(2),CURRENTDATE VARCHAR(10),DROPPEDCOL VARCHAR(8)
         |   )
         |partitioned by (nds string)
         |stored as ORC
         |location '${hdfsRoot}gps_sri_nonopen/gps_all_table1';
         |create schema if not exists gps_storage;
         |use gps_storage;
         |drop table if exists gps_all_table1_verifytypes;
         |create external table if not exists gps_all_table1_verifytypes(
         |   rowid string
         |   ,c_journaltime string,c_transactionid string,c_operationtype string,c_userid string,SYSTEMID VARCHAR(2),CURRENTDATE VARCHAR(10),DROPPEDCOL VARCHAR(8)
         |   )
         |partitioned by (vds string)
         |stored as ORC
         |location '${hdfsRoot}gps_storage/gps_all_table1_verifytypes';create schema if not exists gps_ops;
         |use gps_ops;
         |drop table if exists gps_all_rowcounts;
         |create external table if not exists gps_ops.gps_all_rowcounts(
         |   schemaname string COMMENT 'schema (or database)',
         |   tablename string COMMENT 'physical table that is the source of rowcount computation',
         |   rowcount bigint,
         |   functionaltable string COMMENT 'logical table whose data is embedded in the physical table and that forms the granularity of rowcounts',
         |   asof string COMMENT 'date timestamp at which the row count entry was made',
         |   comment string COMMENT 'processing step that populated this row',
         |   attempt_id string COMMENT 'job attempt that executed this step'
         |   )
         |partitioned by (rcds string)
         |stored as ORC
         |location '${hdfsRoot}gps_ops/gps_all_rowcounts';
         | """.stripMargin

    margin.split(";").zip(tableList).foreach {
      x =>
        x._1.toLowerCase().trim == x._2.toLowerCase().trim
    }
  }


  "createNonCdcTable" should "return Schema without Cdc columns" in {
    val tableConfig: TableConfig = TableConfig("gps_all_table1", "delta", "SYSTEMID",
      "SYSTEMID VARCHAR(2) NOT NULL DEFAULT 'AB'^CURRENCYVAL VARCHAR(4) DROP^CURRENTDATE VARCHAR(10)^DROPPEDCOL VARCHAR(8)", null, "", "", "", "", "", "", None)

    val sriAuditColSchema = "start_date STRING NOT NULL ^start_time STRING NOT NULL ^end_date STRING NOT NULL ^end_time STRING NOT NULL ^del_flag STRING NOT NULL"
    val vTypesAuditColSchema = "rowId STRING NOT NULL ^filename STRING NOT NULL ^vds STRING NOT NULL"

    val sriParams: SriParams = IntegrationTestDataHelpers.createSriParams("", "", "", "", SourceType.CDC, "target/batchIngestion", vTypesAuditColSchema, sriAuditColSchema, "", "", "")

    val tablesList = SchemaUtils.getBusinessTablesSchema(tableConfig, sriParams)
    assert(tablesList.length == 9)

    val hdfsRoot = sriParams.hdfsBaseDir

    val margin: String =
      s"""create schema if not exists gps_sri_open;
         |drop table if exists gps_sri_open.gps_all_table1;
         |create external table if not exists gps_sri_open.gps_all_table1(
         | ROWID string
         |,s_startdt string, s_starttime string, s_enddt string, s_endtime string, s_deleted_flag string
         |,c_journaltime string,c_transactionid string,c_operationtype string,c_userid string,SYSTEMID VARCHAR(2),CURRENTDATE VARCHAR(10),DROPPEDCOL VARCHAR(8)
         |)
         |partitioned by(ods string)
         |stored as ORC
         |location '${hdfsRoot}gps_sri_open/gps_all_table1';create schema if not exists gps_sri_nonopen;
         |use gps_sri_nonopen;
         |drop table if exists gps_sri_nonopen.gps_all_table1;
         |create external table if not exists gps_sri_nonopen.gps_all_table1(
         |   rowid string
         |   ,s_startdt string,s_starttime string,s_enddt string,s_endtime string,s_deleted_flag string
         |   ,SYSTEMID VARCHAR(2),CURRENTDATE VARCHAR(10),DROPPEDCOL VARCHAR(8)
         |   )
         |partitioned by (nds string)
         |stored as ORC
         |location '${hdfsRoot}gps_sri_nonopen/gps_all_table1';
         |create schema if not exists gps_storage;
         |use gps_storage;
         |drop table if exists gps_all_table1_verifytypes;
         |create external table if not exists gps_all_table1_verifytypes(
         |   rowid string
         |   ,SYSTEMID VARCHAR(2),CURRENTDATE VARCHAR(10),DROPPEDCOL VARCHAR(8)
         |   )
         |partitioned by (vds string)
         |stored as ORC
         |location '${hdfsRoot}gps_storage/gps_all_table1_verifytypes';create schema if not exists gps_ops;
         |use gps_ops;
         |drop table if exists gps_all_rowcounts;
         |create external table if not exists gps_ops.gps_all_rowcounts(
         |   schemaname string COMMENT 'schema (or database)',
         |   tablename string COMMENT 'physical table that is the source of rowcount computation',
         |   rowcount bigint,
         |   functionaltable string COMMENT 'logical table whose data is embedded in the physical table and that forms the granularity of rowcounts',
         |   asof string COMMENT 'date timestamp at which the row count entry was made',
         |   comment string COMMENT 'processing step that populated this row',
         |   attempt_id string COMMENT 'job attempt that executed this step'
         |   )
         |partitioned by (rcds string)
         |stored as ORC
         |location '${hdfsRoot}gps_ops/gps_all_rowcounts';
         | """.stripMargin

    margin.split(";").zip(tablesList).foreach {
      x =>
        x._1.toLowerCase().trim == x._2.toLowerCase().trim
    }
  }

}
