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

import com.sc.sdm.models.SriParams
import com.sc.sdm.utils.SriUtils._
import com.sc.sdm.utils.XmlParser.TableConfig
import org.apache.commons.lang3.StringUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession

import scala.io.Source

/**
  * Reruns for a table or a set of tables for a previous business date are driven out of this class.
  */
object RerunProcessor {
  val logger: Logger = Logger.getLogger(RerunProcessor.getClass)

  /** Given a comma separated list of table names, this would validate them for correctness */
  def getRerunTableList(rerunTableList: String, fs: FileSystem, tableConfigs: List[TableConfig]): scala.List[String] = rerunTableList match {
    case x if StringUtils.isEmpty(x) => List()

    case x if x.contains(",") || tableConfigs.map(_.name).contains(x) =>
      lazy val tableNames = tableConfigs.map(_.name)
      logger.info(" Rerun for tables " + x)
      val tables = x.split(',')
      lazy val rerunTables = tables.filter(x => tableNames.contains(x))
      tables.filterNot(x => tableNames.contains(x)).foreach(wrongTable => logger.warn(s"$wrongTable is not a proper table name in the configured list!"))
      require(rerunTables.length > 0, s"$x did not contain a single configured table name, please check rerun inputs")
      rerunTables.toList

    case x if fs.exists(new Path(x)) =>
      val tableList = Source.fromInputStream(fs.open(new Path(x))).mkString
      tableList.split("\n", -1).filter(StringUtils.isNotEmpty(_)).toList

    case _ => throw new IllegalStateException(s"Rerun table list $rerunTableList is incorrect")
  }

  /**
    * If a job is submitted for any day before the current business day, then a cascading runs of Sri would be achieved
    * using this class.
    */
  def cascadedReruns(paramPath: String, runType: String,
                     rerunTableList: String, businessDate: String,
                     rerunTables: List[String],
                     paramConf: Configuration,
                     sriParams: SriParams,
                     tableDictionary: List[TableConfig])(implicit spark: SparkSession): Unit = {
    rerunTables match {
      case List() => logger.info("Normal run ...")
      case tables if tables.nonEmpty && tableDictionary.exists(_.isDeltaSource) =>
        //fetch next business date
        val nextBusinessDate = getNextExistingSriPartition(sriParams, businessDate, tables.head, 10)
        //fetch that date's eod marker
        if (nextBusinessDate.isDefined) {
          logger.info(" Preparing to cascade reprocessing towards the next business date " + nextBusinessDate.head)
          val tableName = paramConf.get("edmhdpif.config.eod.table.name", "eod_table")
          val marker = spark.sql(s"select markertime from ${sriParams.opsSchema}.$tableName where businessdate='${nextBusinessDate.head}'")
          if (marker.rdd.isEmpty()) {
            logger.warn(" No EOD marker was found for business date " + nextBusinessDate.head + " this cannot be cascaded automatically ")
          } else {
            //run
            val argz = Array(marker.head().getString(0), nextBusinessDate.head + " 00:00:00", paramPath, runType, rerunTableList)
            BootstrapProcessor.businessDateProcess(argz)
          }
        } else {
          logger.info(" Need not be cascaded ... as there are not further business dates from " + businessDate)
        }
      case tables if tables.nonEmpty && tableDictionary.exists(_.isTxnSource) =>
        logger.info("Transaction table re-run. Ignoring...")
    }
  }
}
