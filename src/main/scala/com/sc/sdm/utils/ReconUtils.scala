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

import java.math
import java.sql.Timestamp
import java.util.Date

import com.sc.sdm.models.OpsModels.EdmRecon
import com.sc.sdm.models.SriParams
import com.sc.sdm.utils.XmlParser.TableConfig
import org.antlr.stringtemplate.StringTemplate
import org.antlr.stringtemplate.language.DefaultTemplateLexer
import org.apache.commons.lang3.StringUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.log4j.Logger
import org.apache.spark.sql._

import scala.collection.JavaConverters.mapAsJavaMapConverter
import scala.util.{Failure, Success, Try}

/** Runs reconciliation between the source reconciliation data and the data that SDM stored and processed */
object ReconUtils {
  val logger: Logger = Logger.getLogger(ReconUtils.getClass)

  /**
    * Runs the reconciliation task for each of the tables.
    * Upon comparison, either a Success or a Failure status file is written.
    *
    * @param sriParams       Instance of [[SriParams]]
    * @param tableDictionary List of [[TableConfig]]
    * @param fs              Handle to the HDFS
    * @param paramConf       Unparsed Hadoop configuration
    * @param spark           Spark session
    */
  def recon(sriParams: SriParams, tableDictionary: List[TableConfig], fs: FileSystem, paramConf: Configuration)(implicit spark: SparkSession): Unit = {

    val load = sourceReconValues(sriParams)
    val columns = sriParams.reconCountColumn.split(",", -1).map(_.toInt)
    val srcReconValues: Array[(String, Seq[math.BigDecimal])] = load.rdd.map {
      x =>
        (x.getString(sriParams.reconTableNameColumn), columns.map(xx => checkTypeAndParseDecimal(x.get(xx))).toSeq)
    }.collect()

    val reconTableConfig: List[TableConfig] = tableDictionary.filterNot(_.name.isEmpty).map(config => setDefaultReconQueryWhenEmpty(sriParams, config))
    //val reconTableConfig: List[TableConfig] = tableDictionary.filterNot(_.name.isEmpty).filter(_.name=="a1_mantas_acctemailaddr").map(config => setDefaultReconQueryWhenEmpty(sriParams, config))

    if (reconTableConfig.nonEmpty) {
      val reconResult = reconTableConfig.par.map {
        tableConfig =>
          val tableName: String = tableConfig.name.split("_", -1).drop(2).mkString("_")
          Try {
            val context = Map(
              "business_date" -> sriParams.businessDate,
              "eod_maker" -> sriParams.eodMarker,
              "country" -> sriParams.country,
              "source" -> sriParams.source,
              "type_of_source" -> sriParams.sourcingType.toString, //Yikes
              "table" -> tableName,
              "keys" -> tableConfig.keyCols,
              "columns" -> tableConfig.colSchema,
              "type" -> tableConfig.sourceType
            )
            val query = tableConfig.reconQuery.split(";", -1).map(fillInPlaceholders(_, context)).filter(StringUtils.isNotEmpty)
            val sql = Try(spark.sql(query.last))
            val srcRecon = srcReconValues.find(source => StringUtils.containsIgnoreCase(source._1, tableName)).getOrElse {
              logger.warn(s" Recon misconfigured , there was no recon count found for $tableName while ${sriParams.reconTableNameColumn} was used to mark table name columns")
              (tableName, Seq(new math.BigDecimal(0)))
            }
            columns.indices.map {
              column =>
                val sourceValue = sql.map(_.take(1).map(x => x.getLong(column)).head).toOption.getOrElse {
                  logger.warn(s"Sri Count could not be found for $tableName using ${query.last} and $column , hence count assumed to be zero")
                  0L
                }
                val sourceValueBig: BigDecimal = BigDecimal.apply(sourceValue)
                val compare: Int = srcRecon._2(column).compareTo(sourceValueBig.bigDecimal)
                logger.info(tableName + " -> " + sourceValue + " : " + srcRecon + " : compare " + compare)
                prepareReconRecord(sriParams, tableConfig, srcRecon, column, sourceValueBig, compare)
            }

          } match {
            case Success(recon) =>
              Ops.createStatusFile(fs, paramConf, sriParams, "tables", "_COMPLETED_RECON_TABLE_" + tableName)
              Some(recon)
            case Failure(ex) =>
              logger.error(ex.getMessage + " was encountered trying to reconcile ")
              Ops.createStatusFile(fs, paramConf, sriParams, "tables", "_ERROR_RECON_TABLE_" + tableName, ex.getStackTrace.mkString("\n"))
              None
          }
      }
      val reconList = reconResult.filter(_.isDefined).flatMap(_.get).toList
      if (reconList.nonEmpty) {
        writeReconTableOutput(sriParams, reconList)
      }
    }
  }


  /**
    * Check to see if recon sourcing query was configured rather  than
    * recon table name configuration. Returns the one that's configured as a [[DataFrame]].
    *
    * @param spark     Spark session
    * @param sriParams Sri Params
    * @return
    */
  private def sourceReconValues(sriParams: SriParams)(implicit spark: SparkSession): DataFrame = {
    sriParams match {
      case `sriParams` if StringUtils.isEmpty(sriParams.reconSourceQuery) =>
        spark.read.format("orc").load(sriParams.getSriOpenPartitionPath(sriParams.reconTableName, sriParams.businessDate))
      case _ =>
        val context = Map(
          "business_date" -> sriParams.businessDate,
          "eod_maker" -> sriParams.eodMarker,
          "country" -> sriParams.country,
          "source" -> sriParams.source,
          "type_of_source" -> sriParams.sourcingType.toString
        )
        spark.sql(sriParams.
          reconSourceQuery.
          trim.split(";", -1)
          .map(fillInPlaceholders(_, context))
          .filter(StringUtils.isNotEmpty).last)
    }

  }

  /**
    * Sets the default reconciliation query if there's no configured query.
    * Default query checks the rowcount of the table
    *
    * @param sriParams   instance of SriParams
    * @param tableConfig instance of TableConfig
    * @return updated TableConfig
    */
  def setDefaultReconQueryWhenEmpty(sriParams: SriParams, tableConfig: XmlParser.TableConfig): XmlParser.TableConfig = {
    if (StringUtils.isEmpty(tableConfig.reconQuery)) {
      tableConfig.copy(reconQuery = s"select count(*) from ${sriParams.sriOpenSchema}.${tableConfig.name} where ods='${sriParams.businessDate}'")
    }
    else {
      tableConfig
    }
  }

  /** Constructs [[EdmRecon]] output record by comparing the source and the target recon values */
  private def prepareReconRecord(sriParams: SriParams, tableConfig: TableConfig, srcRecon: (String, Seq[math.BigDecimal]),
                                 column: Int, sourceReconValue: BigDecimal, compare: Int): EdmRecon = {

    import sriParams._
    implicit val stringToDecimal: String => BigDecimal = new java.math.BigDecimal(_)
      EdmRecon(source, country, sourcingType.toString,
        tableConfig.name, sourceReconValue.longValue().toString, srcRecon._2(column).longValue().toString,
        column.toString, sourceReconValue.longValue().toFloat,
        srcRecon._2(column).longValue().toFloat, getCountStatus(srcRecon._2(column).longValue().toDouble, sourceReconValue.toDouble),
        getCountStatus(srcRecon._2(column).longValue().toDouble, sourceReconValue.toDouble), new Timestamp(new Date().getTime), batchPartition)
  }

  private def getCountStatus(first: Double, second: Double) = if (first == second) "Success" else "Failed"

  /**
    * Persists the [[EdmRecon]] [[DataFrame]] to the appropriate HDFS location and creates a
    * Hive partition over it
    *
    * @param sriParams    instance of [[SriParams]] for this job
    * @param reconRecords [[DataFrame]] of [[EdmRecon]] objects
    * @param spark        Spark Session
    */
  private def writeReconTableOutput(sriParams: SriParams, reconRecords: List[EdmRecon])(implicit spark: SparkSession): Unit = {

    val reconTableLocation = s"${sriParams.hdfsBaseDir}${sriParams.opsSchema}/${sriParams.reconOutputTableName}"
    val partitionQuoted = s"""batch_date='${sriParams.businessDate}'"""

    import spark.sqlContext.implicits._
    spark.sparkContext.parallelize(reconRecords, 1)
      .toDF()
      .write
      .format("orc")
      .partitionBy("batch_date")
      .mode(SaveMode.Append)
      .save(s"$reconTableLocation")

    SriUtils.createHivePartition(sriParams.opsSchema, sriParams.reconOutputTableName, partitionQuoted)
  }

  /**
    * This fills in the query with the corresponding template
    *
    * @param input
    * @param context
    * @return
    */
  def fillInPlaceholders(input: String, context: Map[String, String]): String = {
    val template = new StringTemplate(input, classOf[DefaultTemplateLexer])
    template.setAttributes(context.asJava)
    Try(template.toString()) match {
      case Success(x) => x
      case Failure(x) => logger.warn("Template resolution failed for recon query " + input + " with " + x.getLocalizedMessage); x.printStackTrace(); input
    }
  }

  /** Returns the Recon value as BigDecimal **/
  private def checkTypeAndParseDecimal(input: Any): java.math.BigDecimal = {
    input match {
      case decimal: java.math.BigDecimal =>
        decimal
      case _ =>
        logger.info(s"${input.toString} was encountered as count value")
        new math.BigDecimal(input.toString)
    }
  }
}
