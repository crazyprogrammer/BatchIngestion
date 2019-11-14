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

package com.sc.sdm.recon

import java.io.{File, FileInputStream}

import com.sc.sdm.models.SriParams
import com.sc.sdm.models.cli.SDMReconCli
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

object SDMRecon {

  /**
    * This program will go through the recon files sent by the source and generate corresponding output for recon comparision.
    *
    * @param args Input arguments to the program. Please refer SDMReconCli for different params expected.
    */
  def main(args: Array[String]): Unit = {

    val cliArguments = if (!args.isEmpty) args else Array("--help")

    val cliParams = new SDMReconCli(cliArguments)
    println(s"Parameters passed to SDM Recon: $cliParams")

    val configuration = new Configuration()
    configuration.addResource(new FileInputStream(s"${cliParams.getConfigLocation}/${cliParams.getSource}_${cliParams.getCountry}_param.xml"))

    val fs = FileSystem.get(configuration)
    val sriParams: SriParams = SriParams(configuration)
    val sriOpenSchema = sriParams.sriOpenSchema
    val hdfsBaseDir = sriParams.hdfsBaseDir

    val sparkConf = new SparkConf().setAppName(s"SDM-Recon-${cliParams.getSource}-${cliParams.getCountry}")
    val sc = new SparkContext(sparkConf)
    val sqlContext = new HiveContext(sc)

    val inputReconDataSet: List[ReconData] = getInputReconData(cliParams.getSource, cliParams.getCountry, cliParams.getBusinessDate, sriParams)
    val reconResults = inputReconDataSet.map { input =>
      val sriDF = getSourceDataFrame(sqlContext, hdfsBaseDir + File.separator + sriOpenSchema + File.separator + input.tableName, sriParams.sriOpenPartitionColumn, cliParams.getBusinessDate, "orc")

      //TODO checksum computation pending
      val outputChecksum = input.checksums.map(x => x)

      (input, new ReconData(input.source, input.country, input.tableName, input.businessDate, sriDF.count, outputChecksum))
    }

    handleReconViolations(reconResults.filter(x => x._1 == x._2))

  }

  /**
    * This implementation can be changed accordingly to get the input recon values either from hive table, local file or a hdfs file
    */
  private def getInputReconData(source: String, country: String, businessDate: String, sriParams: SriParams): List[ReconData] = {
    //TODO implementation pending
    List[ReconData]()
  }

  private def getSourceDataFrame(sqlContext: SQLContext, path: String, partitionColumnName: String, partition: String, format: String): DataFrame = {
    sqlContext.read.format(format).load(path).filter(col(partitionColumnName) === partition).cache
  }

  private def handleReconViolations(reconViolations: List[(ReconData, ReconData)]) = {
    //TODO Implementation Pending
    reconViolations.foreach(println(_))
  }

  case class ReconData(source: String, country: String, tableName: String, businessDate: String, rowCount: Long, checksums: Seq[(String, Double)])

}