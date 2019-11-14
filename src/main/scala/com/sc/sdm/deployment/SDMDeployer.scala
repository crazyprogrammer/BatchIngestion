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

package com.sc.sdm.deployment

import java.io.{File, _}
import java.util.concurrent.TimeUnit

import com.sc.sdm.models.SriParams
import com.sc.sdm.models.cli.SDMDeployerCli
import com.sc.sdm.utils.{SchemaUtils, XmlParser}
import io.swagger.client.ApiClient
import io.swagger.client.api._
import io.swagger.client.auth.{Authentication, OAuth}
import io.swagger.client.model._
import org.apache.commons.io.FileUtils
import org.apache.commons.lang3.StringUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.JavaConversions._
import scala.io.Source
import scala.util.Random

/**
  * This program will be used to deploy for a particular source - country system.
  *
  * It will create :
  *
  * 1. Folder structure on NAS/Local file system for given source - country
  *
  * 2. Folder structure on HDFS for given source - country
  *
  * 3. Tables in Hive based on table configs for source - country
  *
  * 4. SDM Nifi template instantiation for source - country
  *
  */
object SDMDeployer {

  /**
    * Entry point to the Deployer. Please refer [[SDMDeployerCli]] for different params expected.
    * @param args Input arguments to the program.
    */
  def main(args: Array[String]): Unit = {
    val cliArguments = if (!args.isEmpty) args else Array("--help")

    val cliParams = new SDMDeployerCli(cliArguments)
    println(s"Parameters passed to SDM Deployer: $cliParams")

    val console = System.console
    if (console == null) {
      System.err.println("No console available.")
      System.exit(1)
    }

    val nifiPassword = if (cliParams.isNifiAuthenticationEnabled) cliParams.nifiPassword.toOption.getOrElse(new String(console.readPassword("Please enter Nifi Password: "))) else null
    val hivePassword = cliParams.hivePassword.toOption.getOrElse(new String(console.readPassword("Please enter Hive Password for Controller Service : ")))
    val hiveDRPassword = cliParams.drHivePassword.toOption.getOrElse(new String(console.readPassword("Please enter DR Hive Password for Controller Service : ")))

    val deployerConfiguration = new Configuration()
    deployerConfiguration.addResource(new FileInputStream(s"${cliParams.getConfigLocation}/${cliParams.getSource}_${cliParams.getCountry}_param.xml"))

    val fs = FileSystem.get(deployerConfiguration)

    if (!cliParams.isTemplateOnlyDeployment) {
      //Create NAS mount directory
      prepareNASDirectory(deployerConfiguration, cliParams.getConfigLocation, cliParams.getSource, cliParams.getCountry)

      //Create HDFSDirectories
      prepareHDFSDirectories(cliParams.getSource, cliParams.getCountry, cliParams.getConfigLocation, deployerConfiguration, fs)

      //Create SRI Open, Non Open, Storage and Ops tables
      createHiveTables(deployerConfiguration, fs, cliParams.getSource, cliParams.getCountry)
    }

    //Upload Nifi Template
    uploadNifiTemplate(cliParams, deployerConfiguration, nifiPassword, hivePassword, hiveDRPassword)
  }

  //FIXME Error folder????
  /** Creates the required NAS mounts/local folders
    * Includes
    * 1. Input file incoming folder
    * 2. Archival folder
    * 3. Rerun incoming folder
    * 4. Reject folder
    * 5. Error folder
    * 6. Application files folder (scripts, config files)
    *
    * @param primConfig Hadoop configuration
    */
  private def prepareNASDirectory(primConfig: Configuration, inputConfigLocation: String, source: String, country: String) = {

    val incomingLocation = primConfig.get("edmhdpif.config.source.nfs.incoming.location")
    val archivalLocation = primConfig.get("edmhdpif.config.source.nfs.archival.location")
    val reRunLocation = primConfig.get("edmhdpif.config.source.nfs.rerun.location")
    val rejectLocation = primConfig.get("edmhdpif.config.source.nfs.reject.location")
    val errorLocation = primConfig.get("edmhdpif.config.source.nfs.error.location")
    val applLocation = primConfig.get("edmhdpif.config.source.nfs.appl.location")
    val configLocation = primConfig.get("edmhdpif.config.source.nfs.config.location")

    new File(s"$incomingLocation").mkdirs()
    new File(s"$archivalLocation").mkdirs()
    new File(s"$reRunLocation").mkdirs()
    new File(s"$rejectLocation").mkdirs()
    new File(s"$errorLocation").mkdirs()
    new File(s"$applLocation").mkdirs()

    FileUtils.deleteDirectory(new File(configLocation))

    new File(s"$configLocation").mkdirs()
    FileUtils.copyFile(new File(inputConfigLocation, s"/${source}_${country}_param.xml"), new File(configLocation, s"/${source}_${country}_param.xml"))
    FileUtils.copyFile(new File(inputConfigLocation, s"/${source}_${country}_tables_config.xml"), new File(configLocation, s"/${source}_${country}_tables_config.xml"))
    FileUtils.copyFile(new File(inputConfigLocation, s"/${source}_${country}_tables_rename.xml"), new File(configLocation, s"/${source}_${country}_tables_rename.xml"))
    FileUtils.copyFile(new File(inputConfigLocation, "ColumnTransformationRules.xml"), new File(configLocation, "ColumnTransformationRules.xml"))

    println("Successfully created NAS Directories locations")
  }

  /** Creates HDFS directories
    * Includes
    * 1. HDFS data parent directory - root of storage and sri hdfs directories
    * 2. Verify types/Storage folder
    * 3. Sri Open folder
    * 4. Sri NonOpen folder
    * 5. Ops folder
    * 6. Config location in HDFS
    *
    * @param source
    * @param country
    * @param configLocation
    * @param primConfig
    * @param fs
    */
  private def prepareHDFSDirectories(source: String, country: String, configLocation: String, primConfig: Configuration, fs: FileSystem) = {

    val parentPath = primConfig.get("edmhdpif.config.hdfs.data.parent.dir")
    val verifyTypesSchema = primConfig.get("edmhdpif.config.storage.schema")
    val sriOpenSchema = primConfig.get("edmhdpif.config.sri.open.schema")
    val sriNonOpenSchema = primConfig.get("edmhdpif.config.sri.nonopen.schema")
    val opsSchemaName = primConfig.get("edmhdpif.config.ops.schema")
    val hdfsConfigDir = primConfig.get("edmhdpif.config.hdfs.config.dir")

    fs.mkdirs(new Path(s"hdfs://$parentPath/$verifyTypesSchema"))
    fs.mkdirs(new Path(s"hdfs://$parentPath/$sriOpenSchema"))
    fs.mkdirs(new Path(s"hdfs://$parentPath/$sriNonOpenSchema"))
    fs.mkdirs(new Path(s"hdfs://$parentPath/$opsSchemaName"))
    fs.mkdirs(new Path(s"hdfs://$hdfsConfigDir"))
    println("Successfully created NAS Directories locations")
    fs.delete(new Path(s"hdfs://${hdfsConfigDir}/${source}_${country}_*.xml"), false)

    fs.copyFromLocalFile(new Path(configLocation + s"/${source}_${country}_param.xml"), new Path(s"hdfs://${hdfsConfigDir}/${source}_${country}_param.xml"))
    fs.copyFromLocalFile(new Path(configLocation + s"/${source}_${country}_tables_config.xml"), new Path(s"hdfs://${hdfsConfigDir}/${source}_${country}_tables_config.xml"))
    println(s"Successfully uploaded configs to hdfs location: ${hdfsConfigDir}")
  }

  /** Invokes the [[SchemaUtils]] for creation of Hive tables after parsing the param and table configs
    *
    * @param primConfig HDFS configuration
    * @param fs         HDFS filesystem
    * @param source     Source application name of this deployment
    * @param country    Country/Region of this deployment
    */
  private def createHiveTables(primConfig: Configuration, fs: FileSystem, source: String, country: String) = {

    val sparkConf = new SparkConf().setAppName(s"SDMDeployer-${source}-${country}")
    val sc = new SparkContext(sparkConf)
    val sqlContext = new HiveContext(sc)

    val sriParams = SriParams(primConfig)
    val tableConfigs = XmlParser.parseXmlConfig(sriParams.tableConfigXml, fs)

    //Build OPS Tables
    queryExecutor(SchemaUtils.getOpsSchema(sriParams), sqlContext)

    //Build Storage, Open and Non Open Tables
    tableConfigs.par.foreach { config =>
      queryExecutor(SchemaUtils.getBusinessTablesSchema(config, sriParams), sqlContext)
    }

    //Build Data directory
    SchemaUtils.populateCommonTables(tableConfigs, sqlContext, sriParams, fs)
  }

  /** Executes a list of queries, given a SQLContext **/
  private def queryExecutor(items: List[String], sqlContext: HiveContext) = {
    items.filter(x => StringUtils.isNotEmpty(x.trim))
      .foreach { query =>
        println(s"Executing query :$query")
        sqlContext.sql(query)
      }
  }

  /**
    * Upload template to Nifi after replacing all the placeholders.
    * The process will also instantiate the template and initialize and enable all the controller services associated to that Nifi Process Group.
    *
    * @param cliParams
    * @param deployerConfiguration
    * @param nifiPassword
    * @param hivePassword
    * @param hiveDRPassword
    */
  private def uploadNifiTemplate(cliParams: SDMDeployerCli, deployerConfiguration: Configuration, nifiPassword: String, hivePassword: String, hiveDRPassword: String) = {

    val rootProcessorGroupId = "root"

    val client = new ApiClient(Map[String, Authentication]("oAuth" -> new OAuth)).setBasePath(cliParams.getNifiUrl)
    io.swagger.client.Configuration.setDefaultApiClient(client)
    client.getHttpClient.setReadTimeout(0, TimeUnit.MILLISECONDS)
    client.getHttpClient.setConnectTimeout(0, TimeUnit.MILLISECONDS)

    val flowApi = new FlowApi()
    val templatesApi = new TemplatesApi()
    val processGroupsApi = new ProcessgroupsApi()
    val controllerServicesApi = new ControllerservicesApi()

    createTemplate(cliParams, createPropertiesResolutionMap(cliParams, deployerConfiguration, hivePassword, hiveDRPassword))
    println(s"Replaced placeholders in the nifi template file and created final file: ${getTemplateFileName(cliParams)}")

    if (cliParams.isNifiAuthenticationEnabled) {
      client.setAccessToken(new AccessApi().createAccessToken(cliParams.getNifiUsername, nifiPassword))
      println("Successfully Logged in to nifi and set acccess token")
    }

    val processGroupEntity = new ProcessGroupEntity().revision(new RevisionDTO().version(0l)).component(new ProcessGroupDTO().name(s"SDM-${cliParams.getSource}"))
    val pgDetails = flowApi.getFlow(rootProcessorGroupId).getProcessGroupFlow.getFlow.getProcessGroups.map(o => (o.getComponent.getName, o.getComponent.getId)).toMap
    val sourceGroupId = if (pgDetails.contains((s"SDM-${cliParams.getSource}"))) pgDetails(s"SDM-${cliParams.getSource}") else processGroupsApi.createProcessGroup(rootProcessorGroupId, processGroupEntity).getId
    processGroupsApi.uploadTemplate(rootProcessorGroupId, new File(cliParams.getConfigLocation, getTemplateFileName(cliParams)))
    println(s"Uploaded Template XML to the Nifi Process Group : SDM-${cliParams.getSource} with process group id : ${sourceGroupId}")

    flowApi.getTemplates().getTemplates().filter(_.getTemplate.getName == s"SDM-${cliParams.getSource}-${cliParams.getCountry}").foreach { availableTemplate =>

      val inputTemplate = availableTemplate.getTemplate()
      val body = new InstantiateTemplateRequestEntity()
        .templateId(inputTemplate.getId())
        .originX(new Random().nextInt(100).toDouble)
        .originY(new Random().nextInt(100).toDouble)

      val result = processGroupsApi.instantiateTemplate(sourceGroupId, body)
      val processGroupId = result.getFlow.getProcessGroups.get(0).getId
      println(s"Successfully Instantiated the Process Group : ${inputTemplate.getName} with process group id : ${processGroupId}")

      val distributedMapCacheServer = new ControllerServiceEntity()
        .revision(new RevisionDTO().version(0l))
        .component(new ControllerServiceDTO().`type`("org.apache.nifi.distributed.cache.server.map.DistributedMapCacheServer").properties(Map("Port" -> cliParams.getDistributedMapCachePort)))

      processGroupsApi.createControllerService(processGroupId, distributedMapCacheServer)

      flowApi.getControllerServicesFromGroup(processGroupId).getControllerServices.filter(processGroupId == _.getComponent.getParentGroupId)
        .foreach { controllerService =>
          val controllerServiceName = controllerService.getComponent.getName
          println(s"Enabling Controller Service : ${controllerServiceName}")
          controllerService.getComponent.setState(ControllerServiceDTO.StateEnum.ENABLED)
          if ("HiveConnectionPool" == controllerServiceName) {
            controllerService.getComponent.setProperties(Map("hive-db-password" -> hivePassword))
          } else if ("DRHiveConnectionPool" == controllerServiceName) {
            controllerService.getComponent.setProperties(Map("hive-db-password" -> hiveDRPassword))
          }
          controllerServicesApi.updateControllerService(controllerService.getId, controllerService)
        }
      templatesApi.removeTemplate(inputTemplate.getId())
    }

    FileUtils.deleteQuietly(new File(cliParams.getConfigLocation, getTemplateFileName(cliParams)))
    println(s"Deployment for ${cliParams.getSource} - ${cliParams.getCountry} completed")
  }

  /**
    * Merge different data structures to create a placeholder map used for nifi template placeholder replacements.
    *
    * @param cliParams
    * @param deployerConfiguration
    * @param hivePassword
    * @param hiveDRPassword
    * @return
    */
  private def createPropertiesResolutionMap(cliParams: SDMDeployerCli, deployerConfiguration: Configuration, hivePassword: String, hiveDRPassword: String): Map[String, String] = {
    deployerConfiguration
      .filter(entry => entry.getKey.startsWith("edmhdpif.config"))
      .map(entry => (entry.getKey.replace("edmhdpif.config.", ""), entry.getValue)).toMap ++
      cliParams.getPropertyResolutionMap ++ Map(
      "source" -> cliParams.getSource,
      "country" -> cliParams.getCountry,
      "template.name" -> (s"SDM-${cliParams.getSource}-${cliParams.getCountry}"),
      "distributed.map.cache.port" -> cliParams.getDistributedMapCachePort,
      "hive.password" -> hivePassword,
      "hive.dr.password" -> hiveDRPassword
    )
  }

  /**
    * Update Nifi Template xml which has placeholders with proper filled in values.
    * Format for placeholder: ##<placeholder>##
    *
    * @param cliParams      input params passed to program to create intermitent file
    * @param placeholderMap placeholderMap used for template placeholders replacement
    */
  private def createTemplate(cliParams: SDMDeployerCli, placeholderMap: Map[String, String]) = {

    val writer = new PrintWriter(new File(cliParams.getConfigLocation, getTemplateFileName(cliParams)))
    Source.fromFile(new File(cliParams.getConfigLocation, "SDM-Template.xml")).getLines().foreach { line =>
      var updatedLine = line
      placeholderMap.foreach(x => updatedLine = updatedLine.replaceAll(s"##${x._1}##", x._2))
      writer.println(updatedLine)
    }
    writer.flush()
    writer.close()
  }

  private def getTemplateFileName(cliParams: SDMDeployerCli) = {
    s"SDM-Template-${cliParams.getSource}-${cliParams.getCountry}.xml"
  }
}