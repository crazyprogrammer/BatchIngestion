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
package com.sc.sdm.models.cli

import org.rogach.scallop.ScallopConf

/**
  * Holder of Command Line params passed to SDM Deployer main method.
  * This class uses Scallop framework to help better handling on input params.
  *
  * @param arguments cli args passed to SDMDeployer program
  */
class SDMDeployerCli(arguments: Seq[String]) extends ScallopConf(arguments) {

  val source = opt[String](required = true, descr = "Source for which deployment is being done", argName = "source")
  val country = opt[String](required = true, descr = "Country for which deployment is being done", argName = "country")
  val configLocation = opt[String](required = true, descr = "Location on local file system where config files are available", argName = "configLocation")
  val nifiUrl = opt[String](required = true, descr = "Nifi REST URL for Template deployment. eg: http://localhost:8888/nifi-api", argName = "nifiUrl")
  val distributedMapCachePort = opt[String](required = true, argName = "distributedMapCachePort", descr = "Unique Port number for Distributed map cache controller service")
  val templateOnlyDeployment = toggle("template-only-deployment", default = Option(false), descrYes = "Only Nifi Template deployment for given source and country. Set this parameter for template upgrade", descrNo = "Complete Deployment for given source and country. To be used for first time deployment of given source and country. By default it will be full deployment.")
  val nifiAuthenticationEnabled = toggle("nifi-authentication-enabled", default = Option(false), descrYes = "Set this parameter for authenticated nifi instance", descrNo = "By default it will be unauthenticated instance without username / password")
  val nifiUsername = opt[String](argName = "nifiUserName", descr = "Nifi Username")
  val nifiPassword = opt[String](argName = "nifiPassword", descr = "Nifi Password")
  val hivePassword = opt[String](argName = "hivePassword", descr = "Hive Password")
  val drHivePassword = opt[String](argName = "drHivePassword", descr = "DR Hive Password")
  val propertyResolutionMap = opt[Map[String, String]](argName = "propertyResolutionMap", descr = "Additional properties to be used for template resolution.")

  override def toString = "SDMDeployer(" +
    s"\n\tsource=$getSource" +
    s"\n\tcountry=$getCountry" +
    s"\n\tconfigLocation=$getConfigLocation" +
    s"\n\tnifiUrl=$getNifiUrl" +
    s"\n\tnifiUsername=$getNifiUsername" +
    s"\n\tdistributedMapCachePort=$getDistributedMapCachePort" +
    s"\n\tpropertyResolutionMap=$getPropertyResolutionMap" +
    s"\n\ttemplateOnlyDeployment=$isTemplateOnlyDeployment" +
    s"\n\tnifiAuthenticationEnabled=$isNifiAuthenticationEnabled" +
    s"\n)"

  def getSource = source.toOption.get.trim

  def getCountry = country.toOption.get.trim

  def getConfigLocation = configLocation.toOption.get.trim

  def getNifiUrl = nifiUrl.toOption.get.trim

  def getNifiUsername = nifiUsername.toOption.get.trim

  def getDistributedMapCachePort = distributedMapCachePort.toOption.get.trim

  def isTemplateOnlyDeployment = templateOnlyDeployment.toOption.get

  def isNifiAuthenticationEnabled = nifiAuthenticationEnabled.toOption.get

  def getPropertyResolutionMap = propertyResolutionMap.toOption.getOrElse(Map())

  verify()

}