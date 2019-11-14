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

class SDMReconCli(arguments: Seq[String]) extends ScallopConf(arguments) {

  val source = opt[String](required = true, descr = "Source for which recon is being done", argName = "source")
  val country = opt[String](required = true, descr = "Country for which recon is being done", argName = "country")
  val businessDate = opt[String](required = true, descr = "Business date for which recon is being done", argName = "businessDate")
  val configLocation = opt[String](required = true, descr = "Location on local file system where config files are available", argName = "configLocation")

  def getBusinessDate = businessDate.toOption.get.trim

  override def toString = "SDMDeployer(" +
    s"\n\tsource=$getSource" +
    s"\n\tcountry=$getCountry" +
    s"\n\tconfigLocation=$getConfigLocation" +
    s"\n)"

  def getSource = source.toOption.get.trim

  def getCountry = country.toOption.get.trim

  def getConfigLocation = configLocation.toOption.get.trim

  verify()

}