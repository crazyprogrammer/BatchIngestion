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

import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfterAll, Suite}

/** A minor extension of `SharedSparkContext` from the spark testing library to set parallelism */
trait SdmSharedSparkContext extends BeforeAndAfterAll {
  self: Suite =>

  @transient private var _spark: SparkSession = _
  def spark: SparkSession = _spark

  override def beforeAll() {
    _spark = SparkSession.builder()
      .appName("Test Context")
      .enableHiveSupport()
      .master("local[*]")
      .getOrCreate()

    _spark.conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    _spark.conf.set("spark.kryo.registrationRequired", "true")
    _spark.conf.set("mapreduce.fileoutputcommitter.algorithm.version", "2")
    _spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")
    _spark.conf.set("spark.driver.extraJavaOptions", "-XX:+UseG1GC")
    _spark.conf.set("spark.executor.extraJavaOptions", "--XX:+UseG1GC")
    _spark.conf.set("spark.scheduler.mode", "FAIR")
    _spark.conf.set("spark.sql.shuffle.partitions", "1")
    _spark.conf.set("spark.default.parallelism", "1")
    _spark.sparkContext.setLogLevel("ERROR")
  }
}
