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

package com.sc.sdm.models

import java.sql.Timestamp

object OpsModels {

  //TODO Expand this to support multiple checksums
  /** Reconciliation result record for each table comparing the rowCount and checksum/aggregated column value
    * The [[org.apache.spark.sql.DataFrame]] that's constructed out of this would be saved as the data for the recon table
    */
  case class EdmRecon(source: String, country: String, sourceType: String,
                      tableName: String, sourceCount: BigDecimal, targetCount: BigDecimal,
                      checksumFieldName: String, sourceChecksumValue: Float, targetChecksumValue: Float,
                      countReconStatus: String, checksumReconStatus: String, insertTime: Timestamp, batch_date: String)

  /** If Table Level EOD is configured, then an instance of this class would represent the marker time for each table **/
  case class TableLevelEodMarkers(tableName: String, marker: String)

  /** EOD Record for a job that is stored away for audit purposes **/
  case class EODTableRecord(source: String, country: String, markerTime: String, businessDate: String, previousBusinessDate: String)

  /** Instance of a Rowcount for a table. There are multiple instances for a single table based on the
    * granularity of the counts taken - VerifyTypes, Duplicates, SriOpen, NonOpen.
    * The [[org.apache.spark.sql.DataFrame]] that's constructed out of this would be saved as the data for the rowcount table
    */
  case class RowCount(schemaName: String, tableName: String, fileName: String, rowCounts: Long,
                      functionalTableName: String, asof: String, stepName: String, attemptId: String)


  /** A short representation of a metadata event **/
  case class OpsEvent(time: String, event: String)

  /** Instance of a process me metadata event
    * The [[org.apache.spark.sql.DataFrame]] that's constructed out of this would be saved as the data for the Process Metadata table
    */
  case class ProcessMetadataEvent(currentTime: Timestamp, partitionName: String, tableName: String, status: String, description: String,
                                  executionTime: String, actionName: String, fileName: String, runType: String, batchDate: String,
                                  jsonAttribute: String, source: String, country: String, module: String)
}
