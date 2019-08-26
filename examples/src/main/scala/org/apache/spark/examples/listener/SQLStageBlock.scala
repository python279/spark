/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.spark.examples.listener

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.internal.Logging
import org.apache.spark.scheduler._
import org.apache.spark.sql.execution.SQLExecution
import org.apache.spark.sql.execution.metric._
import org.apache.spark.sql.execution.ui.{SparkListenerSQLExecutionEnd, SparkListenerSQLExecutionStart}
import org.apache.spark.sql.execution.ui.SparkListenerDriverAccumUpdates
import org.apache.spark.status.api.v1.StageStatus


class SQLStageBlock(conf: SparkConf) extends SparkListener with Logging {

  private val stageMaxTasks = conf.getLong("spark.sql.execution.stage.maxtasks", 5000)

  override def onJobStart(event: SparkListenerJobStart): Unit = {
    event.stageInfos.foreach(stageInfo => {
      if (stageInfo.numTasks >= stageMaxTasks) {
        val msg = printf("number of the job %d stage %d's tasks %d exceeded threshold %d",
          event.jobId, stageInfo.stageId, stageInfo.numTasks, stageMaxTasks)
        logError(msg.toString)
        val sc = SparkContext.getActive
        sc.foreach(_.cancelJob(event.jobId, msg.toString))
      }
    })
  }

  override def onJobEnd(event: SparkListenerJobEnd): Unit = {
    return
  }

  override def onStageSubmitted(event: SparkListenerStageSubmitted): Unit = {
    if (event.stageInfo.numTasks >= stageMaxTasks) {
      val msg = printf("current stage numTasks %d exceeded threshold %d",
        event.stageInfo.numTasks, stageMaxTasks)
      logError(msg.toString)
      val stageId = event.stageInfo.stageId
      val sc = SparkContext.getActive
      sc.foreach(_.cancelStage(stageId, msg.toString))
    }
  }

  override def onStageCompleted(event: SparkListenerStageCompleted): Unit = {
    return
  }

  override def onExecutorMetricsUpdate(event: SparkListenerExecutorMetricsUpdate): Unit = {
    return
  }

  override def onTaskStart(event: SparkListenerTaskStart): Unit = {
    return
  }

  override def onTaskEnd(event: SparkListenerTaskEnd): Unit = {
    return
  }

  private def onExecutionStart(event: SparkListenerSQLExecutionStart): Unit = {
    return
  }

  private def onExecutionEnd(event: SparkListenerSQLExecutionEnd): Unit = {
    return
  }

  private def onDriverAccumUpdates(event: SparkListenerDriverAccumUpdates): Unit = {
    return
  }

  override def onOtherEvent(event: SparkListenerEvent): Unit = event match {
    case e: SparkListenerSQLExecutionStart => onExecutionStart(e)
    case e: SparkListenerSQLExecutionEnd => onExecutionEnd(e)
    case e: SparkListenerDriverAccumUpdates => onDriverAccumUpdates(e)
    case _ => // Ignore
  }
}

