package com.example

import org.apache.flink.api.common.JobID
import org.apache.flink.client.program.ClusterClient
import org.apache.flink.configuration.{AkkaOptions, ConfigConstants, Configuration, TaskManagerOptions}
import org.apache.flink.test.util.{MiniClusterResource, MiniClusterResourceConfiguration}
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, FunSuite}
import org.scalatest.time.SpanSugar._

import scala.collection.JavaConverters._


trait FlinkTest extends FunSuite with BeforeAndAfterAll with BeforeAndAfterEach {

  val streamingTimeout = 60.seconds

  var flink: MiniClusterResource = _
  var flinkClient: ClusterClient[_] = _

  override def beforeAll(): Unit = {

    val fConfig = new Configuration()
    fConfig.setString(AkkaOptions.WATCH_HEARTBEAT_PAUSE, "5 s")
    fConfig.setString(AkkaOptions.WATCH_HEARTBEAT_INTERVAL, "1 s")
    fConfig.setString(TaskManagerOptions.MANAGED_MEMORY_SIZE, "16m")
    fConfig.setString(ConfigConstants.RESTART_STRATEGY_FIXED_DELAY_DELAY, "0 s")

    flink = new MiniClusterResource(
      new MiniClusterResourceConfiguration.Builder()
        .setConfiguration(fConfig)
        .setNumberTaskManagers(1)
        .setNumberSlotsPerTaskManager(8).build())

    flink.before()
    flinkClient = flink.getClusterClient
  }

  override def afterAll(): Unit = {
    flink.after()
    super.afterAll()
  }

  override def beforeEach(): Unit = {
    waitUntilNoJobIsRunning(flinkClient)
  }

  def waitUntilJobIsRunning(client: ClusterClient[_]): Unit = {
    while (getRunningJobs(client).isEmpty) {
      Thread.sleep(50)
    }
  }

  def waitUntilNoJobIsRunning(client: ClusterClient[_]): Unit = {
    while (!getRunningJobs(client).isEmpty) {
      Thread.sleep(50)
      cancelRunningJobs(client)
    }
  }

  def getRunningJobs(client: ClusterClient[_]): Seq[JobID] = {
    val statusMessages= client.listJobs.get
    statusMessages.asScala.filter(!_.getJobState.isGloballyTerminalState).map(_.getJobId).toSeq
  }

  def cancelRunningJobs(client: ClusterClient[_]): Unit = {
    val runningJobs = getRunningJobs(client)
    runningJobs.foreach { job =>
      client.cancel(job)
    }
  }
}
