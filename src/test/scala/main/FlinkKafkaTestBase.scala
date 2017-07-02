package main

import java.util.Properties

import grizzled.slf4j.Logging
import org.apache.flink.configuration.ConfigConstants._
import org.apache.flink.configuration.Configuration
import org.apache.flink.configuration.TaskManagerOptions._
import org.apache.flink.runtime.minicluster.LocalFlinkMiniCluster
import org.apache.flink.util.InstantiationUtil
import org.apache.flink.streaming.util.TestStreamEnvironment
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}

trait FlinkKafkaTestBase extends FlatSpec with Matchers with BeforeAndAfterAll with Logging {

  import FlinkKafkaTestBase._

  var flink: LocalFlinkMiniCluster = _
  var kafkaServer: KafkaTestEnvironment = _
  protected var brokerConnectionStrings: String = _
  protected var standardProps: Properties = _

  override def beforeAll(): Unit = {
    delineate()
    info("Starting FlinkKafkaTestBase")

    startClusters()

    TestStreamEnvironment.setAsContext(flink, Parallelism)

    info("FlinkKafkaTestBase started")
    delineate()
  }

  override def afterAll(): Unit = {
    delineate()
    info("Shutdown FlinkKafkaTestBase")

    TestStreamEnvironment.unsetAsContext()

    shutdownClusters()

    info("FlinkKafkaTestBase finished")
    delineate()
  }

  def delineate(): Unit = {
    info("=" * 80)
  }

  def getFlinkConfiguration: Configuration = {
    val flinkConfig = new Configuration()
    flinkConfig.setInteger(LOCAL_NUMBER_TASK_MANAGER, TaskManagersNumber)
    flinkConfig.setInteger(TASK_MANAGER_NUM_TASK_SLOTS, TaskSlotNumber)
    flinkConfig.setLong(MANAGED_MEMORY_SIZE, 16L)
    flinkConfig.setString(RESTART_STRATEGY_FIXED_DELAY_DELAY, "0 s")
    flinkConfig
  }

  def startClusters(): Unit = {
    kafkaServer = InstantiationUtil.instantiate(classOf[KafkaTestEnvironment])

    info("Starting KafkaTestBase.prepare() for Kafka " + kafkaServer.version)

    kafkaServer.prepare(NumberOfKafkaServers)

    standardProps = kafkaServer.getStandardProperties

    brokerConnectionStrings = kafkaServer.getBrokerConnectionString

    flink = new LocalFlinkMiniCluster(getFlinkConfiguration, useSingleActorSystem = false)
    flink.start()
  }

  def shutdownClusters(): Unit = {
    if (flink != null) {
      flink.stop()
    }

    kafkaServer.shutdown()
  }


  def createTestTopic(topic: String, numberOfPartitions: Int, replicationFactor: Int): Unit = {
    kafkaServer.createTestTopic(topic, numberOfPartitions, replicationFactor, new Properties())
  }

  def deleteTestTopic(topic: String): Unit = {
    kafkaServer.deleteTestTopic(topic)
  }

}

object FlinkKafkaTestBase {
  val NumberOfKafkaServers = 1
  val TaskManagersNumber = 1
  val TaskSlotNumber = 8
  val Parallelism: Int = TaskManagersNumber * TaskSlotNumber
}
