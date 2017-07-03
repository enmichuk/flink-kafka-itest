package main

import java.util.Properties

import grizzled.slf4j.Logging
import kafka.consumer.{Consumer, ConsumerConfig, ConsumerTimeoutException}
import org.apache.flink.configuration.ConfigConstants._
import org.apache.flink.configuration.Configuration
import org.apache.flink.configuration.TaskManagerOptions._
import org.apache.flink.runtime.minicluster.LocalFlinkMiniCluster
import org.apache.flink.streaming.util.TestStreamEnvironment
import org.apache.flink.util.InstantiationUtil
import org.apache.kafka.clients.producer.ProducerConfig._
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.serialization.ByteArraySerializer
import org.scalatest.concurrent.Eventually
import org.scalatest.time.{Millis, Seconds, Span}
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}

import scala.collection.JavaConverters._
import scala.concurrent.duration.{FiniteDuration, _}
import scala.util.{Failure, Success, Try}

trait FlinkKafkaTestBase extends FlatSpec with Matchers with Eventually
  with BeforeAndAfterAll with Logging {

  import FlinkKafkaTestBase._

  var flink: LocalFlinkMiniCluster = _
  var kafkaServer: KafkaTestEnvironment = _
  protected var brokerConnectionStrings: String = _
  protected var standardProps: Properties = _
  implicit val defaultWaitingTime = Span(30, Seconds)
  implicit override val patienceConfig = PatienceConfig(
    timeout = scaled(defaultWaitingTime),
    interval = scaled(Span(500, Millis)))
  private lazy val producer: KafkaProducer[String, Array[Byte]] = {
    val config = Map[String, AnyRef](
      BOOTSTRAP_SERVERS_CONFIG -> brokerConnectionStrings,
      ACKS_CONFIG -> "1",
      KEY_SERIALIZER_CLASS_CONFIG -> classOf[ByteArraySerializer].getName,
      VALUE_SERIALIZER_CLASS_CONFIG -> classOf[ByteArraySerializer].getName
    ).asJava

    new KafkaProducer[String, Array[Byte]](config)
  }

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

  def writeToTopic(topic: String, data: Array[Byte]): Unit = {
    producer.send(new ProducerRecord(topic, null, data)).get()
  }

  def readFromTopic(groupId: String, topic: String,
    autoOffsetReset: String = "largest", timeout: FiniteDuration = 5.seconds): Seq[Array[Byte]] = {

    val props = new java.util.Properties
    props.setProperty("group.id", groupId)
    props.setProperty("zookeeper.connect", kafkaServer.getZookeeperConnectionString)
    props.setProperty("consumer.timeout.ms", timeout.toMillis.toString)
    props.setProperty("auto.offset.reset", autoOffsetReset)

    val consumer = Consumer.create(new ConsumerConfig(props))
    val streams = consumer.createMessageStreams(Map(topic -> 1))
    val messageIterator = streams(topic).head.iterator()

    val messageTries = Iterator
      .continually()
      .map { _ => Try(messageIterator.next().message()) }
      .takeWhile {
        case Success(_) => true
        case Failure(_: ConsumerTimeoutException) => false
        case _ => throw new IllegalStateException("Exception occurred when reading from Kafka topic")
      }
      .toList

    consumer.shutdown()

    messageTries map (_.get)
  }

}

object FlinkKafkaTestBase {
  val NumberOfKafkaServers = 1
  val TaskManagersNumber = 1
  val TaskSlotNumber = 8
  val Parallelism: Int = TaskManagersNumber * TaskSlotNumber
}
