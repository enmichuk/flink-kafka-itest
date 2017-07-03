package main

import java.io.File
import java.net.BindException
import java.util.{Properties, UUID}

import grizzled.slf4j.Logging
import kafka.admin.AdminUtils
import kafka.common.KafkaException
import kafka.server.{KafkaConfig, KafkaServer}
import kafka.utils.{SystemTime, ZkUtils}
import org.I0Itec.zkclient.ZkClient
import org.I0Itec.zkclient.serialize.ZkSerializer
import org.apache.curator.test.TestingServer
import org.apache.flink.configuration.ConfigConstants
import org.apache.flink.util.FileUtils
import org.apache.flink.util.NetUtils.{getAvailablePort, hostAndPortToUrlString}
import org.apache.kafka.common.protocol.SecurityProtocol
import org.scalatest.Assertions

import scala.collection.mutable.ListBuffer

class KafkaTestEnvironment extends Assertions with Logging {
  import KafkaTestEnvironment._

  private var tmpZkDir: File = _
  private var tmpKafkaParent: File = _
  private var tmpKafkaDirs: ListBuffer[File] = _
  private var brokers: ListBuffer[KafkaServer] = _
  private var zookeeper: TestingServer = _
  private var standardProps: Properties = _
  private var zookeeperConnectionString: String = _
  private var brokerConnectionString: String = _

  def version: String = "0.10"

  def getBrokerConnectionString: String = brokerConnectionString

  def getZookeeperConnectionString: String = zookeeperConnectionString

  def getStandardProperties: Properties = standardProps

  def prepare(numKafkaServers: Int): Unit = {
    val tempDir: File = new File(System.getProperty("java.io.tmpdir"))
    tmpZkDir = new File(tempDir, "kafkaITcase-zk-dir-" + UUID.randomUUID.toString)
    assert(tmpZkDir.mkdirs, "cannot create zookeeper temp dir")
    tmpKafkaParent = new File(tempDir, "kafkaITcase-kafka-dir-" + UUID.randomUUID.toString)
    assert(tmpKafkaParent.mkdirs, "cannot create kafka temp dir")
    tmpKafkaDirs = ListBuffer[File]()

    for (i <- 0 until numKafkaServers) yield {
      val tmpDir: File = new File(tmpKafkaParent, s"server-$i")
      assert(tmpDir.mkdir, "cannot create kafka temp dir")
      tmpKafkaDirs += tmpDir
    }

    zookeeper = null
    brokers = null

    try {
      zookeeper = new TestingServer(-1, tmpZkDir)
      zookeeperConnectionString = zookeeper.getConnectString
      info(s"Starting Zookeeper with zookeeperConnectionString: $zookeeperConnectionString")

      info("Starting KafkaServer")
      brokers = ListBuffer[KafkaServer]()

      for (i <- 0 until numKafkaServers) yield {
        brokers += getKafkaServer(i, tmpKafkaDirs(i))

        brokerConnectionString += hostAndPortToUrlString(KafkaHost, brokers(i).socketServer.boundPort()) + ","
      }
      info("ZK and KafkaServer started.")
    } catch {
      case t: Throwable =>
        error(t)
        fail("Test setup failed: " + t.getMessage)
    }
    standardProps = new Properties
    standardProps.setProperty("zookeeper.connect", zookeeperConnectionString)
    standardProps.setProperty("bootstrap.servers", brokerConnectionString)
    standardProps.setProperty("group.id", "flink-tests")
    standardProps.setProperty("enable.auto.commit", "false")
    standardProps.setProperty("zookeeper.session.timeout.ms", String.valueOf(ZkTimeout))
    standardProps.setProperty("zookeeper.connection.timeout.ms", String.valueOf(ZkTimeout))
    standardProps.setProperty("auto.offset.reset", "earliest") // read from the beginning. (earliest is kafka 0.10 value)
    standardProps.setProperty("max.partition.fetch.bytes", "256") // make a lot of fetches (MESSAGES MUST BE SMALLER!)
  }

  protected def getKafkaServer(brokerId: Int, tmpFolder: File): KafkaServer = {
    info(s"Starting broker with id $brokerId")
    val kafkaProperties: Properties = new Properties
    // properties have to be Strings
    kafkaProperties.put("advertised.host.name", KafkaHost)
    kafkaProperties.put("broker.id", Integer.toString(brokerId))
    kafkaProperties.put("log.dir", tmpFolder.toString)
    kafkaProperties.put("zookeeper.connect", zookeeperConnectionString)
    kafkaProperties.put("message.max.bytes", String.valueOf(50 * 1024 * 1024))
    kafkaProperties.put("replica.fetch.max.bytes", String.valueOf(50 * 1024 * 1024))
    // for CI stability, increase zookeeper session timeout
    kafkaProperties.put("zookeeper.session.timeout.ms", "30000")
    kafkaProperties.put("zookeeper.connection.timeout.ms", "30000")
    val numTries: Int = 5
    for (i <- 0 until numTries) {
      val kafkaPort: Int = getAvailablePort
      kafkaProperties.put("port", Integer.toString(kafkaPort))
      val kafkaConfig = new KafkaConfig(kafkaProperties)

      try {
        val server = new KafkaServer(kafkaConfig, SystemTime)
        server.startup
        return server
      } catch {
        case e: KafkaException =>
          e.getCause match {
            case ex: BindException =>
              // port conflict, retry...
              info("Port conflict when starting Kafka Broker. Retrying...")
            case _ => throw e
          }
      }
    }
    throw new Exception("Could not start Kafka after " + numTries + " retries due to port conflicts.")
  }

  def shutdown() {
    for (broker <- brokers) {
      if (broker != null) broker.shutdown
    }

    brokers.clear()

    if (zookeeper != null) {
      try {
        zookeeper.stop()
      } catch {
        case e: Exception => warn("ZK.stop() failed", e)
      }
      zookeeper = null
    }

    // clean up the temp spaces
    if (tmpKafkaParent != null && tmpKafkaParent.exists) {
      try {
        FileUtils.deleteDirectory(tmpKafkaParent)
      }
      catch {
        case e: Exception => // ignore
      }
    }

    if (tmpZkDir != null && tmpZkDir.exists) {
      try {
        FileUtils.deleteDirectory(tmpZkDir)
      }
      catch {
        case e: Exception => // ignore
      }
    }
  }

  def createTestTopic(topic: String, numberOfPartitions: Int, replicationFactor: Int, topicConfig: Properties) {
    info(s"Creating topic $topic")
    val zkUtils: ZkUtils = getZkUtils
    try {
      AdminUtils.createTopic(zkUtils, topic, numberOfPartitions, replicationFactor, topicConfig)
    } finally {
      zkUtils.close
    }

    // validate that the topic has been created
    val deadline: Long = System.nanoTime + 30000000000L
    do {
      try {
        Thread.sleep(100)
      } catch {
        case e: InterruptedException => // restore interrupted state
      }
      // we could use AdminUtils.topicExists(zkUtils, topic) here, but it's results are
      // not always correct.
      // create a new ZK utils connection
      val checkZKConn: ZkUtils = getZkUtils
      if (AdminUtils.topicExists(checkZKConn, topic)) {
        checkZKConn.close
        return
      }
      checkZKConn.close
    } while (System.nanoTime < deadline)
    fail("Test topic could not be created")
  }

  def deleteTestTopic(topic: String) {
    val zkUtils: ZkUtils = getZkUtils
    try {
      info(s"Deleting topic $topic")
      val zk = new ZkClient(zookeeperConnectionString, ZkTimeout, ZkTimeout, new ZooKeeperStringSerializer)
      AdminUtils.deleteTopic(zkUtils, topic)
      zk.close()
    } finally {
      zkUtils.close
    }
  }

  def getZkUtils: ZkUtils = {
    val creator: ZkClient = new ZkClient(zookeeperConnectionString, ZkTimeout, ZkTimeout, new ZooKeeperStringSerializer)
    ZkUtils(creator, isZkSecurityEnabled = false)
  }

}

class ZooKeeperStringSerializer extends ZkSerializer {
  def serialize(data: Any): Array[Byte] = data match {
    case s: String => s.getBytes(ConfigConstants.DEFAULT_CHARSET)
    case _ => throw new IllegalArgumentException("ZooKeeperStringSerializer can only serialize strings.")
  }

  def deserialize(bytes: Array[Byte]): AnyRef = {
    if (bytes == null) null
    else new String(bytes, ConfigConstants.DEFAULT_CHARSET)
  }
}

object KafkaTestEnvironment {
  val KafkaHost = "localhost"
  val ZkTimeout = 30000
}
