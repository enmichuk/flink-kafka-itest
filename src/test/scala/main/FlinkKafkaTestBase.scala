package main

import java.util.Properties
import java.util.concurrent.TimeUnit

import grizzled.slf4j.Logging
import kafka.consumer.{Consumer, ConsumerConfig, ConsumerTimeoutException}
import org.apache.flink.configuration.ConfigConstants._
import org.apache.flink.configuration.Configuration
import org.apache.flink.configuration.TaskManagerOptions._
import org.apache.flink.runtime.client.{JobCancellationException, JobStatusMessage, JobTimeoutException}
import org.apache.flink.runtime.messages.JobManagerMessages
import org.apache.flink.runtime.messages.JobManagerMessages.{CancellationFailure, CancellationSuccess}
import org.apache.flink.runtime.minicluster.LocalFlinkMiniCluster
import org.apache.flink.runtime.state.memory.MemoryStateBackend
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.util.TestStreamEnvironment
import org.apache.flink.util.InstantiationUtil
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord, RecordMetadata}
import org.apache.kafka.common.serialization.ByteArraySerializer
import org.scalatest.concurrent.Eventually
import org.scalatest.time.{Millis, Seconds, Span}
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.{FiniteDuration, _}
import scala.concurrent.{Await, Future}
import scala.util.{Failure, Success, Try}

trait FlinkKafkaTestBase extends FlatSpec with Matchers with Eventually
  with BeforeAndAfterAll with Logging {

  import FlinkKafkaTestBase._
  import KafkaOffsetReset._

  var flinkCluster: LocalFlinkMiniCluster = _
  var kafkaServer: KafkaTestEnvironment = _
  protected var brokerConnectionString: String = _
  protected var zookeeperConnectionString: String = _
  protected var standardProps: Properties = _
  implicit val defaultWaitingTime = Span(30, Seconds)
  implicit override val patienceConfig = PatienceConfig(
    timeout = scaled(defaultWaitingTime),
    interval = scaled(Span(500, Millis)))
  private lazy val producer: KafkaProducer[String, Array[Byte]] = {
    import org.apache.kafka.clients.producer.ProducerConfig._
    val config = Map[String, AnyRef](
      BOOTSTRAP_SERVERS_CONFIG -> brokerConnectionString,
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

    TestStreamEnvironment.setAsContext(flinkCluster, Parallelism)

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
    flinkConfig.setLong(MANAGED_MEMORY_SIZE, 128L)
    flinkConfig.setString(RESTART_STRATEGY_FIXED_DELAY_DELAY, "0 s")
    flinkConfig
  }

  def startClusters(): Unit = {
    kafkaServer = InstantiationUtil.instantiate(classOf[KafkaTestEnvironment])

    info("Starting KafkaTestBase.prepare() for Kafka " + kafkaServer.version)

    kafkaServer.prepare(NumberOfKafkaServers)

    standardProps = kafkaServer.getStandardProperties

    brokerConnectionString = kafkaServer.getBrokerConnectionString

    zookeeperConnectionString = kafkaServer.getZookeeperConnectionString

    flinkCluster = new LocalFlinkMiniCluster(getFlinkConfiguration, useSingleActorSystem = false)
    flinkCluster.start()
  }

  def withFlinkEnv(fn: StreamExecutionEnvironment => Unit): Future[Unit] = {
    val f = Future{
      val env = StreamExecutionEnvironment.getExecutionEnvironment
      env.setStateBackend(new MemoryStateBackend)
      fn(env)
    }

    f.onFailure {
      case _: JobTimeoutException =>
      case _: JobCancellationException =>
      case e => error("Error while executing job", e)
    }

    f
  }

  def shutdownClusters(): Unit = {
    if (flinkCluster != null) {
      flinkCluster.stop()
    }

    kafkaServer.shutdown()
  }

  def cancelCurrentJob(): Unit = {
    var jobList: List[JobStatusMessage] = List()
    val askTimeout = new FiniteDuration(30, TimeUnit.SECONDS)
    val jobManager = flinkCluster.getLeaderGateway(askTimeout)
    var i = 0

    while(jobList.isEmpty && i < 200) {
      i += 1
      val listResponse = jobManager.ask(JobManagerMessages.getRequestRunningJobsStatus, askTimeout)

      val jobs = try {
        val result = Await.result(listResponse, askTimeout)
        result.asInstanceOf[JobManagerMessages.RunningJobsStatus].getStatusMessages().toList
      } catch {
        case e: Exception =>
          throw new Exception("Could not cancel job - failed to retrieve running jobs from the JobManager", e);
      }

      jobList = jobs

      Thread.sleep(50)
    }

    jobList.foreach { job =>
      val jobId = job.getJobId

      val response = jobManager.ask(JobManagerMessages.CancelJob(jobId), askTimeout)
      try {
        Await.result(response, askTimeout) match {
          case CancellationSuccess(_, _) => info(s"Job $jobId is successfully cancelled")
          case CancellationFailure(_, cause) => info(s"Job $jobId cancellation failed", cause)
        }
      } catch {
        case e: Exception => throw new Exception("Sending cancel failed", e)
      }
    }

    waitUntilNoJobIsRunning()
  }

  def waitUntilNoJobIsRunning(timeout: FiniteDuration = new FiniteDuration(30, TimeUnit.SECONDS)): Unit = {
    val jobManager = flinkCluster.getLeaderGateway(timeout)
    while ( {
      val listResponse = jobManager.ask(JobManagerMessages.getRequestRunningJobsStatus, timeout)

      val result = Await.result(listResponse, timeout)
      val jobs = result.asInstanceOf[JobManagerMessages.RunningJobsStatus].getStatusMessages().toList

      if(jobs.nonEmpty) {
        info(s"Jobs [${jobs.map(_.getJobName).mkString(",")}] are in process")
      }

      jobs.nonEmpty
    }) {
      Thread.sleep(100)
    }
  }


  def createTestTopic(topic: String, numberOfPartitions: Int, replicationFactor: Int): Unit = {
    kafkaServer.createTestTopic(topic, numberOfPartitions, replicationFactor, new Properties())
  }

  def writeToTopic(topic: String, data: Array[Byte]): RecordMetadata = {
    producer.send(new ProducerRecord(topic, null, data)).get()
  }

  def readFromTopic(groupId: String, topic: String,
                    autoOffsetReset: String = Smallest, timeout: FiniteDuration = 1.seconds): Seq[Array[Byte]] = {
    import org.apache.kafka.clients.consumer.ConsumerConfig._

    val props = new java.util.Properties
    props.setProperty(GROUP_ID_CONFIG, groupId)
    props.setProperty("zookeeper.connect", kafkaServer.getZookeeperConnectionString)
    props.setProperty("consumer.timeout.ms", timeout.toMillis.toString)
    props.setProperty(AUTO_OFFSET_RESET_CONFIG, autoOffsetReset)

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
