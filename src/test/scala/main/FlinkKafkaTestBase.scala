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
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord, RecordMetadata}
import org.apache.kafka.common.serialization.ByteArraySerializer
import org.scalatest.concurrent.Eventually
import org.scalatest.time.{Millis, Seconds, Span}
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.FiniteDuration
import scala.concurrent.{Await, Future}
import scala.util.{Failure, Success, Try}
import scala.concurrent.duration._

trait FlinkKafkaTestBase extends FlatSpec with Matchers with Eventually
  with BeforeAndAfterAll with Logging {

  import FlinkKafkaTestBase._
  import KafkaOffsetReset._

  var flink: LocalFlinkMiniCluster = _
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

    flink = new LocalFlinkMiniCluster(getFlinkConfiguration, useSingleActorSystem = false)
    flink.start()
  }

  def withFlinkEnv(fn: StreamExecutionEnvironment => Unit): Future[Unit] = {
    val f = Future{
      val env = StreamExecutionEnvironment.getExecutionEnvironment
      env.setStateBackend(new MemoryStateBackend)
      fn(env)
    }

    f.onComplete {
      case Failure(e) if e.isInstanceOf[JobCancellationException] => info(e.getMessage)
      case Failure(e) if e.isInstanceOf[JobTimeoutException] => info(e.getMessage)
      case Failure(e) => error("Error while executing job", e)
      case _ =>
    }

    f
  }

  def shutdownClusters(): Unit = {
    if (flink != null) {
      flink.stop()
    }

    kafkaServer.shutdown()
  }

  def cancelCurrentJob(name: String): Unit = {
    var status: JobStatusMessage = null
    val askTimeout = new FiniteDuration(30, TimeUnit.SECONDS)
    val jobManager = flink.getLeaderGateway(askTimeout)

    for (i <- 0 to 200) {
      // find the jobID
      val listResponse = jobManager.ask(JobManagerMessages.getRequestRunningJobsStatus, askTimeout)

      var jobs: List[JobStatusMessage] = null
      try {
        val result = Await.result(listResponse, askTimeout)
        jobs = result.asInstanceOf[JobManagerMessages.RunningJobsStatus].getStatusMessages().toList
      } catch {
        case e: Exception =>
          throw new Exception("Could not cancel job - failed to retrieve running jobs from the JobManager.", e);
      }

      if (jobs.isEmpty) {
        // try again, fall through the loop
        Thread.sleep(50)
      }
      else if (jobs.size == 1) {
        status = jobs.get(0)
      }
      else if (name != null) {
        for (msg <- jobs) {
          if (msg.getJobName.equals(name)) {
            status = msg
          }
        }
        if (status == null) {
          throw new Exception("Could not cancel job - no job matched expected name = '" + name + "' in " + jobs)
        }
      } else {
        var jobNames: String = ""
        for (jsm <- jobs) {
          jobNames += jsm.getJobName + ", "
        }
        throw new Exception("Could not cancel job - more than one running job: " + jobNames)
      }
    }

    if (status == null) {
//      throw new Exception("Could not cancel job - no running jobs")
    }
    else if (status.getJobState.isGloballyTerminalState) {
//      throw new Exception("Could not cancel job - job is not running any more")
    } else {
      val jobId = status.getJobId

      val response = jobManager.ask(JobManagerMessages.CancelJob(jobId), askTimeout)
      try {
        Await.result(response, askTimeout) match {
          case CancellationSuccess(_, _) => info(s"Job $jobId is successfully cancelled")
          case CancellationFailure(_, cause) => info(s"Job $jobId cancellation failed", cause)
        }
      } catch {
        case e: Exception =>
          throw new Exception("Sending the 'cancel' message failed.", e)
      }
    }
    waitUntilNoJobIsRunning()
  }

  def waitUntilNoJobIsRunning(timeout: FiniteDuration = new FiniteDuration(30, TimeUnit.SECONDS)): Unit = {
    val jobManager = flink.getLeaderGateway(timeout)
    while (true) {
      val listResponse = jobManager.ask(JobManagerMessages.getRequestRunningJobsStatus, timeout)

      val result = Await.result(listResponse, timeout)
      val jobs = result.asInstanceOf[JobManagerMessages.RunningJobsStatus].getStatusMessages().toList

      if (jobs.isEmpty) {
        return
      }

      Thread.sleep(50)
    }
  }


  def createTestTopic(topic: String, numberOfPartitions: Int, replicationFactor: Int): Unit = {
    kafkaServer.createTestTopic(topic, numberOfPartitions, replicationFactor, new Properties())
  }

  def deleteTestTopic(topic: String): Unit = {
    kafkaServer.deleteTestTopic(topic)
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

  def ensureTopicIsEmptyForGroup(group: String, topic: String): Unit = {
    eventually {
      readFromTopic(group, topic) shouldBe empty
    }
    info(s"Topic [$topic] has no records from group [$group]")
  }

}

object FlinkKafkaTestBase {
  val NumberOfKafkaServers = 1
  val TaskManagersNumber = 1
  val TaskSlotNumber = 8
  val Parallelism: Int = TaskManagersNumber * TaskSlotNumber
}
