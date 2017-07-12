package main

import java.util.UUID
import java.util.concurrent.atomic.AtomicInteger

import com.typesafe.config.ConfigFactory
import org.apache.flink.configuration.ConfigConstants
import org.apache.kafka.clients.producer.RecordMetadata
import org.scalatest.BeforeAndAfterEach
import org.scalatest.Inspectors._

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}

class FlinkKafkaJobITTest extends FlinkKafkaTestBase with BeforeAndAfterEach {

  import FlinkKafkaJob.Config._
  import FlinkKafkaJob._
  import FlinkKafkaJobITTest._

  private var currentInputTopicName: String = _
  private var currentOutputTopicName: String = _

  override def beforeEach(): Unit = {
    currentInputTopicName = generateInputTopicName
    currentOutputTopicName = generateOutputTopicName
    createTestTopic(currentInputTopicName, 1, 1)
    createTestTopic(currentOutputTopicName, 1, 1)
  }

  override def afterEach(): Unit = {
    cancelCurrentJob(JobName)
  }

  "FlinkKafkaJob" should "transform correctly message" in {
    val message = "message"
    writeToInputTopic(message)

    runFlinkKafkaJob()

    eventually {
      readFromOutputTopic() should contain (new TransformMapFunction().map(message))
    }
  }

  it should "transform correctly 100 messages" in {
    val size = 100

    val buffer =
      Await.result(
        Future {
          val buffer = ArrayBuffer[String]()
          Thread.sleep(2000)
          for (_ <- 0 until size) yield {
            val msg = UUID.randomUUID().toString
            buffer += msg
            writeToInputTopic(msg)
          }
          buffer
        },
        Duration.Inf
      )

    runFlinkKafkaJob()

    val bufferTransformed = buffer.map(new TransformMapFunction().map(_))

    val records = eventually {
      val records = readFromOutputTopic()
      records.size shouldBe size
      records
    }

    forAll(records) { record =>
      bufferTransformed should contain (record)
    }
  }

  def readFromOutputTopic(): Seq[String] = {
    readFromTopic(KafkaGroupId, currentOutputTopicName).map(new String(_, ConfigConstants.DEFAULT_CHARSET))
  }

  def writeToInputTopic(msg: String): RecordMetadata = {
    writeToTopic(currentInputTopicName, msg.getBytes(ConfigConstants.DEFAULT_CHARSET))
  }

  private def config = ConfigFactory.parseMap(Map(
    KafkaBrokersParam -> brokerConnectionString,
    InputTopicParam -> currentInputTopicName,
    OutputTopicParam -> currentOutputTopicName
  ).asJava).withFallback(ConfigFactory.load())

  private def runFlinkKafkaJob(): Future[Unit] = {
    withFlinkEnv { env => new FlinkKafkaJob().run(config, env) }
  }

}

object FlinkKafkaJobITTest {
  val topicIndex = new AtomicInteger(0)
  def generateInputTopicName = s"input-${topicIndex.incrementAndGet()}"
  def generateOutputTopicName = s"output-${topicIndex.incrementAndGet()}"
}
