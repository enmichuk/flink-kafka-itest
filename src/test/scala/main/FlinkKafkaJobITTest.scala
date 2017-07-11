package main

import java.util.UUID

import com.typesafe.config.ConfigFactory
import org.apache.flink.configuration.ConfigConstants
import org.apache.kafka.clients.producer.RecordMetadata
import org.scalatest.BeforeAndAfterEach

import org.scalatest.Inspectors._

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

class FlinkKafkaJobITTest extends FlinkKafkaTestBase with BeforeAndAfterEach {

  import FlinkKafkaJob.Config._
  import FlinkKafkaJob._
  import FlinkKafkaJobITTest._

  override def beforeAll(): Unit = {
    super.beforeAll()
    createTestTopic(InputTopic, 1, 1)
    createTestTopic(OutputTopic, 1, 1)
  }

  override def afterEach(): Unit = {
    readFromOutputTopic()

    cancelCurrentJob(JobName)

    eventually {
      readFromOutputTopic() shouldBe empty
    }
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
    val buffer = ArrayBuffer[String]()
    Future {
      Thread.sleep(2000)
      for (_ <- 0 until size) yield {
        val msg = UUID.randomUUID().toString
        buffer += msg
        writeToInputTopic(msg)
      }
    }

    runFlinkKafkaJob()

    val bufferTransformed = buffer.map(new TransformMapFunction().map(_))

    eventually {
      forAll(readFromOutputTopic()) { msg =>
        bufferTransformed should contain (msg + "1")
      }
    }
  }

  def readFromOutputTopic(): Seq[String] = {
    readFromTopic(KafkaGroupId, OutputTopic).map(new String(_, ConfigConstants.DEFAULT_CHARSET))
  }

  def writeToInputTopic(msg: String): RecordMetadata = {
    writeToTopic(InputTopic, msg.getBytes(ConfigConstants.DEFAULT_CHARSET))
  }

  private lazy val config = ConfigFactory.parseMap(Map(
    KafkaBrokersParam -> brokerConnectionString,
    InputTopicParam -> InputTopic,
    OutputTopicParam -> OutputTopic
  ).asJava).withFallback(ConfigFactory.load())

  private def runFlinkKafkaJob(): Future[Unit] = {
    withFlinkEnv { env => new FlinkKafkaJob().run(config, env) }
  }

}

object FlinkKafkaJobITTest {
  val InputTopic = "input"
  val OutputTopic = "output"
}
