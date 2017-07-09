package main

import com.typesafe.config.ConfigFactory
import org.apache.flink.configuration.ConfigConstants
import org.scalatest.BeforeAndAfterEach

import scala.collection.JavaConverters._
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
    cancelCurrentJob(JobName)

    ensureTopicIsEmptyForGroup(KafkaGroupId, OutputTopic)
    ensureTopicIsEmptyForGroup(KafkaGroupId, InputTopic)
    ensureTopicIsEmptyForGroup(TestGroupId, OutputTopic)
    ensureTopicIsEmptyForGroup(TestGroupId, InputTopic)
    info("Environment is clean and ready for next test")
  }

  "FlinkKafkaJob" should "work" in {
    val size = 30000
    for(i <- 0 until size) yield {
      if(i % 1000 == 0 && i >0) info(s"$i messages has been sent")
      writeToTopic(InputTopic, "message".getBytes(ConfigConstants.DEFAULT_CHARSET))
    }
    info("All messages were sent")

    eventually {
      readFromTopic(TestGroupId, InputTopic).size shouldBe size
    }

    runFlinkKafkaJob()

    eventually {
      readFromTopic(KafkaGroupId, OutputTopic) should not be empty
    }

    writeToTopic(InputTopic, "message".getBytes(ConfigConstants.DEFAULT_CHARSET))

    writeToTopic(InputTopic, "message".getBytes(ConfigConstants.DEFAULT_CHARSET))
  }

  it should "also work" in {
    1 shouldBe 1
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
  val TestGroupId = "test"
}
