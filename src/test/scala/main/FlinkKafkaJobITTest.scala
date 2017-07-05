package main

import com.typesafe.config.ConfigFactory
import org.apache.flink.configuration.ConfigConstants
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.scalatest.BeforeAndAfterEach

import scala.collection.JavaConverters._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.util.{Failure, Success}

class FlinkKafkaJobITTest extends FlinkKafkaTestBase with BeforeAndAfterEach {

  import FlinkKafkaJob.Config._
  import FlinkKafkaJobITTest._
  import FlinkKafkaJob._

  override def beforeEach(): Unit = {
    createTestTopic(InputTopic, 1, 1)
    createTestTopic(OutputTopic, 1, 1)
  }

  override def afterEach(): Unit = {
    deleteTestTopic(InputTopic)
    deleteTestTopic(OutputTopic)
  }

  "FlinkKafkaJob" should "work" in {
    writeToTopic(InputTopic, "message".getBytes(ConfigConstants.DEFAULT_CHARSET))
    eventually {
      readFromTopic(TestGroupId, InputTopic) should not be empty
    }
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val f = Future(new FlinkKafkaJob().run(config, env))
    f.onComplete{
      case Success(res) => info(s"Success $res")
      case Failure(e) => error("Error while executing job", e)
    }
    eventually {
      readFromTopic(KafkaGroupId, OutputTopic) should not be empty
    }
  }

  private lazy val config = ConfigFactory.parseMap(Map(
    KafkaBrokersParam -> brokerConnectionString,
    KafkaZkConnectParam -> zookeeperConnectionString,
    InputTopicParam -> InputTopic,
    OutputTopicParam -> OutputTopic
  ).asJava).withFallback(ConfigFactory.load())

}

object FlinkKafkaJobITTest {
  val InputTopic = "input"
  val OutputTopic = "output"
  val TestGroupId = "test"
}
