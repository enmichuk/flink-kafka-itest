package main

import com.typesafe.config.ConfigFactory
import org.apache.flink.configuration.ConfigConstants
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.scalatest.BeforeAndAfterEach

import scala.collection.JavaConverters._

class FlinkKafkaJobITTest extends FlinkKafkaTestBase with BeforeAndAfterEach {

  import FlinkKafkaJob.Config._
  import FlinkKafkaJobITTest._

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
      readFromTopic("test", InputTopic) should not be empty
    }
//    val env = StreamExecutionEnvironment.getExecutionEnvironment
//    new FlinkKafkaJob().run(config, env)
  }

  private lazy val config = ConfigFactory.parseMap(Map(
    KafkaBrokersParam -> kafkaServer.getBrokerConnectionString,
    KafkaZkConnectParam -> kafkaServer.getZookeeperConnectionString,
    InputTopicParam -> InputTopic,
    OutputTopicParam -> OutputTopic
  ).asJava).withFallback(ConfigFactory.load())

}

object FlinkKafkaJobITTest {
  val InputTopic = "input"
  val OutputTopic = "output"
}
