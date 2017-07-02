package main

import java.io.File
import java.util.Properties

import com.typesafe.config.{Config, ConfigFactory}
import grizzled.slf4j.Logging
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer010, FlinkKafkaProducer010}
import org.apache.flink.streaming.util.serialization.SimpleStringSchema
import org.apache.kafka.clients.consumer.ConsumerConfig._

class FlinkKafkaJob {
  import FlinkKafkaJob.Config._
  import FlinkKafkaJob._

  def run(conf: Config, env: StreamExecutionEnvironment): Unit = {
    val inputTopic = conf.getString(InputTopicParam)
    val outputTopic = conf.getString(OutputTopicParam)
    val kafkaBrokers =  conf.getString(KafkaBrokersParam)
    val zkConnect = conf.getString(KafkaZkConnectParam)
    val parallelism = conf.getInt(ParallelismParam)

    val props = new Properties()
    props.setProperty(BOOTSTRAP_SERVERS_CONFIG, kafkaBrokers)
    props.setProperty(GROUP_ID_CONFIG, KafkaGroupId)
    props.setProperty("zookeeper.connect", zkConnect)

    val producerProps = new Properties()
    producerProps.setProperty(BOOTSTRAP_SERVERS_CONFIG, kafkaBrokers)

    val inputStream = env
      .addSource(new FlinkKafkaConsumer010(inputTopic, new SimpleStringSchema, props))
      .name("InputStream")
      .setParallelism(parallelism)

    val transformedStream = inputStream
      .map(new TransformMapFunction())
      .name("TransformedStream")
      .setParallelism(parallelism)

    transformedStream
      .addSink(new FlinkKafkaProducer010(outputTopic, new SimpleStringSchema, producerProps))
      .name("OutputStream")
      .setParallelism(parallelism)

    env.execute(JobName)
  }

}

object FlinkKafkaJob extends Logging {
  val JobName = "flink-kafka-itest"
  val KafkaGroupId = JobName

  object Config {
    val InputTopicParam = "kafka.topics.input"
    val OutputTopicParam = "kafka.topics.output"
    val KafkaBrokersParam = "kafka.kafkaBrokers"
    val KafkaZkConnectParam = "kafka.zkConnect"
    val ParallelismParam = "kafka.parallelism"
  }

  def main(args: Array[String]) {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val params = ParameterTool.fromArgs(args)

    val config = ConfigFactory.parseFile(new File(params.getRequired("config")))
      .withFallback(ConfigFactory.load())

    new FlinkKafkaJob().run(config, env)
  }
}
