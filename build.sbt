name := "flink-kafka-itest"

version := "1.0"

scalaVersion := "2.11.8"

val flinkVersion = "1.3.1"
val kafkaMajorVersion = "0.10"
val kafkaVersion = "0.10.0.1"
val grizzledVersion = "1.2.0"
val scalatestVersion = "3.0.3"
val logbackVersion = "1.2.3"

libraryDependencies ++= Seq(
  "org.apache.flink" %% "flink-streaming-scala" % flinkVersion,
  "org.apache.flink" %% s"flink-connector-kafka-$kafkaMajorVersion" % flinkVersion,
  "org.clapper" %% "grizzled-slf4j" % grizzledVersion,
  "ch.qos.logback" % "logback-classic" % logbackVersion,
  "org.apache.flink" %% "flink-test-utils" % flinkVersion % "test",
  "org.apache.kafka" %% "kafka" % kafkaVersion % "test",
  "com.101tec" % "zkclient" % "0.7" % "test",
  "org.scalatest" %% "scalatest" % scalatestVersion % "test"
)
    