package main

import grizzled.slf4j.Logging
import org.apache.flink.api.common.functions.MapFunction

class TransformMapFunction extends MapFunction[String, String] with Logging {
  override def map(value: String): String = {
    info(s"Message [$value] received in TransformMapFunction")
    s"$value-transformed"
  }
}
