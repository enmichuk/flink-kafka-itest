package main

import org.apache.flink.api.common.functions.MapFunction

class TransformMapFunction extends MapFunction[String, String] {
  override def map(value: String): String = s"$value-transformed"
}
