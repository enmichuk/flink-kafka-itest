package main

import java.util.UUID

import org.apache.flink.configuration.ConfigConstants
import org.scalatest.BeforeAndAfterEach

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

class KafkaITTest extends FlinkKafkaTestBase with BeforeAndAfterEach {

  import KafkaITTest._

  override def beforeAll(): Unit = {
    super.beforeAll()
    createTestTopic(InputTopic, 1, 1)
  }

  "Kafka" should "receive and provide records" in {
    val originalMessage = UUID.randomUUID().toString.getBytes(ConfigConstants.DEFAULT_CHARSET)

    writeToTopic(InputTopic, originalMessage)

    eventually {
      readFromTopic(TestGroupId, InputTopic) should contain only originalMessage
    }
  }

  it should "receive and provide 100 records" in {
    val originalMessage = UUID.randomUUID().toString.getBytes(ConfigConstants.DEFAULT_CHARSET)

    val size = 100
    for(_ <- 0 until size) yield {
      writeToTopic(InputTopic, originalMessage)
    }
    info(s"$size messages were sent")

    eventually {
      readFromTopic(TestGroupId, InputTopic).length shouldBe size
    }
  }

  it should "receive and provide 100 records sent in asynchronous manner" in {
    val originalMessage = UUID.randomUUID().toString.getBytes(ConfigConstants.DEFAULT_CHARSET)

    val size = 100
    Future {
      Thread.sleep(2000)
      for (_ <- 0 until size) yield {
        writeToTopic(InputTopic, originalMessage)
      }
      info(s"$size messages were sent")
    }

    eventually {
      readFromTopic(TestGroupId, InputTopic).length shouldBe size
    }
  }

  it should "receive and provide 30000 records" in {
    val originalMessage = UUID.randomUUID().toString.getBytes(ConfigConstants.DEFAULT_CHARSET)

    val size = 30000
    for(_ <- 0 until size) yield {
      writeToTopic(InputTopic, originalMessage)
    }
    info(s"$size messages were sent")

    eventually {
      readFromTopic(TestGroupId, InputTopic).length shouldBe size
    }
  }

}

object KafkaITTest {
  val InputTopic = "input"
  val TestGroupId = "test"
}