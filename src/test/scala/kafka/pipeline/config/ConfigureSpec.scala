package kafka.pipeline.config

import org.scalatest._


class ConfigureSpec extends FlatSpec with Matchers {
  "A user-defined configure" should "overwrite default configure" in {
    KafkaPipelineConfigure.setUp()
    KafkaPipelineConfigure.configure.consumer.number shouldBe 3
    KafkaPipelineConfigure.configure.handler.number shouldBe 4
  }

}
