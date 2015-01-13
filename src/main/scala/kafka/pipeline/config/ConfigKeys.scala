package kafka.pipeline.config

/**
 * TODO: To be implemented
 */
object ConfigKeys {
  object consumer {
    val number = "kafkaPipeline.consumer.number"
    object kafka {
      val brokerList = "kafkaPipeline.consumer.kafka.brokerList"
      val topicId = "kafkaPipeline.consumer.kafka."
    }
  }
  object handler {

  }
  object sender {

  }
}
