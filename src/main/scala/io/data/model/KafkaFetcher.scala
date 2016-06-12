package io.data.model

import io.data.framework.KafkaProperties
import kafka.api.{FetchRequest, FetchRequestBuilder}
import kafka.javaapi.consumer.SimpleConsumer

/**
  * Created by dawson on 11/06/16.
  */
object KafkaFetcher {

  def main(args: Array[String]) {
    val simpleConsumer: SimpleConsumer = new SimpleConsumer(KafkaProperties.KAFKA_SERVER_URL, KafkaProperties.KAFKA_SERVER_PORT, KafkaProperties.CONNECTION_TIMEOUT, KafkaProperties.KAFKA_PRODUCER_BUFFER_SIZE, KafkaProperties.CLIENT_ID)

    System.out.println("Testing single fetch")
    var req: FetchRequest = new FetchRequestBuilder().
      clientId(KafkaProperties.CLIENT_ID).addFetch("test", 0, 0L, 100).build


  }

}
