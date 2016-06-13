package io.data.model

import java.util.Properties

import io.data.mq.QKafkaMessager
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}

/**
  * Created by dawson on 09/06/16.
  */
object KafkaMessager {

  def main(args: Array[String]) {
    val props: Properties = new Properties
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "104.196.128.233:9092")
    props.put(ProducerConfig.CLIENT_ID_CONFIG,"KafkaMessager")
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringSerializer")
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringSerializer")


    val  qProducer = new QKafkaMessager("test2" , "104.196.128.233" , true)
    qProducer.run

  }

}
