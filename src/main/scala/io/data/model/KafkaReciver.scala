package io.data.model

import java.util
import java.util.Properties

import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer}

/**
  * Created by dawson on 09/06/16.
  */
object KafkaReciver {

  def main(args: Array[String]) {
    val props: Properties = new Properties
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "104.196.128.233:9092")
    props.put(ConsumerConfig.GROUP_ID_CONFIG, "DemoConsumer2")
    props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true")
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest")
    props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000")
    props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "30000")
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer")
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer")

    val  consumer = new KafkaConsumer[String,String](props)
    val  topics =new  util.ArrayList[String]()
    topics.add("test2")

    consumer.subscribe(topics)
   // consumer.seek(consumer.assignment().iterator().next(),0)
    val  messagers = consumer.poll(1000)

    println(messagers.count())

    val  ime = messagers.iterator()

    while (ime.hasNext ){
        var o = ime.next()
        println( o.key()  + ":::" + o.value())
    }


  }

}
