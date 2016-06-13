package io.data.mq

import java.util.{Properties, UUID}

import org.apache.kafka.clients.producer.{Callback, KafkaProducer, ProducerRecord, RecordMetadata}

/**
  * Created by dawson on 11/06/16.
  */
class QKafkaMessager extends Runnable{

  private var producer: KafkaProducer[ String , String ] = null
  private var topic: String = null
  private var isAsync: Boolean = false

  def this(topic: String,   brokerHostName:String   , isAsync: Boolean){
    this
    val props: Properties = new Properties
    props.put("bootstrap.servers", brokerHostName + ":9092")
    props.put("client.id", "DemoProducer")
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("acks", "1")
    producer = new KafkaProducer[String , String](props)
    this.topic = topic
    this.isAsync = isAsync
  }

  override def run(): Unit = {
    val  messageNo = 0
    while (true){
      val  messageId = UUID.randomUUID.toString
      val  startTime = System.currentTimeMillis
      val  messageStr = "Message ::" +  messageNo
      val  record = new  ProducerRecord(topic,messageId , messageStr)
      if(isAsync){

        producer.send(record ,  new   DemoCallBack( startTime , messageId ,  messageStr) )

      }else{
        producer.send(record).get
      }

      println("start  next ::")
      Thread.sleep(6000)
      println("start  next   message  now ::")

    }

  }

  class DemoCallBack(val startTime: Long, val key: String, val message: String) extends Callback {
    def onCompletion(metadata: RecordMetadata, exception: Exception) {
      val elapsedTime: Long = System.currentTimeMillis - startTime
      if (metadata != null) {
        System.out.println("message(" + key + ", " + message + ") sent to partition(" + metadata.partition + "), " + "offset(" + metadata.offset + ") in " + elapsedTime + " ms")
      }
      else {
        exception.printStackTrace
      }
    }
  }

}
