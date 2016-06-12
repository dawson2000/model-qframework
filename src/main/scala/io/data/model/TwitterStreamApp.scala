package io.data.model

import org.apache.spark.streaming.twitter.TwitterUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}


/**
  * Created by dawson on 11/06/16.
  */
object TwitterStreamApp {

  def main(args: Array[String]) {
    val config = new SparkConf()
    config.setAppName("Twitter Sentiments").setMaster("local[5]")


    val ssc = new StreamingContext(config, Seconds(10))
    ssc.sparkContext.setLogLevel("ERROR")

    val tw = TwitterUtils.createStream(ssc, None, Seq("@roger,@bell,@telus,@apple"))



    tw.filter(s => s.getLang.equals("en")
    ).foreachRDD(rdd => {
      rdd.foreach(line => {
        println(line.getText)

      })
    })
    ssc.start()
    ssc.awaitTermination()


  }

}
