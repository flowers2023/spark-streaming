package org.github.drunk2013.spark.streaming
import java.util.HashMap

import org.apache.kafka.clients.producer.{ProducerConfig, KafkaProducer, ProducerRecord}

import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka._
import org.apache.spark.SparkConf

object KafkaDemo{

  def main(args: Array[String]) {
    println("start sparkstreaming............")

    //val Array(zkQuorum, group, topics, numThreads) = args
    val sparkConf = new SparkConf().setAppName("KafkaWordCount")
    val ssc = new StreamingContext(sparkConf, Seconds(2))
    ssc.checkpoint("checkpoint")

    var tops : String="test"
    //val topicMap = topics.split(",").map((_, numThreads.toInt)).toMap
    val topicMap = tops.split(",").map((_, 1)).toMap
    val lines = KafkaUtils.createStream(ssc, "localhost", "spark", topicMap).map(_._2)
    val words = lines.flatMap(_.split(" "))
    val wordCounts = words.map(x => (x, 1L)).reduceByKeyAndWindow(_ + _, _ - _, Minutes(10), Seconds(2), 2)
    wordCounts.print()

    ssc.start()
    ssc.awaitTermination()
  }

}
