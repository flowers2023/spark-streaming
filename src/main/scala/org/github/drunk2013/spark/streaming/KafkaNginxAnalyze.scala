package org.github.drunk2013.spark.streaming
import java.util.HashMap

import org.apache.kafka.clients.producer.{ProducerConfig, KafkaProducer, ProducerRecord}
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.Time
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka._
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.streaming.{Time, Seconds, StreamingContext}
import org.apache.spark.util.IntParam
import org.apache.spark.sql.SQLContext
import org.apache.spark.storage.StorageLevel

object KafkaNginxAnalyze{

  def main(args: Array[String]) {
    println("start sparkstreaming............")

    //val Array(zkQuorum, group, topics, numThreads) = args
    val sparkConf = new SparkConf().setAppName("KafkaWordCount")
    val ssc = new StreamingContext(sparkConf, Seconds(4))
    //ssc.checkpoint("checkpoint")

    var tops : String="test"
    //val topicMap = topics.split(",").map((_, numThreads.toInt)).toMap
    val topicMap = tops.split(",").map((_, 1)).toMap
    val lines = KafkaUtils.createStream(ssc, "localhost", "spark", topicMap).map(_._2)

    //每条数据的分隔符
    val words = lines.flatMap(_.split("\n"))
    //words.print()
    words.foreachRDD{rdd: RDD[String] => 
      val sqlContext = SQLContextSingleton.getInstance(rdd.sparkContext)
      import sqlContext.implicits._
      // 数据格式化成表
      val studentDataFrame = rdd.map(_.split(",")).map(row => Student(row(0).toString,row(1).toString,row(2).toInt)).toDF()
      studentDataFrame.registerTempTable("student")
      // SQL统计数据
      val result = sqlContext.sql("select id, count(*) as count from student group by id")

      result.show()

    }
    //lines.print()

    ssc.start()
    ssc.awaitTermination()
  }

}

/** Case class for converting RDD to DataFrame */
case class Student(id: String,name: String,age: Int)


/** Lazily instantiated singleton instance of SQLContext */
object SQLContextSingleton {

  @transient  private var instance: SQLContext = _

    def getInstance(sparkContext: SparkContext): SQLContext = {
      if (instance == null) {
	instance = new SQLContext(sparkContext)
      }
      instance
    }
}
