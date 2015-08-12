package org.github.drunk2013.spark.streaming

import java.util.concurrent.TimeUnit

import com.typesafe.config.ConfigFactory
import org.apache.commons.pool2.impl.GenericObjectPoolConfig
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.streaming._
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.flume.{SparkFlumeEvent, FlumeUtils}
import org.apache.spark.SparkContext._
//import redis.clients.jedis.{JedisPool, Jedis}
import org.apache.spark.util.IntParam
import org.apache.spark.SparkConf

import java.sql.DriverManager
import org.apache.spark.SparkContext
import org.apache.spark.rdd.JdbcRDD
import java.sql.{DriverManager, PreparedStatement, Connection}
import java.util._

/**
 *
 * 实时统计每个nginx数据的UV,PV,流量
 *
 */

object FlumeNginxAnalyze {
  
  var mysql_url: String = ""
  var mysql_username: String = ""
  var mysql_password: String = "" 
  var mysql_hostname: String = ""
  var mysql_db: String = ""
  var mysql_port: String = ""
  var client_type:Int = 0
  //192.168.45.212,2015-06-04 13:33,28,adfasdfasdfasdfas
  case class AccessInfo(ip: String, time: String, http_method: String ,access_url: String ,http_version: String,http_status: String , traffic: Long ,referrers: String,agents: String, cookie: String)
  
  //
  case class result(count: Int, pv: Int, uv:Int, traffic: Long)

  def etl(flumeEvent: SparkFlumeEvent): Boolean = {
    val raw = new String(flumeEvent.event.getBody.array())
    val regex="""(.*) - - \[(.*)\] "([A-Z]+) (.*) (.*) ([\d]+) ([\d]+) (.*) "(.*)" "(.*)""".r
    var flag = false
    try {
       val regex(ips,tm,http_method,access_url,http_version,httd_status,traffic,referrers,agents,cookie) = raw
       flag = true
    } catch {
       case e: Exception => false
    }
    if(!flag){
      println(raw)
    }
    flag
   
  }

  def parseRawAccessInfo(flumeEvent: SparkFlumeEvent): AccessInfo = {
    val raw = new String(flumeEvent.event.getBody.array())
    
    val regex="""(.*) - - \[(.*)\] "([A-Z]+) (.*) (.*) ([\d]+) ([\d]+) (.*) "(.*)" "(.*)""".r
    val regex(ips,tm,http_method,access_url,http_version,http_status,traffic,referrers,agents,cookie) = raw
    AccessInfo(ips, tm, http_method, access_url, http_version, http_status ,traffic.toLong,referrers,agents,cookie)
  }


  def trafficCount(source: DStream[AccessInfo]): DStream[(String, Long)] = {
    source.map{accessInfo =>
      val traffic = accessInfo.traffic
      val ip = accessInfo.ip
      ("1",traffic)
    }.reduceByKey(_ + _)
    
    //println("end:===============")
  }

  def pvCount(source: DStream[AccessInfo]): DStream[(String, Long)] = {
    source.map{accessInfo =>
    ("1",1.toLong)
    }.reduceByKey(_ + _)
  }


  def uvList(source: DStream[AccessInfo]): DStream[(String, Long)] = {
    source.map{accessInfo =>
    (accessInfo.cookie,1.toLong)
    }.reduceByKey(_ + _)
  }

  def uvCount(source: DStream[(String, Long)]): DStream[(String, Long)] = {
    source.map{accessInfo =>
    ("1",1.toLong)
    }.reduceByKey(_ + _)
  }
  
  //发送到mysql中
  def sinkToMysql(traffic: DStream[(String, Long)],pv: DStream[(String, Long)],uv: DStream[(String, Long)],uv_list: DStream[(String, Long)]): Unit = {
    uv_list.foreachRDD(rdd => {
      rdd.foreachPartition(partitionOfRecords => {
        partitionOfRecords.foreach { case (cookie: String, accessCount: Long) =>
          //println(cookie)
          //println(accessCount)
          var conn: Connection = null
          val sql = "insert into uvList(cookie,count,client_type) values (?,?,?)"
          //conn = DriverManager.getConnection("jdbc:mysql://"+mysql_hostname+":"+mysql_port+"/"+mysql_db,mysql_username, mysql_password)
          conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/test",mysql_username, mysql_password)

          var ps: PreparedStatement = null
          ps = conn.prepareStatement(sql)
          try {
            ps.setString(1,cookie)
            ps.setLong(2,accessCount)
            ps.setInt(3,client_type)
            ps.executeUpdate()
          } catch {
            case e: Exception => println("Mysql Exception"+e)
          } finally {
            if (ps != null) {
              ps.close()
            }
            if (conn != null) {
              conn.close()
            }
          }
        }
      })
    })
  
    //流量
    traffic.cogroup(pv).cogroup(uv).foreachRDD(rdd =>{
      rdd.foreachPartition(partitionOfRecords => {
        partitionOfRecords.foreach { case (key: String, value2: ((Iterable[Long],Iterable[Long]),Iterable[Long])) =>
           val (traffic,pv) = getNu(value2._1.toString())
           val uv = value2._2.toList(0)
           //写入到mysql
           var conn: Connection = null
           val sql = "insert into pvflow(pv,uv,traffic,client_type) values (?,?,?,?)"
           //conn = DriverManager.getConnection("jdbc:mysql://"+mysql_hostname+":"+mysql_port+"/"+mysql_db,mysql_username, mysql_password)
           conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/test",mysql_username, mysql_password)

           var ps: PreparedStatement = null
           ps = conn.prepareStatement(sql)
           try {
             ps.setLong(1, pv.toLong)
             ps.setLong(2, uv.toLong)
             ps.setLong(3, traffic.toLong)
             ps.setInt(4,client_type)
             ps.executeUpdate()
           } catch {
            case e: Exception => println("Mysql Exception"+e)
           } finally {
             if (ps != null) {
               ps.close()
             }
             if (conn != null) {
               conn.close()
             }
          }
        }
      })
    })
  
  }

  def getNu(str: String): (String, String) = {
    val regex="""CompactBuffer\(\(CompactBuffer\(([0-9]+)\),CompactBuffer\(([0-9]+)\)\)\)""".r
    val regex(traffic,pv) = str
    (traffic,pv)
  }
    
  def main(args: Array[String]) {
    //val config = ConfigFactory.load()
    //val task_name = config.getString("task.name")
    //mysql_hostname = config.getString("mysql.hostname")
    //mysql_port = config.getString("mysql.port")
    //mysql_db = config.getString("mysql.db")
    //mysql_username = config.getString("mysql.username")
    //mysql_password = config.getString("mysql.password")
    //client_type = config.getInt("client.type")

    

    //val ssc = new StreamingContext(master, task_name, Seconds(batch_interval))
    //ssc.checkpoint("hdfs://localhost:8020/checkpoint")
    
    println("start sparkstreaming............")

    val Array(host, port,batch_interval,mysql_hostname,mysql_port,mysql_db,mysql_username,mysql_password,client_type,task_name) = args
    println(host)
    val batchInterval = Milliseconds(batch_interval.toInt)
    val sparkConf = new SparkConf().setAppName(task_name)
    val ssc = new StreamingContext(sparkConf, batchInterval)
    //val source = FlumeUtils.createStream(ssc, ip, port, StorageLevel.MEMORY_ONLY)
    val source = FlumeUtils.createStream(ssc,host, port.toInt, StorageLevel.MEMORY_ONLY_SER_2)

    //使用线程池复用链接
    //val pool = {
    //  val pool = createRedisPool(redisIp, redisPort, redisPwd)
    //  ssc.sparkContext.broadcast(pool)
    //}

    //预处理
    val cleanSource = source.filter(etl).map(parseRawAccessInfo).cache()

    //cleanSource.print()
    /**
     * TODO
     * 如何清除过期字典
     */
    //val dict = cleanSource.map { userDownload =>
    //  (combine(userDownload.imei, userDownload.appName), userDownload.timestamp)
    //}
    //dict.print()
    //实时字典：用户最近一次下载记录作为字典
    //val currentDict = dict.updateStateByKey(updateDict)

    //统计用户下载记录
    //val downloadCount = AppDownloadCount(cleanSource, currentDict)
    val traffic = trafficCount(cleanSource)
    val pv = pvCount(cleanSource)
    val uv_list = uvList(cleanSource)
    val uv = uvCount(uv_list)

    //打印
    //traffic.print()
    //pv.print()
    //uv.print()

    println("\n\n\n==========================sparkstreaming is running............")
    //输出到Mysql
    sinkToMysql(traffic,pv,uv,uv_list)

    ssc.start()
    ssc.awaitTermination()
  }

}
