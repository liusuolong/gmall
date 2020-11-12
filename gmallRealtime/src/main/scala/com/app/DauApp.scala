package com.app

import java.text.SimpleDateFormat
import java.util.Date

import com.alibaba.fastjson.JSON
import com.bean.StartUpLog
import com.handler.DauHandler
import com.utils.MyKafkaUtil
import com.ym123.GmallConstants
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.phoenix.spark._
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * @author ymstart
 * @create 2020-11-05 12:52
 */
object DauApp {
  def main(args: Array[String]): Unit = {
    //1.创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //2.创建sparkStreaming上下文环境对象
    val ssc = new StreamingContext(conf, Seconds(3))

    //3.消费kafka中的启动数据
    val kafkaDStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(GmallConstants.KAFKA_TOPIC_START, ssc)

    //4将数据转换为样例类，并添加时间字段 Driver端
    val sdf = new SimpleDateFormat("yyyy-MM-dd HH") //可序列化的

    val startUpLogDStream: DStream[StartUpLog] = kafkaDStream.map(
      record => {
        //a.ConsumerRecord[String, String] =>取出value
        val value: String = record.value()
        //b.取出时间戳字段
        //b.1 解析对象
        val startUpLog: StartUpLog = JSON.parseObject(value, classOf[StartUpLog])
        //b.2 取出ts
        val ts: Long = startUpLog.ts
        //c.将时间戳 => 字符串
        val dateStr: String = sdf.format(new Date(ts))
        //d.给时间字段重新赋值
        val dateArr: Array[String] = dateStr.split(" ")
        //yyyy-MM-dd
        startUpLog.logDate = dateArr(0)
        //HH
        startUpLog.logHour = dateArr(1)
        //e.返回数据
        startUpLog
      }
    )

    //5.redis 跨批次去重
    val fielderByRedis: DStream[StartUpLog] = DauHandler.fielderByRedis(startUpLogDStream)

    /*    startUpLogDStream.cache()
        startUpLogDStream.count().print()
        fieldedByRedis.cache()
        fieldedByRedis.count().print()*/

    //6.根据mid 同批次去重
    val filteredByMidDStream: DStream[StartUpLog] = DauHandler.fielderByMid(fielderByRedis)

    //7.将去重的mid保存到redis
    DauHandler.saveMidToRedis(filteredByMidDStream)

    //8.将去重后的数据存入phoenix
    filteredByMidDStream.foreachRDD(
      //GMALL2020_DAU 表名 Seq()表字段 Some()连接地址
      rdd => {
        rdd.saveToPhoenix("GMALL2020_DAU",
          Seq("MID", "UID", "APPID", "AREA", "OS", "CH", "TYPE", "VS", "LOGDATE", "LOGHOUR", "TS"),
          HBaseConfiguration.create(),
          Some("hadoop002,hadoop003,hadoop004:2181"))
      }
    )

    /*    //测试数据流是否打通
        kafkaDStream.foreachRDD(
          rdd=>{
            rdd.foreach(
              record=>{
                val str: String = record.value()
                println(str)
              }
            )
          }
        )*/

    ssc.start()

    ssc.awaitTermination()
  }
}
