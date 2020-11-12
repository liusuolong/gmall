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
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.phoenix.spark._

/**
 * @author ymstart
 * @create 2020-11-11 20:30
 */
object DauAPP2 {
  def main(args: Array[String]): Unit = {
    //1.创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //2.创建sparkStreaming上下文环境对象
    val ssc = new StreamingContext(conf,Seconds(3))

    //3消费kafka数据
    val kafkaDStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(GmallConstants.KAFKA_TOPIC_START,ssc)

    val sdf = new SimpleDateFormat("yyyy-MM-dd HH")
    //4启动日志
    val startUpLogDStream: DStream[StartUpLog] = kafkaDStream.map(
      record => {
        val valueStr: String = record.value()
        //解析样例类
        val startUpLog: StartUpLog = JSON.parseObject(valueStr, classOf[StartUpLog])
        //取出ts字段
        val ts: Long = startUpLog.ts
        //需要将ts==>字符串
        val dateArr: Array[String] = sdf.format(new Date(ts)).split(" ")
        //添加字段
        startUpLog.logDate = dateArr(0)
        startUpLog.logHour = dateArr(1)
        //返回数据
        startUpLog
      }
    )
    //5.跨批次去重(redis)
    val groupByRedis: DStream[StartUpLog] = DauHandler.fielderByRedis(startUpLogDStream)

    //6.同批次去重
    val groupByMid: DStream[StartUpLog] = DauHandler.fielderByMid(groupByRedis)

    //7.将数据存入phoenix中
    groupByMid.foreachRDD(
      rdd => {
        rdd.saveToPhoenix(
          "GMALL2020_DAU",
          Seq("MID", "UID", "APPID", "AREA", "OS", "CH", "TYPE", "VS", "LOGDATE", "LOGHOUR", "TS"),
          HBaseConfiguration.create(),
          Some("hadoop002,hadoop003,hadoop004")
        )
      })
    ssc.start()

    ssc.awaitTermination()
  }
}
