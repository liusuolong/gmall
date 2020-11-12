package com.app

import com.alibaba.fastjson.JSON
import com.bean.OrderInfo
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
 * @create 2020-11-12 16:53
 */
object GmvApp {
  def main(args: Array[String]): Unit = {
    //1.创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //2.创建sparkStreaming上下文环境对象
    val ssc = new StreamingContext(conf,Seconds(3))

    //3.获取kafka连接
    val kafkaDStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(GmallConstants.GMALL_ORDER_INFO,ssc)
    //4.加时间戳 转换为样例类
    val orderInfoDStream: DStream[OrderInfo] = kafkaDStream.map(
      record => {
        val valueStr: String = record.value()
        //解析样例类
        val orderInfo: OrderInfo = JSON.parseObject(valueStr, classOf[OrderInfo])
        //取出时间
        val create_time: String = orderInfo.create_time

        val dataAndTime: Array[String] = create_time.split(" ")

        orderInfo.create_date = dataAndTime(0)
        orderInfo.create_hour = dataAndTime(1)

        //脱敏
        val tuple: (String, String) = orderInfo.consignee_tel.splitAt(4)
        orderInfo.consignee_tel = tuple._1 + "*****"
        //返回数据
        orderInfo
      }
    )
    //将数据写入phoenix
    orderInfoDStream.foreachRDD(
      rdd=>{
        println("1212")
        rdd.saveToPhoenix(
          "GMALL2020_ORDER_INFO",
          //方式一：
          //Seq(表字段),
          //方式二：
          classOf[OrderInfo].getDeclaredFields.map(_.getName.toUpperCase()),
          HBaseConfiguration.create(),
          Some("hadoop002,hadoop003,hadoop004:2181")
        )
      })

    ssc.start()

    ssc.awaitTermination()
  }

}
