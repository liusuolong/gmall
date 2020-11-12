package com.app

import java.text.SimpleDateFormat
import java.util
import java.util.Date

import com.alibaba.fastjson.JSON
import com.bean.{CouponAlertInfo, EventLog}
import com.utils.{MyEsUtil, MyKafkaUtil}
import com.ym123.GmallConstants
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.{SparkConf, streaming}
import org.apache.spark.streaming.{Minutes, Seconds, StreamingContext}

import scala.util.control.Breaks._


/**
 * @author ymstart
 * @create 2020-11-10 9:11
 */
object AlertApp {
  def main(args: Array[String]): Unit = {
    //1.创建SparkConf并设置App名称
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("AlertApp")
    ////2.创建sparkStreaming上下文环境对象
    val ssc = new StreamingContext(sparkConf, Seconds(8))
    //3.消费kafka中的启动数据
    val kafkaDStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(GmallConstants.KAFKA_TOPIC_EVENT, ssc)
    //4.将数据放入样例类中,并补充时间字段  转换结构(eventLog.mid, eventLog)
    val sdf = new SimpleDateFormat("yyyy-MM-dd HH")
    val midToLogDStream: DStream[(String, EventLog)] = kafkaDStream.map(
      record => {
        //a.转换为样例类
        val eventLog: EventLog = JSON.parseObject(record.value(), classOf[EventLog])
        //b.处理时间
        val dataHourStr: String = sdf.format(new Date(eventLog.ts))
        val date: Array[String] = dataHourStr.split(" ")
        eventLog.logDate = date(0)
        eventLog.logHour = date(1)

        //c.返回数据
        (eventLog.mid, eventLog)
      })
    //midToLogDStream.print()

    //5.开窗2分钟
    val midToLogByWindowDStream: DStream[(String, EventLog)] = midToLogDStream.window(Minutes(2))
    //6.分组
    val minToIterDStream: DStream[(String, Iterable[EventLog])] = midToLogByWindowDStream.groupByKey()
    //7.组内筛选
    val AlertDStream: DStream[(Boolean, CouponAlertInfo)] = minToIterDStream.map {
      case (mid, iter) =>

        //创建Set集合存放领劵uid
        val uids: util.HashSet[String] = new util.HashSet[String]()
        //存放优惠劵及商品信息
        val iterIds: util.HashSet[String] = new util.HashSet[String]()
        //存放所有行为
        val events: util.ArrayList[String] = new util.ArrayList[String]()

        //设定除去浏览行为标志位
        var noClick: Boolean = true
        //遍历iter
        breakable {
          iter.foreach(
            log => {
              val evid: String = log.evid
              events.add(evid)
              //判断行为
              //浏览行为
              if ("clickIter".equals(evid)) {
                //有浏览行为不要
                noClick = false
                break()
              } else if ("coupon".equals(evid)) {
                //领卷行为
                //领卷的用户
                uids.add(log.uid)
                iterIds.add(log.itemid)
              }
            })
        }
        //预警日志
        /*        if (uids.size() >= 3 && noClick) {
                  CouponAlertInfo(mid, uids, iterIds, events, System.currentTimeMillis())
                }else{
                  null
                }*/
        //疑是预警日志
        (uids.size() >= 3 && noClick, CouponAlertInfo(mid, uids, iterIds, events, System.currentTimeMillis()))

    }

    //8.生成预警日志
    val AlertLog: DStream[CouponAlertInfo] = AlertDStream.filter(_._1).map(_._2)
    AlertLog.print()

    //9写入ES
    AlertLog.foreachRDD(
      rdd=>{
        rdd.foreachPartition(
          itre =>{
            //创建索引名
            val todayStr: String = sdf.format(new Date(System.currentTimeMillis())).split(" ")(0)
            val indexName =s"${GmallConstants.ES_ALERT_INDEX_PRE}-$todayStr"
            //处理数据，补充docId
            val doList: List[(String, CouponAlertInfo)] = itre.toList.map(
              alertInfo => {
                val m: Long = alertInfo.ts / 1000 / 60
                (s"${alertInfo.mid}-$m", alertInfo)
              })
            //写入ES
          MyEsUtil.insertBulk(indexName,doList)
          })
      })
    //10
    ssc.start()
    ssc.awaitTermination()
  }
}
