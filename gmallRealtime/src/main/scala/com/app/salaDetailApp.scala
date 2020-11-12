package com.app

import java.util

import com.alibaba.fastjson.JSON
import com.bean.{OrderDetail, OrderInfo, SaleDetail}
import com.utils.{MyKafkaUtil, RedisUtil}
import com.ym123.GmallConstants
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.json4s.DefaultFormats

import scala.collection.mutable.ListBuffer
import org.json4s.native.Serialization
import redis.clients.jedis.Jedis
import collection.JavaConverters._

/**订单详情与订单进行双流join并根据use_id 在redis中查询数据
 *
 * @author ymstart
 * @create 2020-11-10 20:45
 */
object salaDetailApp {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("salaDetailApp")
    val ssc = new StreamingContext(sparkConf, Seconds(5))

    //从kafka中获取数据
    //订单详情
    val orderDetailInputDStream: InputDStream[ConsumerRecord[String, String]] =
      MyKafkaUtil.getKafkaStream(GmallConstants.GMALL_ORDER_DETAIL,ssc)
    //订单
    val orderInfoInputDStream: InputDStream[ConsumerRecord[String, String]] =
      MyKafkaUtil.getKafkaStream(GmallConstants.GMALL_ORDER_INFO,ssc)

    //将数据转换为样例类
    //订单
    val orderInfoDStream: DStream[(String, OrderInfo)] = orderInfoInputDStream.map(
      record => {
        //a.取出value并封装到样例类中
        val orderInfo: OrderInfo = JSON.parseObject(record.value(), classOf[OrderInfo])
        //b.取出样例类中的create_time
        val create_time: String = orderInfo.create_time
        //c.重定义时间格式
        val timeArr: Array[String] = create_time.split(" ")
        orderInfo.create_date = timeArr(0)
        orderInfo.create_hour = timeArr(1).split(":")(0)
        //d.数据脱敏
        val tuple: (String, String) = orderInfo.consignee_tel.splitAt(4)
        tuple._1 +: "*****"
        //e.返回结果
        (orderInfo.id, orderInfo)

      })
    //订单详情
    val orderDetailDStream: DStream[(String, OrderDetail)] = orderDetailInputDStream.map(
      record => {
        val orderDetail: OrderDetail = JSON.parseObject(record.value(), classOf[OrderDetail])
        (orderDetail.order_id, orderDetail)
      })

    //full join
    val fullDStream: DStream[(String, (Option[OrderInfo], Option[OrderDetail]))] = orderInfoDStream.fullOuterJoin(orderDetailDStream)

    //数据处理
    fullDStream.mapPartitions(
      iter=>{
        implicit val formats: DefaultFormats.type = org.json4s.DefaultFormats
        //创建集合存放关联数据
        val details = new ListBuffer[SaleDetail]
        //获取redis连接
          val jedisClient: Jedis = RedisUtil.getJedisClient


        //遍历迭代器 数据处理
        iter.foreach{
          case (orderId,(optInfo,optDetail))=>{

            val orderInfokey = s"order_Info:$orderId"
            val orderDetailKey = s"order_Detail:$orderId"

            //a.判断
            if(optInfo.isDefined){
              //optinfo 不为空
              //取出optInfo数据
              val optInfoData: OrderInfo = optInfo.get
              //a.1 orderDetail不为空
              if(optDetail.isDefined){
                //取出orderDetail数据
                val orderDetailData: OrderDetail = optDetail.get
                //将数据放入SaleDetail集合中
                val saleDetail = new SaleDetail(optInfoData,orderDetailData)
                details += saleDetail
              }
              //a.2将数据缓存到redis
              val infoStr: String = Serialization.write(optInfo)
              jedisClient.set(orderInfokey,infoStr)
              //添加超时时间
              jedisClient.expire(orderInfokey,100)

              //a.3判断redis中是否有对应的Detail数据
              if (jedisClient.exists(orderDetailKey)){
                val detailJsonSet: util.Set[String] = jedisClient.smembers(orderDetailKey)
                detailJsonSet.asScala.foreach(detailJson=>{
                  val orderDetail: OrderDetail = JSON.parseObject(detailJson,classOf[OrderDetail])
                  //将获取的orderInfo 与 redis中的对应的 orderDetail组合放入集合中
                  details += new SaleDetail(optInfoData,orderDetail)
                })
              }
            }else{
              //opyinfo 为空
              //b.获取Detail数据
              val orderDetail: OrderDetail = optDetail.get
              //查看redis缓冲中是否有对应的orderInfo数据
              if(jedisClient.exists(orderInfokey)){
                val orderInfoStr: String = jedisClient.get(orderInfokey)
                //转换为样例类
                val orderInfo: OrderInfo = JSON.parseObject(orderInfoStr,classOf[OrderInfo])
                val saleDetail = new SaleDetail(orderInfo,orderDetail)
                //加入集合中
                details += saleDetail
              }else{
                //b.2查询redis中没有对应数据数据
                val orderDetailStr: String = Serialization.write(orderDetail)
                //写入redis
                jedisClient.sadd(orderDetailKey,orderDetailStr)
                //添加超时时间
                jedisClient.expire(orderDetailKey,100)
              }
            }
          }
        }
        //归还连接
        jedisClient.close()
        //返回值
        details.iterator
      })
    ssc.start()
    ssc.awaitTermination()
  }
}
