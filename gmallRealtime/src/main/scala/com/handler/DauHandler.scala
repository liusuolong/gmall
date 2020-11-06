package com.handler

import java.lang

import com.bean.StartUpLog
import com.utils.RedisUtil
import org.apache.spark.SparkContext
import org.apache.spark.streaming.dstream.DStream
import redis.clients.jedis.Jedis

/**
 * @author ymstart
 * @create 2020-11-05 14:36
 */

object DauHandler {

  /**
   * 同批次去重
   * @param fieldedByRedis
   */
  def fielderByMid(fieldedByRedis: DStream[StartUpLog]): DStream[StartUpLog] = {
    //1.转换数据结构 log => ((log.mid,log.logDate),log)
    val filteredByMid: DStream[StartUpLog] = fieldedByRedis.map(log => ((log.mid, log.logDate), log))
      .groupByKey()
      .mapValues(
        iter => iter.toList.sortWith(_.ts < _.ts).take(1)
      ).flatMap(_._2)

    filteredByMid


     //wzx
/*    fieldedByRedis.map(log => ((log.mid, log.logDate), log))
      .reduceByKey((left, right) => if(left.ts < right.ts) left else right)
      .map(_._2)*/
  }

  /**
   * 对redis跨批次去重
   *
   * @param startUpLogDStream
   */
  def fielderByRedis(startUpLogDStream: DStream[StartUpLog]):DStream[StartUpLog] = {
    //方案一 : 一次过滤一条
/*    val value: DStream[StartUpLog] = startUpLogDStream.filter(
      log => {
        //1.获取连接
        val jedisClient: Jedis = RedisUtil.getJedisClient
        //2.判断数据是否存在mid
        val redisKey = s"DAU:${log.logDate}"
        val boolean: lang.Boolean = jedisClient.sismember(redisKey, log.mid)

        //3.关闭连接
        jedisClient.close()

        !boolean
      }
    )*/
    //方案二 : 一次处理一个分区
    val value2: DStream[StartUpLog] = startUpLogDStream.transform(
      rdd => {
        rdd.mapPartitions(
          iter => {
            //1.获取连接
            val jedisClient: Jedis = RedisUtil.getJedisClient
            //2.处理业务
            val logs: Iterator[StartUpLog] = iter.filter(log => {
              val redisKey = s"DAU${log.logDate}"
              !jedisClient.sismember(redisKey, log.mid)
            })
            //3.关闭连接
            jedisClient.close()

            logs
          }
        )
      }
    )
    value2
    //value
  }


  /**
   * 两次去重后的结果
   * 将去重的mid保存到redis
   *
   * @param startUpLogDStream
   */
  def saveMidToRedis(startUpLogDStream: DStream[StartUpLog]): Unit = {
    startUpLogDStream.foreachRDD(
      rdd => {
        //分区处理
        rdd.foreachPartition(
          iter => {
            //1.获取连接
            val jedisClient: Jedis = RedisUtil.getJedisClient
            //遍历写入库中
            iter.foreach(log => {
              val redisKey = s"DAU:${log.logDate}"
              //写入
              jedisClient.sadd(redisKey,log.mid)
            })
            //归还连接
            jedisClient.close()
          }
        )
      }
    )
  }

}
