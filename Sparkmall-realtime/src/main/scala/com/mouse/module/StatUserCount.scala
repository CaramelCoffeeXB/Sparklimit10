package com.mouse.module

import java.text.SimpleDateFormat

import com.mouse.beans.AdsInfo
import com.mouse.common.utils.RedisUtil
import org.apache.spark.streaming.dstream.DStream
import redis.clients.jedis.Jedis

/**
  * 对每一条数据进行累计，如果发现数据量超过100,则加入黑名单
  */
object StatUserCount {
  /**
    *  每条数据进来进行累加，如果超过100，则直接拉入黑名单，过滤掉数据
    */
  def statUserCount(adsInfoDStream:DStream[AdsInfo]) ={
    adsInfoDStream.foreachRDD{rdd=>{
      //每个分区内的数据进行检查
      rdd.foreachPartition{ adsIter =>{
        val redisConn: Jedis = RedisUtil.getJedisClient
        for(adsInfo <- adsIter){
          //redis 中的对象值,由 日+广告+用户组成
          val everydayKey = adsInfo.dateFormat_day+"_"+adsInfo.adsId+"_"+adsInfo.userId
          //存入redis,不存在，存入给定的值，存在则进行累加
          val currentCountNum: Long = redisConn.hincrBy("ADS_count",everydayKey,1).toLong
          //如果该数大于100，则添加进黑名单
          if(currentCountNum>=1000){
            redisConn.sadd("Blacklist",adsInfo.userId)
          }
        }
        redisConn.close()
      }}
    }}
  }
}
