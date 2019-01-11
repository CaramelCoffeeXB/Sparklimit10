package com.mouse.module

import java.util

import com.mouse.beans.AdsInfo
import com.mouse.common.utils.RedisUtil
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.DStream

/**
  * 黑名单数据是统计一天，同一用户点击广告的数量超过100，则拉入黑名单中，即过滤掉数据
  */
object BlackList {

  /**
    * 将黑名单中的数据过滤出来。不过存在广播变量需要动态发生改变
    * 所以需要采用   transform  ,定期更新广播变量
    */
  def filterBlacklist(adsInfoDStream: DStream[AdsInfo],sparkContext: SparkContext): DStream[AdsInfo] ={
    adsInfoDStream.transform{adsInfoRDD=>{
      //获取redis连接
      val redisConn = RedisUtil.getJedisClient
      //将黑名单数据传入广播变量
      val blacklist: util.Set[String] = redisConn.smembers("Blacklist")
      val blacklist_broadcast: Broadcast[util.Set[String]] = sparkContext.broadcast(blacklist)
      //以上都在driver 上执行
      val filterRDD: RDD[AdsInfo]=adsInfoRDD.filter(adsInfo=>{
        //executor上执行
        !blacklist_broadcast.value.contains(adsInfo.userId)
      })
      redisConn.close()
      filterRDD
    }}
  }
}
