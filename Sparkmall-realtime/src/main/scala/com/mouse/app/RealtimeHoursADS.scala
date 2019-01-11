package com.mouse.app

import java.text.SimpleDateFormat
import java.util.Date

import com.mouse.beans.AdsInfo
import com.mouse.common.utils.RedisUtil
import org.apache.spark.streaming.{Minutes, Seconds}
import org.apache.spark.streaming.dstream.DStream
import org.json4s.native.JsonMethods
import org.json4s.JsonDSL._
import redis.clients.jedis.Jedis

/**
  * @author 咖啡不加糖
  * @Date: 2019/1/10 22:07
  */
object RealtimeHoursADS {
      def realtimeADS(AdsInfoDStream: DStream[AdsInfo]) ={
        val windowsStreaming: DStream[AdsInfo] = AdsInfoDStream.window(Minutes(60),Seconds(20))
        val rddTotal =windowsStreaming.map{info=>
          val minutesDate: String = new SimpleDateFormat("HH:mm").format(new Date(info.date_to_String))
          println(minutesDate + "_" + info.adsId)
          (minutesDate + "_" + info.adsId, 1L)
        }.reduceByKey(_+_).map{case (key,count)=>
          val strings: Array[String] = key.split("_")
          (strings(1),(strings(0),count))
        }.groupByKey()

        rddTotal.foreachRDD{rdd=>
          rdd.foreach{case (key,values)=>
            val redisConn: Jedis = RedisUtil.getJedisClient
            val dateAndCountJson: String = JsonMethods.compact(JsonMethods.render(values))
            redisConn.hset("realTimeTotal",key,dateAndCountJson)
            redisConn.close()
          }
        }
      }
}
