package com.mouse.app

import com.mouse.beans.AdsInfo
import com.mouse.common.utils.RedisUtil
import org.apache.spark.SparkContext
import org.apache.spark.streaming.dstream.DStream
import redis.clients.jedis.Jedis


object Area_ADS_Top3 {
    def stat_ADS_Top3(adsInfoDStream: DStream[AdsInfo],sparkContext: SparkContext):Unit= {
        //将 DStream[AdsInfo] 对象转变成（“时间+区域+广告”，广告数）格式元组
        val ads_Area_count = adsInfoDStream.map { adsinfo =>
            (adsinfo.dateFormat_day + "_" + adsinfo.area + "#" + adsinfo.adsId, 1L)
        } reduceByKey (_ + _)
        //将每条数据元组再次切割，进行再次细化
        val everyArea_array: DStream[(String, Iterable[(String, Long)])] = ads_Area_count.map { case (key, ads_Count) => {
            val day_Area: Array[String] = key.split("#")
            // 日期_区域        广告      广告数
            (day_Area(0), (day_Area(1), ads_Count))
        }
        }.groupByKey()
        import org.json4s.native.JsonMethods
        import org.json4s.JsonDSL._
        val dataToRedis: DStream[(String, String)] = everyArea_array.map { case (areaKey, adsId_list) => {
            val adsId_count = adsId_list.toList.sortWith(_._2 > _._2).take(3)
            val top3AdsCountJson: String = JsonMethods.compact(JsonMethods.render(adsId_count))
            //返回格式 日期+区域   每个区域top3的Json
            (areaKey, top3AdsCountJson)
        }}

        //将数据加入redis
        dataToRedis.foreachRDD{rdd=>
          rdd.foreachPartition{ area_top3=>
            val redisConn: Jedis = RedisUtil.getJedisClient //driver
            area_top3.foreach{case (day_Are_Key,top3Ads)=>
              //import  collection.JavaConversions._
              val day_Are=day_Are_Key.split("_")
              redisConn.hset("area_top3_"+day_Are(0),day_Are(1),top3Ads)
            }
            //记得关闭redis连接
            redisConn.close()
          }

        }

    }
}
