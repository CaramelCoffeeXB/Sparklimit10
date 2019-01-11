package com.mouse

import java.text.SimpleDateFormat
import java.util.Date

import org.apache.spark.streaming.{Minutes, Seconds, StreamingContext}
import com.mouse.app.{Area_ADS_Top3, RealtimeHoursADS, Realtime_ADS_Stat}
import com.mouse.beans.AdsInfo
import com.mouse.common.utils.{MyKafkaUtil, RedisUtil}
import com.mouse.module.{BlackList, StatUserCount}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.{SparkConf, SparkContext}





object RealtimeApp{
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setAppName("realtime_ads").setMaster("local[*]")
    val sparkContext = new SparkContext(sparkConf)
    //spark流计算文件配置,计算时间为5秒一次
    val streamingContext = new StreamingContext(sparkContext,Seconds(5))
    val recordStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream("ads_logs",streamingContext)
    //将数据切割后转成对象
    val AdsInfo_Object: DStream[AdsInfo] = recordStream.map { record => {
      val ads: Array[String] = record.value().split(" ")
      val dateMark: String = new SimpleDateFormat("yyyy-MM-dd").format(new Date(ads(0).toLong))
      AdsInfo(ads(0).toLong,dateMark, ads(1), ads(2), ads(3), ads(4))
    }}

    //检验是否为黑名单
    val filterTotal: DStream[AdsInfo] = BlackList.filterBlacklist(AdsInfo_Object,sparkContext)
    //将过滤后的数据传入进行累加
    StatUserCount.statUserCount(filterTotal)
    //****************************************************************
    //统计城市一天广告量
   // Realtime_ADS_Stat.stat_City_ADS(total,sparkContext)
    //****************************************************************
    //统计区域一天广告量
    //Area_ADS_Top3.stat_ADS_Top3(total,sparkContext)
    //***************************************************************
    //实时一个小时的数据
    RealtimeHoursADS.realtimeADS(filterTotal)

    streamingContext.start()
    streamingContext.awaitTermination()
  }
}
