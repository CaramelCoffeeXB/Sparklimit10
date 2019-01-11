package com.mouse.app

import com.mouse.beans.AdsInfo
import com.mouse.common.utils.RedisUtil
import org.apache.spark.SparkContext
import org.apache.spark.streaming.dstream.DStream
import redis.clients.jedis.Jedis

/**
  * @author msi-
  * @date 2019/1/7 - 23:17
  */
/**
  * 将数据经过黑名单过滤后，统计各个城市一天的广告数量
  */
object Realtime_ADS_Stat {
      //传入sparkStreaming 以及SparkContext
      def stat_City_ADS(adsInfoDStream: DStream[AdsInfo],sparkContext: SparkContext):Unit={
         //1. 将相同的key 进行合并操作
            val streamingStat: DStream[(String, Long)] = adsInfoDStream.map(adsInfo=>{
              (adsInfo.createKeyBy_Day_Area_City_AdsId(),1L)}).reduceByKey(_+_)
        //2. 因为需要一天的数据，所以需要进行数据总体累加操作
        // 采用updateStateByKey 将会把相同Key的值进行累加计算，不同的是，会将新产生的值与原值进行数据合并
            /**
              * 一 UpdateStateByKey
              UpdateStateByKey：统计全局的key的状态，但是就算没有数据输入，他也会在每一个批次的时候返回之前的key的状态。
              假设5s产生一个批次的数据，那么5s的时候就会更新一次的key的值，然后返回。
              这样的缺点就是，如果数据量太大的话，而且我们需要checkpoint数据，这样会占用较大的存储。
              如果要使用updateStateByKey,就需要设置一个checkpoint目录，开启checkpoint机制。
              因为key的state是在内存维护的，如果宕机，则重启之后之前维护的状态就没有了，
              所以要长期保存它的话需要启用checkpoint，以便恢复数据
              如果需要存储到  hdfs上，那么只需要
              String checkpointDir= "hdfs://hdfs-cluster/ecommerce/checkpoint/state";
              rdd.checkpoint(checkpointDir);

              二 MapWithState
             也是用于全局统计key的状态，但是它如果没有数据输入，便不会返回之前的key的状态，
             有一点增量的感觉。
             这样做的好处是，我们可以只是关心那些已经发生的变化的key，对于没有数据输入，
             则不会返回那些没有变化的key的数据。
             这样的话，即使数据量很大，checkpoint也不会像updateStateByKey那样，占用太多的存储。
              */
            val statAllBykey=streamingStat.updateStateByKey((countSeq: Seq[Long],realtimeStat:Option[Long])=>{
                  val newValue=realtimeStat.getOrElse(0L)+countSeq.sum
                  Some(newValue)
            })
            //设置检查点，该方法需要设置检查点避免数据量太大
            //可以是hdfs .也可以是本地文件系统
            sparkContext.setCheckpointDir("./checkPoint")
            statAllBykey.foreachRDD(rdd=>{
                  rdd.foreachPartition{statInfo=>
                    val redisConn: Jedis = RedisUtil.getJedisClient
                    statInfo.foreach{case (key,value)=>{
                      redisConn.hincrBy("Day_Area_City_AdsId",key,value)
                    }}
                    redisConn.close()
                  }
            })
      }
}
