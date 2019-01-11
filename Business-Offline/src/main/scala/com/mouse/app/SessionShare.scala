package com.mouse.app

import java.text.SimpleDateFormat

import com.alibaba.fastjson.JSONObject
import com.mouse.common.beans.UserVisitInfo
import com.mouse.utils.SessionAccumulator
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession


object SessionShare {
  def CountSession(gatherRDD: RDD[(String, Iterable[UserVisitInfo])],sparkSession:SparkSession,taskID:String,jsonObject: JSONObject):Unit={
    //注册累加器
    val sessionAcc  = new SessionAccumulator
    sparkSession.sparkContext.register(sessionAcc)
    //将相同Key中的数据中，提取最大值和最小值，取差值，获取session差
    gatherRDD.foreach(x=>{
      //定义最大值和最小值，其初始是 -- 最大值初始值为0，最小值初始值是long的最大值
      var minTime=Long.MaxValue
      var maxTime=0L
      x._2.foreach(info=>{
        //将用户数据时间字符串转变成long类型
        var actionTimeMs:Long=new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(info.action_time).getTime
        //获取整个Iterable中的最大值与最小值
        minTime = math.min(minTime,actionTimeMs)
        maxTime=math.max(maxTime,actionTimeMs)
      })
      //计算session时差
      val sessionVisitTime=maxTime-minTime
      //依据session访问时间差，采用累加器进行数据累加
      val msg1 = sessionVisitTime match {
        case msg:Long if(msg>10000)=> sessionAcc.add("visit_than_10000")
        case msg:Long if(msg<=10000)=>sessionAcc.add("visit_less_10000")
      }
      //获取session步长
      val sessionStep:Int = x._2.size
      //依据session步长统计数据
      val msg =sessionStep match {
        case msg:Int if(msg>5) =>sessionAcc.add("step_than_5")
        case msg:Int if(msg<=5)=>sessionAcc.add("step_less_5")
      }



    })

  }

}
