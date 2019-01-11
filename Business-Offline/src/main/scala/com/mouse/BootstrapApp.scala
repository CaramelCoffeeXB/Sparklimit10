package com.mouse


import com.alibaba.fastjson.{JSON, JSONObject}
import com.mouse.app.{CategoryAndSessionIdTop10, CategoryTop10, SessionShare}
import com.mouse.common.beans.UserVisitInfo
import com.mouse.common.utils.FileConfiUtils
import org.apache.commons.configuration2.FileBasedConfiguration
import org.apache.spark.{Partition, SparkConf, TaskContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, SparkSession}

import scala.collection.mutable

object BootstrapApp {
  def main(args: Array[String]): Unit = {
    //设置运行模式
    val sparkConf: SparkConf = new SparkConf().setAppName("offline").setMaster("local[*]")
    //获取sparkSQl操作对象
    val sparkSession: SparkSession = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate()
    //读取配置文件中的属性
   val conditionConfig:FileBasedConfiguration = FileConfiUtils("conditions.properties").confi
    //读取配置文件中的json字符串。
    val jsonString: String = conditionConfig.getString("condition.params.json")
    //转成object对象
    val jsonObject: JSONObject = JSON.parseObject(jsonString)
    //第一步： ----> 将HDFS的查找的数据转成RDD,依据json的条件过滤数据
    val actionRDD: RDD[UserVisitInfo] = userActionRDD(sparkSession,jsonObject)
    //将数据缓存
    //actionRDD.cache()

    //****************************************************************************************************************************
    //第二步： ----> 将过滤的数据，通过groupByKey转换成 key , Iterable[UserVisitInfo]
    //val gatherRDD: RDD[(String, Iterable[UserVisitInfo])] = actionRDD.map(info=>(info.session_id,info)).groupByKey()
    //第三步： --->
    //SessionShare.CountSession(gatherRDD,sparkSession,taskID,jsonObject)
    //****************************************************************************************************************************
    //需求三
    //CategoryTop10.statCategory(actionRDD,sparkSession)
    //需求四
    CategoryAndSessionIdTop10.statCategoryAndSession(actionRDD,sparkSession)
    //CateAndSessTop10.statTop10(actionRDD,sparkSession)
    SparkSession.builder().getOrCreate().sparkContext.stop()
  }

  /**
    * conditions.properties文件里面是限制条件
    * SparkSession是SQL操作；JSONObject 里面就是限制文件条件
    */
  def userActionRDD(sparkSession:SparkSession,conditionJSONObject:JSONObject):RDD[UserVisitInfo]={
    var sql = "select v.* from user_visit_info v join user_info u on v.user_id=u.user_id where 1=1"
    //将数据存入Map集合中
    val map = new mutable.HashMap[String,String]
    map.put("startDate",conditionJSONObject.getString("startDate"))
    map.put("endDate",conditionJSONObject.getString("endDate"))
    map.put("startAge",conditionJSONObject.getString("startAge"))
    map.put("endAge",conditionJSONObject.getString("endAge"))
    map.map(x=>{x._1 match {
      case ("startDate")=>sql+=" and date >= '"+x._2+"'"
      case ("endDate")=>sql+=" and date <= '"+x._2+"'"
      case ("endAge")=>sql+=" and u.age<= "+x._2
      case ("startAge")=>sql+=" and u.age>= "+x._2
    }})
    sparkSession.sql("use spark_business")
    import sparkSession.implicits._
    //sparkSession.sql(sql+ " limit 50 ").show
    println("--->  "+sql)
    sparkSession.sql(sql).as[UserVisitInfo].rdd
  }

}
