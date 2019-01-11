package com.mouse.app

import com.mouse.common.beans.UserVisitInfo
import com.mouse.common.utils.{Get_UUID, JdbcUtil}
import com.mouse.utils.{SessionAccumulator, SessionCountAccumulator, StatSessionAccumlator}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.collection.mutable

/**
  * 该类主要用于计算所有热门商品种类Top10中，以及每个热门种类的session_id 的Top10
  */
object CategoryAndSessionIdTop10 {
 def statCategoryAndSession(actionRDD: RDD[UserVisitInfo],sparkSession: SparkSession)={
   // searchWord --- click_category_id (Long)--- order_category_ids(String) --- pay_category_ids（String）
   //公有的是 session_id
   // 四者者互斥
   val accumulator = new StatSessionAccumlator
   //val accumulator = new SessionAccumulator
   sparkSession.sparkContext.register(accumulator)
   actionRDD.foreach(info=>{
     var conditions=""
     if(info.click_category_id != -1L){
       conditions=info.click_category_id.toString
     }else if (info.order_category_ids!=null && ! "".equals(info.order_category_ids)){
       conditions=info.order_category_ids
     }else if(info.pay_category_ids!=null && !"".equals("info.pay_category_ids")){
       conditions=info.pay_category_ids
     }
     if(!"".equals(conditions)){
       val trim: String = conditions.trim
       accumulator.add(trim+"_"+info.session_id)
     }
   })
   //4.将累加器的数据进行Top10计算
  val totalData: mutable.HashMap[String, (Long, mutable.HashMap[String, Long])] = accumulator.value
   totalData.toList.sortWith(_._2._1>_._2._1).take(10).foreach{case (categorykey,cagegoryTuple)=>{
     cagegoryTuple._2.toList.sortWith(_._2>_._2).take(10).foreach{case (session_id,sessionCount)=>{
       println(Get_UUID.mk_UUID+" --> "+categorykey+" --> "+cagegoryTuple._1+" --> "+session_id+" --> "+sessionCount)
       var array = Array(Get_UUID.mk_UUID,categorykey,cagegoryTuple._1,session_id,sessionCount)
       //JdbcUtil.executeUpdate("insert into CategoryAndSessionIdTop10 values(?,?,?,?,?)",array)
       //array=null
     }}
   }}
 }


}
