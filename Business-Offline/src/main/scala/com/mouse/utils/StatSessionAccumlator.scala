package com.mouse.utils

import org.apache.spark.util.AccumulatorV2

import scala.collection.mutable

class StatSessionAccumlator extends AccumulatorV2[String,mutable.HashMap[String,(Long,mutable.HashMap[String,Long])]]{
  var orginalAcc = new mutable.HashMap[String,(Long,mutable.HashMap[String,Long])]
  override def isZero: Boolean = orginalAcc.isEmpty

  override def copy(): AccumulatorV2[String, mutable.HashMap[String, (Long, mutable.HashMap[String, Long])]] = {
    val copyAcc = new StatSessionAccumlator
    copyAcc.value++=orginalAcc
    copyAcc
  }

  override def reset(): Unit = {
    orginalAcc = new mutable.HashMap[String,(Long,mutable.HashMap[String,Long])]
  }

  override def add(data: String): Unit = {
    //data格式->  16,9_4c4f5b4
    val array: Array[String] = data.split("_")
    val category_Id_array: Array[String] = array(0).split(",")
    val session_id=array(1)
    category_Id_array.foreach(category_id=>{
      //不存在，则创建新的
      if(!orginalAcc.contains(category_id)){
        val map = new mutable.HashMap[String,Long]
        map.put(session_id,1L)
        orginalAcc.put(category_id,(1L,map))
        return
      }
      //存在则累加数据
      val tuple: (Long, mutable.HashMap[String, Long]) = orginalAcc.get(category_id).get
      tuple._2.put(session_id,tuple._2.getOrElse(session_id,0L)+1L)
      val tuple1:Long=tuple._1 + 1L
      val tuple2:mutable.HashMap[String, Long]=tuple._2
      orginalAcc.put(category_id,(tuple1,tuple2))
    })
  }

  override def merge(other: AccumulatorV2[String, mutable.HashMap[String, (Long, mutable.HashMap[String, Long])]]): Unit = {
    val preAcc = orginalAcc
    val postAcc = other.value
    postAcc.foreach{case (postKey,postTuple)=>{
      //原始Map里不存在该Key,则添加
      if( !preAcc.contains(postKey)){
        preAcc.put(postKey,postTuple)
        return
      }
      //获取原始数据元组
      val preTuple: (Long, mutable.HashMap[String, Long]) = preAcc.get(postKey).get
      //俩元组数据累加
      val categoryCount= preTuple._1 + postTuple._1
      //获取原始元组的HashMap
      val preTupleMap = preTuple._2
      postTuple._2.foreach{case (postTupleMapKey,postTupleMapCount)=>{
        val newSessionCount=preTupleMap.getOrElse(postTupleMapKey,0L)+postTupleMapCount
        preTupleMap.put(postTupleMapKey,newSessionCount)
      }}
      preAcc.put(postKey,(categoryCount,preTupleMap))
    }}
  }

  override def value: mutable.HashMap[String, (Long, mutable.HashMap[String, Long])] = orginalAcc
}
