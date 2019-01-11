package com.mouse.utils

import org.apache.spark.util.AccumulatorV2

import scala.collection.mutable

/**
  * session用户的累加器，输入值是key值，输出值是HashMap,里面是key ， 以及相同key的所属个数
  */
class SessionCountAccumulator extends AccumulatorV2[String,mutable.HashMap[String,Long]]{
  var sessionMapAccumulator:mutable.HashMap[String,Long]=new mutable.HashMap[String,Long]()
  override def isZero: Boolean = sessionMapAccumulator.isEmpty

  override def copy(): AccumulatorV2[String, mutable.HashMap[String, Long]] = {
    val accumulator = new SessionCountAccumulator
    accumulator
  }

  override def reset(): Unit = {
    this.sessionMapAccumulator = new mutable.HashMap[String,Long]()
  }

  override def add(key: String): Unit = {
    sessionMapAccumulator(key)=sessionMapAccumulator.getOrElse(key,0L)+1L
  }

  override def merge(other: AccumulatorV2[String, mutable.HashMap[String, Long]]): Unit = {
    val sessionMapOther: mutable.HashMap[String, Long]=other.value
    sessionMapOther.foreach(x=>{
      sessionMapAccumulator(x._1)=sessionMapAccumulator.getOrElse(x._1,0L)+x._2
    })
  }

  override def value: mutable.HashMap[String, Long] = {sessionMapAccumulator}
}
