package com.mouse.produce.mock.utils

import scala.collection.mutable
import scala.util.Random

/**
  * 用于模拟数据时生成随机数
  */
object RandomNum {
  def apply(fromNum:Int,toNum:Int): Int =  {
    fromNum+ new Random().nextInt(toNum-fromNum+1)
  }
  def multi(fromNum:Int,toNum:Int,amount:Int,delimiter:String,canRepeat:Boolean):String ={
    // 实现方法  在fromNum和 toNum之间的 多个数组拼接的字符串 共amount个
    //用delimiter分割  canRepeat为false则不允许重复
    val randomNum = new scala.collection.mutable.ArrayBuffer[Int]()
    for(x <- 0 until amount){
      var i = apply(fromNum,toNum)
      if(canRepeat){
        randomNum+=i
      }else{
        while (randomNum.contains(i)){
          i = apply(fromNum,toNum)
        }
        randomNum+=i
      }
    }
    randomNum.mkString(delimiter)
  }

}
