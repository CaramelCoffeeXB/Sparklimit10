package com.mouse.beans

import java.text.SimpleDateFormat
import java.util.Date

/**
  *  注意：date_to_String:Long,   dateFormat_day:String 是一个东西，留下date_to_String
  *  是为了天以内的需求
  * @param date_to_String  时间
  * @param dateFormat_day 将时间转化为以天为日期的字符串
  * @param area 区域
  * @param city 区域内所在城市
  * @param userId 用户Id
  * @param adsId 广告Id
  */
case class AdsInfo (date_to_String:Long,dateFormat_day:String,area:String,city:String,userId:String,adsId:String) {

  def createKeyBy_Day_Area_City_AdsId():String={
    return dateFormat_day+":"+area+":"+city+":"+adsId
  }
}
