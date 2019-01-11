package com.mouse.app

import com.mouse.beans.CategoryCountInfo
import com.mouse.common.beans.UserVisitInfo
import com.mouse.common.utils.{Get_UUID, JdbcUtil}
import com.mouse.utils.SessionCountAccumulator
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.collection.mutable

/**
  * 统计商品Top10
  */
object CategoryTop10 {
    def statCategory(actionRDD: RDD[UserVisitInfo],sparkSession: SparkSession):Unit={

      //注册累加器
      val sessionCountAcc = new SessionCountAccumulator
      sparkSession.sparkContext.register(sessionCountAcc)
      actionRDD.foreach(info => {
        //获取UserVisitInfo 中的 点击，下单，支付数量
        //1.依据商品种类统计一次点击次数
        if (info.click_category_id != -1L) {
          sessionCountAcc.add(info.click_category_id + "_click")
        } else if (info.pay_category_ids != null && !"".equals(info.pay_category_ids)) {
          //2.统计支付的商品种类次数，由于数据2,3,4这样的字符串，所以需要切割统计
          info.pay_category_ids.split(",").foreach(x => {
            sessionCountAcc.add(x + "_pay")
          })
        } else if (info.order_category_ids != null && !"".equals(info.order_category_ids)) {
          //3.统计下单的次数
          info.order_category_ids.split(",").foreach(x => {
            sessionCountAcc.add(x + "_order")
          })
        }
      })
      println("数据量"+actionRDD.count())
      //将各个分区的结果返回回累加器后，将数据进行统计
      val categoryCountMap: mutable.HashMap[String, Long] = sessionCountAcc.value
      //将数据进行相同品类key分组，将iterable的数据进行封装
      val categoryCountInfoList: List[CategoryCountInfo] = categoryCountMap.groupBy(x => {
        x._1.split("_")(0)
      }).map { case (cid, dataSet) => {
        CategoryCountInfo(Get_UUID.mk_UUID, cid, dataSet.getOrElse(cid + "_click", 0L), dataSet.getOrElse(cid + "_order", 0L), dataSet.getOrElse(cid + "_pay", 0L))
      }
      }.toList
//测试多级排序是否有效的 测试数据
//      val newArray=new mutable.ListBuffer[CategoryCountInfo]
//      newArray++=categoryCountInfoList
//      newArray+=CategoryCountInfo(Get_UUID.mk_UUID,"9",1818L,560L,200L)
//      newArray+=CategoryCountInfo(Get_UUID.mk_UUID,"12",1818L,560L,402L)
//      newArray+=CategoryCountInfo(Get_UUID.mk_UUID,"13",1818L,560L,199L)
//     val toList: List[CategoryCountInfo] = newArray.toList
      println("*******************************************************************************************")
      //将数据转变成集合后进行排序
      val sortInfo: List[CategoryCountInfo] = categoryCountInfoList.sortWith{(info1,info2)=>{
        if(info1.click_count>info2.click_count){
          true
        }else if(info1.click_count==info2.click_count){
          if(info1.order_count>info2.order_count){true
          }else if(info1.order_count==info2.order_count){
            if(info1.pay_count>=info2.pay_count){true}
            else{
              false
            }
          }else{
            false
          }
        }else{
          false
        }
      }}.take(10)
      //sortInfo.foreach(x=>println(x.taskId+" -- "+x.category_id+" -- "+x.click_count+" -- "+x.order_count+" -- "+x.pay_count))
      //将返回的数据取Top10之后，将对象转变成List.便于mysql数据插入
      val list2Mysql: List[Array[Any]] = sortInfo.take(10).map(Top10List => Array(
        Top10List.taskId,
        Top10List.category_id,
        Top10List.click_count,
        Top10List.order_count,
        Top10List.pay_count))
      JdbcUtil.executeBatchUpdate("insert into category_top10 values (?,?,?,?,?)",list2Mysql)
    }

}
