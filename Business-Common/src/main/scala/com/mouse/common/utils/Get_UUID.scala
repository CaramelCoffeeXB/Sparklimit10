package com.mouse.common.utils

import java.net.InetAddress
import java.util.UUID

/**
  * @author msi-
  * @date 2019/1/4 - 22:48
  */
object Get_UUID {
    def mk_UUID:String={
      //生成唯一的字段ID,通过UUID获取，集群模式可能出现相同，所以后缀加上IP地址
      val hostString: String = InetAddress.getLocalHost.toString
      val taskID: String = UUID.randomUUID().toString+hostString.split("\\.")(3)
      taskID
    }
}
