package com.mouse.common.utils

import org.apache.commons.configuration2.{FileBasedConfiguration, PropertiesConfiguration}
import org.apache.commons.configuration2.builder.FileBasedConfigurationBuilder
import org.apache.commons.configuration2.builder.fluent.Parameters

/**
  * 此工具类用于读取配置文件的数据。
  */
object FileConfiUtils {
  def apply(propertiesName:String) = {
    val fileConfiUtils = new FileConfiUtils()
    if (fileConfiUtils.confi == null) {
      fileConfiUtils.confi = new FileBasedConfigurationBuilder[FileBasedConfiguration](classOf[PropertiesConfiguration]).configure(new Parameters().properties().setFileName(propertiesName)).getConfiguration
    }
    fileConfiUtils
  }

  def main(args: Array[String]): Unit = {
    val config: FileBasedConfiguration = FileConfiUtils("config.properties").confi
    println(config.getString("jdbc.user"))
  }

}

class FileConfiUtils{
  var confi:FileBasedConfiguration=null
}
