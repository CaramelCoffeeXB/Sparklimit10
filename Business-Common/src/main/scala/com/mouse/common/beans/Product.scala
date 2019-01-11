package com.mouse.common.beans

/**
  * -------->产品表信息
  * @param product_id  商品ID
  * @param product_name  产品名
  * @param extend_info  商品额外信息
  */
case class Product(product_id: Long, product_name: String, extend_info: String)
