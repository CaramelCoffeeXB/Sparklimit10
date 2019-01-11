package com.mouse.common.beans

/**
  *  -------------------------->用户访问时的状态信息
  * @param date  产生行为的日期
  * @param user_id  用户的ID
  * @param session_id  session的ID
  * @param page_id  某个页面的ID
  * @param action_time 点击行为得时间
  * @param search_keyword  用户搜索的关键字
  * @param click_category_id  某个商品类别信息
  * @param click_product_id  某个商品的ID
  * @param order_category_ids  一次未支付的订单中包含商品种类ID的集合
  * @param order_product_ids   一次未支付的订单中所有商品的ID集合
  * @param pay_category_ids  一次支付过订单中所属商品种类的ID集合
  * @param pay_product_ids   一次支付过订单中所属商品的ID的集合
  * @param city_id  此次记录发生的城市
  */
case class UserVisitInfo(date: String,
                         user_id: Long,
                         session_id: String,
                         page_id: Long,
                         action_time: String,
                         search_keyword: String,
                         click_category_id: Long,
                         click_product_id: Long,
                         order_category_ids: String,
                         order_product_ids: String,
                         pay_category_ids: String,
                         pay_product_ids: String,
                         city_id:Long
                        )
