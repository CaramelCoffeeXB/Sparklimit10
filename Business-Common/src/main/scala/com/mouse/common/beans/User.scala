package com.mouse.common.beans

/**
  * ---------->用户信息表
  * @param user_id 用户ID
  * @param username 账户
  * @param name 用户名
  * @param age  用户年龄
  * @param professional 用户职业
  * @param gender  性别
  */
case class User(user_id: Long, username: String, name: String, age: Int, professional: String, gender: String)
