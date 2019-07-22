package com.LogsUtils

/**
  * 类型转换工具类
  */
object StringUtil {

  def StringtoInt(str:String):Int={
    try{
      str.toInt
    }catch {
      case _ : Exception => 0
    }
  }

  def StringtoDouble(str:String):Double={
    try{
      str.toDouble
    }catch {
      case  _ :Exception => 0.0
    }
  }
}
