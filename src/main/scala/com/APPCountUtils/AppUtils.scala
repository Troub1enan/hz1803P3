package com.APPCountUtils

/**
  * 业务方法
  */
object AppUtils {
  // 处理原始、有效、广告请求方法
  def Request(requestmode:Int,processnode:Int):List[Double]={
    if(requestmode ==1 && processnode ==1){
      // 第一个元素 原始请求
      // 第二个元素 有效请求
      // 第三个元素 广告请求
      List[Double](1,0,0)
    }else if(requestmode ==1 && processnode ==2){
      List[Double](1,1,0)
    }else if (requestmode ==1 && processnode ==3){
      List[Double](1,1,1)
    }else{
      List[Double](0,0,0)
    }
  }
  // 处理参与竞价数 竞价成功数 广告成品 广告消费
  def appad(iseffective:Int,isbilling:Int,
            isbid:Int,iswin:Int,adorderid:Int,winprice:Double,adpayment:Double):List[Double]={
    if(iseffective ==1 && isbilling ==1 && isbid==1){
      if(iseffective ==1 && isbilling ==1 && isbid==1 && iswin ==1 && adorderid != 0){
        List[Double](1,1,winprice/1000.0,adpayment/1000.0)
      }else{
        List[Double](1,0,0,0)
      }
    }else{
      List[Double](0,0,0,0)
    }
  }
  // 处理点击量和展示量
  def ReCount(requestmode:Int,iseffective:Int):List[Double]={
    if(requestmode == 2 && iseffective ==1){
      List[Double](1,0)
    }else if(requestmode == 3 && iseffective ==1){
      List[Double](0,1)
    }else{
      List[Double](0,0)
    }
  }

}
