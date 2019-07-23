package com.APP

import com.APPCountUtils.AppUtils
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SQLContext}

/**
  * 地域指标
  */
object LocationCount {
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "D:\\Huohu\\下载\\hadoop-common-2.2.0-bin-master")
    // 首先判断一下目录参数是否为空
    if(args.length != 2){
      println("目录参数不正确，退出程序")
      sys.exit()
    }
    // 创建数组存储输入输出目录
    val Array(inputPath,outputPath) = args
    // 创建Spark的执行入口
    val conf = new  SparkConf().setMaster("local[*]").setAppName(this.getClass.getName)
      // 设置序列化机制
      .set("spark.serializer","org.apache.spark.serializer.KryoSerializer")
    val sc = new SparkContext(conf)
    // 设置Spark sql 压缩方式，注意spark1.6版本默认不是snappy，到2.0以后是默认的压缩方式
    val sQLContext = new SQLContext(sc)
    sQLContext.setConf("spark.sql.parquet.compression.codec","snappy")
    // Spark Core实现
    val df: DataFrame = sQLContext.read.parquet(inputPath)
    df.map(row=>{
      // 取值Key
      val pro = row.getAs[String]("provincename")
      val city = row.getAs[String]("cityname")
      // 取值Value
      // 先去处理原始、有效、广告请求
      val requestmode = row.getAs[Int]("requestmode")
      val processnode = row.getAs[Int]("processnode")
      // 参与竞价数 竞价成功数 广告成品 广告消费
      val iseffective = row.getAs[Int]("iseffective")
      val isbilling = row.getAs[Int]("isbilling")
      val isbid = row.getAs[Int]("isbid")
      val iswin = row.getAs[Int]("iswin")
      val adorderid = row.getAs[Int]("adorderid")
      val winprice = row.getAs[Double]("winprice")
      val adpayment = row.getAs[Double]("adpayment")
      // 调用业务方法
      val reqList = AppUtils.Request(requestmode,processnode)
      val adList = AppUtils.appad(iseffective,isbilling,isbid,iswin,adorderid,winprice,adpayment)
      val clickList = AppUtils.ReCount(requestmode,iseffective)
      ((pro,city),reqList++adList++clickList)
    })
      // 进行聚合集合的数据
      .reduceByKey((list1,list2)=>{
      list1.zip(list2).map(t=>t._1+t._2)
    })
      // 简单对数据处理一下
      .map(t=>{t._1._1+","+t._1._2+","+t._2.mkString(",")})
       .foreach(println)
      //.saveAsTextFile(outputPath)
    sc.stop()
  }
}
