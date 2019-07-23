package com.APP

import com.APPCountUtils.AppUtils
import org.apache.commons.lang3.StringUtils
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext

/**
  * 媒体指标
  */
object AppCount {
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "D:\\Huohu\\下载\\hadoop-common-2.2.0-bin-master")
    // 首先判断一下目录参数是否为空
    if(args.length != 3){
      println("目录参数不正确，退出程序")
      sys.exit()
    }
    // 创建数组存储输入输出目录
    val Array(inputPath,outputPath,dirPath) = args
    // 创建Spark的执行入口
    val conf = new  SparkConf().setMaster("local[*]").setAppName(this.getClass.getName)
      // 设置序列化机制
      .set("spark.serializer","org.apache.spark.serializer.KryoSerializer")
    val sc = new SparkContext(conf)
    // 设置Spark sql 压缩方式，注意spark1.6版本默认不是snappy，到2.0以后是默认的压缩方式
    val sQLContext = new SQLContext(sc)
    sQLContext.setConf("spark.sql.parquet.compression.codec","snappy")
    // Spark Core实现
    val df = sQLContext.read.parquet(inputPath)
    // 读取字典文件
    val dirfile = sc.textFile(dirPath)
    val map = dirfile.map(_.split("\t",-1)).filter(_.length>=5).map(arr=>(arr(4),arr(1))).collect().toMap
    // 广播出去
    val broadcast = sc.broadcast(map)
    // 处理业务
    df.map(row=>{
      // 通过广播变量进行判断取值
      var appname = row.getAs[String]("appname")
      if(!StringUtils.isNotBlank(appname)){
        appname = broadcast.value.getOrElse(row.getAs[String]("appid"),"unknow")
      }
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
      (appname,reqList++adList++clickList)
    }).reduceByKey((list1,list2)=>{
      list1.zip(list2).map(t=>t._1+t._2)
    }).map(t=>t._1+","+t._2.mkString(","))
      // 如果你在程序中不对分区进行操作的话，那么他的分区数将取决于我们的HDFS的每个块的数量，
      // 当HDFS有很多小文件的时候，那么分区数大大增加，我们需要进行合并分区，如果后面不改变分区数
      // 分区数据量和初始的数量一致
        .coalesce(10).saveAsTextFile(outputPath)
  }
}