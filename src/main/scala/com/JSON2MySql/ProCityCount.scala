package com.JSON2MySql

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 统计各省市的分布情况
  */
object ProCityCount {
  def main(args: Array[String]): Unit = {
    //判断参数目录是否为空
    if(args.length != 2) {
      println("参数目录不正确，退出程序")
      sys.exit()
    }
    //创建数组存储输入输出目录
    val Array(inputPath,outputPath) = args
    //创建Spark的执行入口
    val conf = new SparkConf().setAppName(this.getClass.getName).setMaster("local[*]")
    //设置序列化机制
      .set("spark.serializer","org.apache.spark.serializer.KryoSerializer")
    val sc = new SparkContext(conf)
    //设置SparkSql的压缩方式
    val sQLContext = new SQLContext(sc)
    sQLContext.setConf("spark.sql.parquet.compression.codec","snappy")
    //读取Parquet文件
    val df = sQLContext.read.parquet(inputPath)
    //注册临时表
    df.registerTempTable("log")
    //执行sql
    val result = sQLContext.sql("select provincename,cityname,count(*) from log group by provincename,cityname")
    //保存到本地
    result.write.json(outputPath)
//    //存入mysql
//    val load = ConfigFactory.load()//加载配置文件
//    val prop = new Properties()
//    //这边会自动读取配置文件中的内容
//    prop.setProperty("user",load.getString("jdbc.user"))
//    prop.setProperty("password",load.getString("jdbc.password"))
//    //存入数据库
//    result.write.mode(SaveMode.Append).jdbc(load.getString("jdbc.url"),load.getString("jdbc.TabName"),prop)
    sc.stop()
  }
}
