package com.Parquets

import com.LogsUtils.{SchemaUtils, StringUtil}
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

object Bz2Parquet {
  def main(args: Array[String]): Unit = {
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
    // 读取文件
    val lines = sc.textFile(inputPath)
    // 进行过滤，要保证字段大于八十五个，不然会数组越界
    // 颞部无法解析,,,,,,,，他会识别为一个元素，所以我们切割的时候，需要进行处理
    val rowRDD = lines.map(t=>t.split(",",-1)).filter(_.length >=85).map(arr=>{
      Row(
        arr(0),
        StringUtil.StringtoInt(arr(1)),
        StringUtil.StringtoInt(arr(2)),
        StringUtil.StringtoInt(arr(3)),
        StringUtil.StringtoInt(arr(4)),
        arr(5),
        arr(6),
        StringUtil.StringtoInt(arr(7)),
        StringUtil.StringtoInt(arr(8)),
        StringUtil.StringtoDouble(arr(9)),
        StringUtil.StringtoDouble(arr(10)),
        arr(11),
        arr(12),
        arr(13),
        arr(14),
        arr(15),
        arr(16),
        StringUtil.StringtoInt(arr(17)),
        arr(18),
        arr(19),
        StringUtil.StringtoInt(arr(20)),
        StringUtil.StringtoInt(arr(21)),
        arr(22),
        arr(23),
        arr(24),
        arr(25),
        StringUtil.StringtoInt(arr(26)),
        arr(27),
        StringUtil.StringtoInt(arr(28)),
        arr(29),
        StringUtil.StringtoInt(arr(30)),
        StringUtil.StringtoInt(arr(31)),
        StringUtil.StringtoInt(arr(32)),
        arr(33),
        StringUtil.StringtoInt(arr(34)),
        StringUtil.StringtoInt(arr(35)),
        StringUtil.StringtoInt(arr(36)),
        arr(37),
        StringUtil.StringtoInt(arr(38)),
        StringUtil.StringtoInt(arr(39)),
        StringUtil.StringtoDouble(arr(40)),
        StringUtil.StringtoDouble(arr(41)),
        StringUtil.StringtoInt(arr(42)),
        arr(43),
        StringUtil.StringtoDouble(arr(44)),
        StringUtil.StringtoDouble(arr(45)),
        arr(46),
        arr(47),
        arr(48),
        arr(49),
        arr(50),
        arr(51),
        arr(52),
        arr(53),
        arr(54),
        arr(55),
        arr(56),
        StringUtil.StringtoInt(arr(57)),
        StringUtil.StringtoDouble(arr(58)),
        StringUtil.StringtoInt(arr(59)),
        StringUtil.StringtoInt(arr(60)),
        arr(61),
        arr(62),
        arr(63),
        arr(64),
        arr(65),
        arr(66),
        arr(67),
        arr(68),
        arr(69),
        arr(70),
        arr(71),
        arr(72),
        StringUtil.StringtoInt(arr(73)),
        StringUtil.StringtoDouble(arr(74)),
        StringUtil.StringtoDouble(arr(75)),
        StringUtil.StringtoDouble(arr(76)),
        StringUtil.StringtoDouble(arr(77)),
        StringUtil.StringtoDouble(arr(78)),
        arr(79),
        arr(80),
        arr(81),
        arr(82),
        arr(83),
        StringUtil.StringtoInt(arr(84))
      )
    })
    // 构建DF
    val df = sQLContext.createDataFrame(rowRDD,SchemaUtils.logStructType)
    // 存储到指定位置,这边第一个为一级分区，第二个为二级分区
    df.write.partitionBy("provincename","cityname").parquet(outputPath)
    sc.stop()
  }
}
