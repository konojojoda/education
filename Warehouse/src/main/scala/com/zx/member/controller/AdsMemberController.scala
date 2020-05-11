package com.zx.member.controller

import com.zx.member.service.AdsMemberService
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import com.zx.util.HiveUtil

object AdsMemberController {
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "root")
    val sparkConf = new SparkConf().setAppName("ads_member_controller")
    val sparkSession = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate()
    val ssc = sparkSession.sparkContext
    ssc.hadoopConfiguration.set("fs.defaultFS", "hdfs://mycluster")
    ssc.hadoopConfiguration.set("dfs.nameservices", "mycluster")
    HiveUtil.openDynamicPartition(sparkSession) //开启动态分区
    AdsMemberService.queryDetailApi(sparkSession, "20190722")
    AdsMemberService.queryDetailSql(sparkSession, "20190722")
  }
}
