package com.zx.qz.controller

import com.zx.qz.service.AdsQzService
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import com.zx.util.HiveUtil

object AdsQzController {
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME","root")
    val conf: SparkConf = new SparkConf().setAppName("adsQz")
    val sparkSession: SparkSession = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate()
    val sc: SparkContext = sparkSession.sparkContext
    sc.hadoopConfiguration.set("fs.defaultFS","hdfs://mycluster")
    sc.hadoopConfiguration.set("dfs.nameservices","mycluster")
    HiveUtil.openDynamicPartition(sparkSession)
    HiveUtil.openCompression(sparkSession)

    val dt = "20190722"
    AdsQzService.getTarget(sparkSession,dt)
    AdsQzService.getTargetAPI(sparkSession,dt)

  }
}
