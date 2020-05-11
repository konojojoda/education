package com.zx.qz.controller

import com.zx.qz.service.DwsQzService
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import com.zx.util.HiveUtil

object DwsQzController {
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME","root")
    val conf: SparkConf = new SparkConf().setAppName("DwsQz")
    val sparkSession: SparkSession = SparkSession.builder().enableHiveSupport().config(conf).getOrCreate()
    val sc: SparkContext = sparkSession.sparkContext
    HiveUtil.openDynamicPartition(sparkSession)
    HiveUtil.openCompression(sparkSession)
    sc.hadoopConfiguration.set("fs.defaultFS","hdfs://mycluster")
    sc.hadoopConfiguration.set("dfs.nameservices","mycluster")
    val dt = "20190722"
    DwsQzService.saveDwsQzChapter(sparkSession, dt)
    DwsQzService.saveDwsQzCourse(sparkSession, dt)
    DwsQzService.saveDwsQzMajor(sparkSession, dt)
    DwsQzService.saveDwsQzPaper(sparkSession, dt)
    DwsQzService.saveDwsQzQuestionTpe(sparkSession, dt)
    DwsQzService.saveDwsUserPaperDetail(sparkSession, dt)

  }
}
