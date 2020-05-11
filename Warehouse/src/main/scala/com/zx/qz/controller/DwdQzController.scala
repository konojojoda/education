package com.zx.qz.controller

import com.zx.qz.service.DwdQzService
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import com.zx.util.HiveUtil

object DwdQzController {
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "root")
    val conf: SparkConf = new SparkConf().setAppName("DwdQz")//.setMaster("local[*]")
    val sparkSession: SparkSession = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate()
    val sc: SparkContext = sparkSession.sparkContext
    HiveUtil.openCompression(sparkSession)
    HiveUtil.openDynamicPartition(sparkSession)
    sc.hadoopConfiguration.set("fs.defaultFS", "hdfs://mycluster")
    sc.hadoopConfiguration.set("dfs.nameservices", "mycluster")
    DwdQzService.etlQzChapter(sc, sparkSession)
//    DwdQzService.etlQzChapterList(sc, sparkSession)
    DwdQzService.etlQzPoint(sc, sparkSession)
    DwdQzService.etlQzPointQuestion(sc, sparkSession)
    DwdQzService.etlQzSiteCourse(sc, sparkSession)
    DwdQzService.etlQzCourse(sc, sparkSession)
    DwdQzService.etlQzCourseEdusubject(sc, sparkSession)
    DwdQzService.etlQzWebsite(sc, sparkSession)
    DwdQzService.etlQzMajor(sc, sparkSession)
    DwdQzService.etlQzBusiness(sc, sparkSession)
    DwdQzService.etlQzPaperView(sc, sparkSession)
    DwdQzService.etlQzCenterPaper(sc, sparkSession)
    DwdQzService.etlQzPaper(sc, sparkSession)
    DwdQzService.etlQzCenter(sc, sparkSession)
    DwdQzService.etlQzQuestion(sc, sparkSession)
    DwdQzService.etlQzQuestionType(sc, sparkSession)
    DwdQzService.etlQzMemberPaperQuestion(sc, sparkSession)
  }
}
