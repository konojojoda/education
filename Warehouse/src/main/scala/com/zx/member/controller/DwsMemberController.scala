package com.zx.member.controller

import com.zx.member.service.DwsMemberService
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import com.zx.util.HiveUtil

object DwsMemberController {
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "root")
    val conf: SparkConf = new SparkConf().setAppName("DwsMember")//.setMaster("local[*]")
    val session: SparkSession = SparkSession.builder().enableHiveSupport().config(conf).getOrCreate()
    val sc: SparkContext = session.sparkContext
    sc.hadoopConfiguration.set("fs.defaultFS", "hdfs://mycluster")
    sc.hadoopConfiguration.set("dfs.nameservices", "mycluster")
    HiveUtil.openDynamicPartition(session) //开启动态分区
    HiveUtil.openCompression(session) //开启压缩
    DwsMemberService.importMember(session, "20190722") //根据用户信息聚合用户表数据
    DwsMemberService.importMemberUseApi(session, "20190722")
  }
}
