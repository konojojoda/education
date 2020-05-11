package com.zx.member.controller

import com.zx.member.service.ETLMemberService
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import com.zx.util.HiveUtil

object DwdMemberController {
  def main(args: Array[String]): Unit = {
//    System.setProperty("HADOOP_USER_NAME","root")
//    val conf: SparkConf = new SparkConf().setAppName("dwd_member_import").setMaster("local[*]")
//    val session: SparkSession = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate()
//    val sc: SparkContext = session.sparkContext
//    sc.hadoopConfiguration.set("fs.defaultFS","hdfs://mycluster")
//    sc.hadoopConfiguration.set("dfs.nameservices","mycluster")
//    HiveUtil.openDynamicPartition(session)
//    HiveUtil.openCompression(session)
    System.setProperty("HADOOP_USER_NAME", "root")
    val sparkConf = new SparkConf().setAppName("dwd_member_import")//.setMaster("local[*]")
    val session = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate()
    val sc = session.sparkContext
    sc.hadoopConfiguration.set("fs.defaultFS", "hdfs://mycluster")
    sc.hadoopConfiguration.set("dfs.nameservices", "mycluster")
    HiveUtil.openDynamicPartition(session) //开启动态分区
    HiveUtil.openCompression(session) //开启压缩
    ETLMemberService.etlBaseAdLog(sc,session)
    ETLMemberService.etlBaseWebSitelog(sc,session)
    ETLMemberService.etlMemberRegtype(sc,session)
    ETLMemberService.etlPcentermempaymoneylog(sc,session)
    ETLMemberService.etlPcentermemviplevellog(sc,session)
    ETLMemberService.etlMemberlog(sc,session)



    sc.stop()
    session.stop()
  }

}
