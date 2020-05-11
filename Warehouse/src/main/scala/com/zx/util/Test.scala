package com.zx.util

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

object Test {
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "root")
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("test")
    val session: SparkSession = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate()
    val sc: SparkContext = session.sparkContext
    val rdd: RDD[Int] = sc.makeRDD(List(1,2,3,4),3)
    rdd.foreach(println)
    println(rdd.getNumPartitions)
    //rdd.saveAsTextFile("output1")
    session.stop()
    sc.stop()
  }

}
