package com.zx.streaming

import java.lang
import java.sql.{Connection, ResultSet}
//import java.sql.Connection

import com.zx.util.{DataSourceUtil, QueryCallback, SqlProxy}
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, HasOffsetRanges, KafkaUtils, LocationStrategies, OffsetRange}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Minutes, Seconds, StreamingContext}

import scala.collection.mutable

object RegisterStreaming {
  private val groupid = "register_group_test"
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "root")
    val conf: SparkConf = new SparkConf()
      .setAppName(this.getClass.getSimpleName)
      .setMaster("local[*]")
    val ssc = new StreamingContext(conf, Seconds(3))

    val sparkContext: SparkContext = ssc.sparkContext
    sparkContext.hadoopConfiguration.set("fs.defaultFS", "hdfs://mycluster")
    sparkContext.hadoopConfiguration.set("dfs.nameservices", "mycluster")
    val topics = Array("register_topic")
    val kafkaMap: Map[String, Object] = Map[String, Object](
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> "aliyun1:9092,aliyun2:9092,aliyun3:9092",
      ConsumerConfig.GROUP_ID_CONFIG -> groupid,
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer],
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer],
      ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> "earliest",
      ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG -> (false: lang.Boolean)
    )
    ssc.checkpoint("hdfs://mycluster/sparkstreaming/checkpoint")
    //查询mysql中是否有偏移量
    val sqlProxy = new SqlProxy()
    val offsetMap = new mutable.HashMap[TopicPartition, Long]()
    val client: Connection = DataSourceUtil.getConnection
    try {
      sqlProxy.executeQuery(client, "select * from `offset_manager` where groupid=?",
        Array(groupid),
        new QueryCallback {
          override def process(rs: ResultSet): Unit = {
            while (rs.next()) {
              val model = new TopicPartition(rs.getString(2), rs.getInt(3))
              val offset = rs.getLong(4)
              offsetMap.put(model, offset)
            }
            rs.close()
          }
        })
    } catch {
      case e: Exception => e.printStackTrace()
    } finally {
      sqlProxy.shutdown(client)
    }

    //设置Kafka消费数据的参数 判断本地是否有偏移量，有则根据偏移量继续消费，无则重新消费
    val dStream: InputDStream[ConsumerRecord[String, String]] = if (offsetMap.isEmpty) {
      KafkaUtils.createDirectStream(ssc, LocationStrategies.PreferConsistent,
        ConsumerStrategies.Subscribe[String, String](topics, kafkaMap))
    } else {
      KafkaUtils.createDirectStream(ssc, LocationStrategies.PreferConsistent,
        ConsumerStrategies.Subscribe[String, String](topics, kafkaMap, offsetMap))
    }
    val resultDStream: DStream[(String, Int)] = dStream.filter(item => {
      item.value().split("\t").length == 3
    }).mapPartitions(partition => {
      partition.map(item => {
        val line: String = item.value()
        val arr: Array[String] = line.split("\t")
        val app_name: String = arr(1) match {
          case "1" => "pc"
          case "2" => "APP"
          case "3" => "other"
        }
        (app_name, 1)
      })
    })
    resultDStream.cache()
    resultDStream.reduceByKeyAndWindow((x: Int, y: Int) => x + y, Minutes(1), Seconds(6)).print()
    val updateFunc = (values: Seq[Int], state: Option[Int]) => {
      val currentCount: Int = values.sum
      val previousCount: Int = state.getOrElse(0)
      Some(currentCount + previousCount)
    }
    resultDStream.updateStateByKey(updateFunc).print()

    //处理完业务逻辑后，手动提交offset维护到本地mysql中
    dStream.foreachRDD(rdd => {
      val sqlProxy = new SqlProxy()
      val client: Connection = DataSourceUtil.getConnection
      try {
        val offsetRanges: Array[OffsetRange] = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        for (offran <- offsetRanges) {
          sqlProxy.executeUpdate(client, "replace into `offset_manager` (groupid,topic,`partition`,untilOffset) values(?,?,?,?)",
            Array(groupid, offran.topic, offran.partition.toString, offran.untilOffset))
        }
      } catch {
        case e: Exception => e.printStackTrace()
      } finally {
        sqlProxy.shutdown(client)
      }
    })
    //开启任务
    ssc.start()
    ssc.awaitTermination()
  }

}
