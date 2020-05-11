package com.zx.member.service

import com.alibaba.fastjson.JSONObject
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SaveMode, SparkSession}
import com.zx.util.ParseJsonData

object ETLMemberService {
  def etlMemberlog(sc: SparkContext, session: SparkSession) = {
    import session.implicits._ //隐式转换
    val memberLogRDD: RDD[String] = sc.textFile("/user/zx/ods/member.log")
    memberLogRDD.filter(item=>{
      val obj = ParseJsonData.getJsonData(item)
      obj.isInstanceOf[JSONObject]
    })
      .mapPartitions(iter=>{
        iter.map(item=>{
          val jsonObject = ParseJsonData.getJsonData(item)
          val ad_id = jsonObject.getIntValue("ad_id")
          val birthday = jsonObject.getString("birthday")
          val email = jsonObject.getString("email")
          val fullname = jsonObject.getString("fullname").substring(0, 1) + "xx"
          val iconurl = jsonObject.getString("iconurl")
          val lastlogin = jsonObject.getString("lastlogin")
          val mailaddr = jsonObject.getString("mailaddr")
          val memberlevel = jsonObject.getString("memberlevel")
          val password = "******"
          val paymoney = jsonObject.getString("paymoney")
          val phone = jsonObject.getString("phone")
          val newphone = phone.substring(0, 3) + "*****" + phone.substring(7, 11)
          val qq = jsonObject.getString("qq")
          val register = jsonObject.getString("register")
          val regupdatetime = jsonObject.getString("regupdatetime")
          val uid = jsonObject.getIntValue("uid")
          val unitname = jsonObject.getString("unitname")
          val userip = jsonObject.getString("userip")
          val zipcode = jsonObject.getString("zipcode")
          val dt = jsonObject.getString("dt")
          val dn = jsonObject.getString("dn")
          (uid, ad_id, birthday, email, fullname, iconurl, lastlogin, mailaddr, memberlevel, password, paymoney, newphone, qq,
            register, regupdatetime, unitname, userip, zipcode, dt, dn)
        })
      })
      .toDF().coalesce(1).write.mode(SaveMode.Append)
      .insertInto("dwd.dwd_member")
  }

  def etlPcentermemviplevellog(sc: SparkContext, session: SparkSession) = {
    import session.implicits._
    val baseAdLogRDD: RDD[String] = sc.textFile("/user/zx/ods/pcenterMemViplevel.log")
    baseAdLogRDD.filter(item=>{
      val jSONObject: JSONObject = ParseJsonData.getJsonData(item)
      jSONObject.isInstanceOf[JSONObject]
    })
      .mapPartitions(iter=>{
        iter.map(item=>{
          val jSONObject = ParseJsonData.getJsonData(item)
          val discountval = jSONObject.getString("discountval")
          val end_time = jSONObject.getString("end_time")
          val last_modify_time = jSONObject.getString("last_modify_time")
          val max_free = jSONObject.getString("max_free")
          val min_free = jSONObject.getString("min_free")
          val next_level = jSONObject.getString("next_level")
          val operator = jSONObject.getString("operator")
          val start_time = jSONObject.getString("start_time")
          val vip_id = jSONObject.getIntValue("vip_id")
          val vip_level = jSONObject.getString("vip_level")
          val dn = jSONObject.getString("dn")
          (vip_id, vip_level, start_time, end_time, last_modify_time, max_free, min_free, next_level, operator, dn)
        })
      })
      .toDF().coalesce(1).write.mode(SaveMode.Overwrite)
      .insertInto("dwd.dwd_vip_level")

  }

  def etlPcentermempaymoneylog(sc: SparkContext, session: SparkSession) = {
    import session.implicits._ //隐式转换
    sc.textFile("/user/zx/ods/pcentermempaymoney.log").filter(item => {
      val obj = ParseJsonData.getJsonData(item)
      obj.isInstanceOf[JSONObject]
    }).mapPartitions(partition => {
      partition.map(item => {
        val jSONObject = ParseJsonData.getJsonData(item)
        val paymoney = jSONObject.getString("paymoney")
        val uid = jSONObject.getIntValue("uid")
        val vip_id = jSONObject.getIntValue("vip_id")
        val site_id = jSONObject.getIntValue("siteid")
        val dt = jSONObject.getString("dt")
        val dn = jSONObject.getString("dn")
        (uid, paymoney, site_id, vip_id, dt, dn)
      })
    }).toDF().coalesce(1).write.mode(SaveMode.Append).insertInto("dwd.dwd_pcentermempaymoney")
  }

  def etlMemberRegtype(sc: SparkContext, session: SparkSession) = {
    import session.implicits._ //隐式转换
    sc.textFile("/user/zx/ods/memberRegtype.log")
      .filter(item => {
        val obj = ParseJsonData.getJsonData(item)
        obj.isInstanceOf[JSONObject]
      }).mapPartitions(partitoin => {
      partitoin.map(item => {
        val jsonObject = ParseJsonData.getJsonData(item)
        val appkey = jsonObject.getString("appkey")
        val appregurl = jsonObject.getString("appregurl")
        val bdp_uuid = jsonObject.getString("bdp_uuid")
        val createtime = jsonObject.getString("createtime")
        val isranreg = jsonObject.getString("isranreg")
        val regsource = jsonObject.getString("regsource")
        val regsourceName = regsource match {
          case "1" => "PC"
          case "2" => "Mobile"
          case "3" => "App"
          case "4" => "WeChat"
          case _ => "other"
        }
        val uid = jsonObject.getIntValue("uid")
        val websiteid = jsonObject.getIntValue("websiteid")
        val dt = jsonObject.getString("dt")
        val dn = jsonObject.getString("dn")
        (uid, appkey, appregurl, bdp_uuid, createtime, isranreg, regsource, regsourceName, websiteid, dt, dn)
      })
    }).toDF().coalesce(2).write.mode(SaveMode.Append).insertInto("dwd.dwd_member_regtype")
  }

  def etlBaseWebSitelog(sc: SparkContext, session: SparkSession) = {
    import session.implicits._ //隐式转换
    sc.textFile("/user/zx/ods/baswewebsite.log").filter(item => {
      val obj = ParseJsonData.getJsonData(item)
      obj.isInstanceOf[JSONObject]
    }).mapPartitions(partition => {
      partition.map(item => {
        val jsonObject = ParseJsonData.getJsonData(item)
        val siteid = jsonObject.getIntValue("siteid")
        val sitename = jsonObject.getString("sitename")
        val siteurl = jsonObject.getString("siteurl")
        val delete = jsonObject.getIntValue("delete")
        val createtime = jsonObject.getString("createtime")
        val creator = jsonObject.getString("creator")
        val dn = jsonObject.getString("dn")
        (siteid, sitename, siteurl, delete, createtime, creator, dn)
      })
    }).toDF().coalesce(1).write.mode(SaveMode.Overwrite).insertInto("dwd.dwd_base_website")
  }

//  def etlBaseAdlog(sc: SparkContext, session: SparkSession): Unit = {
//    import session.implicits._
//    val baseAdLogRDD: RDD[String] = sc.textFile("/user/zx/ods/baseadlog.log")
//    baseAdLogRDD.filter(item=>{
//      val jSONObject: JSONObject = ParseJsonData.getJsonData(item)
//      jSONObject.isInstanceOf[JSONObject]
//    })
//      .mapPartitions(iter=>{
//      iter.map(item=>{
//        val jSONObject: JSONObject = ParseJsonData.getJsonData(item)
//        val adid: Int = jSONObject.getIntValue("adid")
//        val adname: String = jSONObject.getString("adname")
//        val dn: String = jSONObject.getString("dn")
//        (adid,adname,dn)
//      })
//    })
//      .toDF().coalesce(1).write.mode(SaveMode.Overwrite).insertInto("dwd.dwd_base_ad")
//  }
def etlBaseAdLog(ssc: SparkContext, sparkSession: SparkSession) = {
  import sparkSession.implicits._ //隐式转换
  val result = ssc.textFile("/user/zx/ods/baseadlog.log").filter(item => {
    val obj = ParseJsonData.getJsonData(item)
    obj.isInstanceOf[JSONObject]
  }).mapPartitions(partition => {
    partition.map(item => {
      val jsonObject = ParseJsonData.getJsonData(item)
      val adid = jsonObject.getIntValue("adid")
      val adname = jsonObject.getString("adname")
      val dn = jsonObject.getString("dn")
      (adid,adname, dn)
    })
  }).toDF().coalesce(1).write.mode(SaveMode.Overwrite).insertInto("dwd.dwd_base_ad")
}
}