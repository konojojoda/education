package com.zx.qz.dao

import org.apache.spark.sql.SparkSession

object AdsQzDao {
  def getQuestionDetail(sparkSession: SparkSession, dt: String) = {
    sparkSession.sql(s"select t.*,cast(t.errcount/(t.errcount+t.rightcount) as decimal(4,2))as rate" +
      s" from((select questionid,count(*) errcount,dt,dn from dws.dws_user_paper_detail where dt='$dt' and user_question_answer='0' " +
      s"group by questionid,dt,dn) a join(select questionid,count(*) rightcount,dt,dn from dws.dws_user_paper_detail where dt='$dt' and user_question_answer='1' " +
      s"group by questionid,dt,dn) b on a.questionid=b.questionid and a.dn=b.dn)t order by errcount desc")
  }

  def getPaperPassDetail(sparkSession: SparkSession, dt: String) = {
    sparkSession.sql("select t.*,cast(t.passcount/(t.passcount+t.countdetail) as decimal(4,2)) as rate,dt,dn" +
      "   from(select a.paperviewid,a.paperviewname,a.countdetail,a.dt,a.dn,b.passcount from " +
      s"(select paperviewid,paperviewname,count(*) countdetail,dt,dn from dws.dws_user_paper_detail where dt='$dt' and score between 0 and 60 group by" +
      s" paperviewid,paperviewname,dt,dn) a join (select paperviewid,count(*) passcount,dn from  dws.dws_user_paper_detail  where dt='$dt' and score >60  " +
      "group by paperviewid,dn) b on a.paperviewid=b.paperviewid and a.dn=b.dn)t")
  }

  def getPaperScoreSegmentUser(sparkSession: SparkSession, dt: String) = {
    sparkSession.sql("select paperviewid,paperviewname,score_segment,concat_ws(',',collect_list(cast(userid as string))),dt,dn" +
      " from (select paperviewid,paperviewname,userid," +
      " case  when score >=0  and score <=20 then '0-20'" +
      "       when score >20 and score <=40 then '20-40' " +
      "       when score >40 and score <=60 then '40-60' " +
      "       when score >60 and score <=80 then '60-80' " +
      "       when score >80 and score <=100 then '80-100' end  as score_segment" +
      s",dt,dn from  dws.dws_user_paper_detail where dt='$dt') group by paperviewid,paperviewname,score_segment,dt,dn order by paperviewid,score_segment")
  }

  def getLow3UserDetail(sparkSession: SparkSession, dt: String) = {sparkSession.sql("select *from (select userid,paperviewname,chaptername,pointname,sitecoursename,coursename,majorname,shortname," +
    s"sitename,papername,score,dense_rank() over (partition by paperviewid order by score asc) as rk,dt,dn from dws.dws_user_paper_detail where dt='$dt') where rk<4")
  }

  def getTop3UserDetail(sparkSession: SparkSession, dt: String) = {
    sparkSession.sql("select *from (select userid,paperviewname,chaptername,pointname,sitecoursename,coursename,majorname,shortname," +
      "sitename,papername,score,dense_rank() over (partition by paperviewid order by score desc) as rk,dt,dn from dws.dws_user_paper_detail) " +
      "where rk<4")
  }

  def getTopScore(sparkSession: SparkSession, dt: String) = {
    sparkSession.sql("select paperviewid,paperviewname,cast(max(score) as decimal(4,1)),cast(min(score) as decimal(4,1)) " +
      s",dt,dn from dws.dws_user_paper_detail where dt=$dt group by paperviewid,paperviewname,dt,dn ")
  }

  def getAvgSPendTimeAndScore(sparkSession: SparkSession, dt: String) = {
    sparkSession.sql(s"select paperviewid,paperviewname,cast(avg(score) as decimal(4,1)) score,cast(avg(spendtime) as decimal(10,2))" +
      s" spendtime,dt,dn from dws.dws_user_paper_detail where dt='$dt' group by " +
      "paperviewid,paperviewname,dt,dn order by score desc,spendtime desc")
  }


}
