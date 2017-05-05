package com.asiainfo.ocdp.stream.label

import com.asiainfo.ocdp.stream.common.StreamingCache
import org.slf4j.LoggerFactory

import scala.collection.mutable

/**
 * Created by yangyx on 4/29/15.
  * 只对一个用户进行打标签，如内容信令中的数据
 */
class UserInfoRule extends Label {

  val logger = LoggerFactory.getLogger(this.getClass)

  override def attachLabel(line: Map[String, String], cache: StreamingCache, labelQryData: mutable.Map[String, mutable.Map[String, String]]): (Map[String, String], StreamingCache) = {

    val normal_imsi = line("imsi")

    val info_cols = conf.get("user_info_cols").split(",")
    val qryKeys = getQryKeys(line)

    var fieldMap = fieldsMap()

    if (qryKeys.size == 0) {
      // do nothing
    } else{
      val qryKey = qryKeys.head
      val userKey = qryKey.split(":")(1)
      //val user_info_map = labelQryData.get(qryKey).get
      val user_info_map = labelQryData.getOrElse(qryKey, Map[String, String]())
      if (userKey == normal_imsi) {
        //常规业务的用户标签:由user_info_cols配置，逗号分隔
        info_cols.foreach(labelName => {
          user_info_map.get(labelName) match {
            case Some(value) =>
              fieldMap += (labelName -> value)
            case None =>
          }
        })
      } else {
        // do nothing
      }
    }
     
    //    line.foreach(fieldMap.+(_))
    fieldMap ++= line
    (fieldMap.toMap, cache)
  }

  override def getQryKeys(line: Map[String, String]): Set[String] =
    Set[String](line("imsi")).
      filterNot(value => {
      value == null || value == "000000000000000"
    }).map("userinfo:" + _)

}
