package com.asiainfo.ocdp.stream.label.beijing

import java.text.SimpleDateFormat

import com.asiainfo.ocdp.stream.common.StreamingCache
import com.asiainfo.ocdp.stream.label.Label
import org.slf4j.LoggerFactory

import scala.collection.mutable

/**
  * 用户标签
  * Created by gengwang on 16/8/11.
  */
class UserBaseInfoLabel extends Label {
  val logger = LoggerFactory.getLogger(this.getClass)

  val imsiFiledName = "imsi"
  //优先对哪个imsi进行打标签，需与现场配置的数据源接口字段一致，可以是主叫imsi、被叫imsi，或其他自定义imsi字段，但必须是数据源接口字段。数据源接口中必须定义。
  val callingImsiFileName = "imsi"
  //数据源接口定义的主叫字段名称，需与配置现场一致。
  val calledImsiFileName = "calledimsi"
  //数据源接口定义的被叫字段名称，需与配置现场一致。
  val label_props_pname = "user_info_cols"
  //标签中定义的props的字段名称，需与配置现场一致。如："props":[{"pname":"user_info_cols","pvalue":"product_no,city_id"}]

  val codis_key_prefix = "userinfo:" //codis中用户信息查询key的前缀

  override def attachLabel(line: Map[String, String], cache: StreamingCache, labelQryData: mutable.Map[String, mutable.Map[String, String]]): (Map[String, String], StreamingCache) = {
    val imsi_val = line(imsiFiledName)

    val info_cols = conf.get(label_props_pname).split(",")
    val qryKeys = getQryKeys(line)

    var fieldMap: Map[String, String] = Map() //定义空map，表示要打的标签字段及值

    if (qryKeys.size == 0) {
      println("-----------qryKeys.size==0 , do nothing!")
      // do nothing
    } else if (qryKeys.size == 1) {
      //只对主叫进行信息增强
      //其中一个imsi无效
      val qryKey = qryKeys.head
      val userKey = qryKey.split(":")(1)
      println("-----------qryKey==" + qryKey + ",userKey==" + userKey)
      //val user_info_map = labelQryData.get(qryKey).get
      val user_info_map = labelQryData.getOrElse(qryKey, Map[String, String]())
      if (userKey == imsi_val) {
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
    fieldMap ++= line

    (fieldMap.toMap, cache)
  }

  override def getQryKeys(line: Map[String, String]): Set[String] =
    Set[String](line(callingImsiFileName)).
      filterNot(value => {
        value == null || value == "000000000000000"
      }).map(codis_key_prefix + _)

}

