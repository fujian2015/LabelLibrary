package com.asiainfo.ocdp.stream.label

import com.asiainfo.ocdp.stream.common.StreamingCache
import org.slf4j.LoggerFactory

import scala.collection.mutable

/**
 * Created by yangyx 2017-01-05
  * 手机号段
  * 使用UserBaseInfoRule增加用户资料信息时，是根据imsi来关联用户资料。
  * 如果MC信令中的imsi不规则或者错误，则会无法关联到用户资料。
  * 在对省外用户进行营销活动时，可能会将省内（未关联用户资料）用户误认为是省外用户。
  * 为避免上述错误，故需要根据手机号段来判断是否省外用户。
 */
class UserPhoneSegmentRule extends Label {
  val logger = LoggerFactory.getLogger(this.getClass)

  override def attachLabel(lineDataMap: Map[String, String], lableCache: StreamingCache, allLabelQryData: mutable.Map[String, mutable.Map[String, String]]): (Map[String, String], StreamingCache) = {
    val mc_phone_no = lineDataMap("user_phone")//MC信令中的号码，该字段是在MC信令的输入topic中配置的
//    val mc_phone_segment=if(mc_phone_no!=null)"" else mc_phone_no.substring(0,7)//手机号段
    logger.debug("user_phone="+mc_phone_no)

    val codis_cols =conf.getFields //conf.get("user_info_cols").split(",")
    var fieldMap = fieldsMap()

    val qryKeys = getQryKeys(lineDataMap.toMap)//获取codis查询key
    if(!qryKeys.isEmpty){
      val qryKey = qryKeys.head
      val user_info_map = allLabelQryData.getOrElse(qryKey, Map[String, String]())

      //常规业务的用户标签:由user_info_cols配置，逗号分隔
      codis_cols.foreach(colName => {
        if (user_info_map.contains(colName)) {
          fieldMap.update(colName , user_info_map.getOrElse(colName,""))
        }
      })
    }
     fieldMap ++= lineDataMap
    //lineDataMap ++= fieldMap
      (fieldMap.toMap, lableCache)
  }

  //组装codis查询key ｛"phone_seg:$号段" :{segment:"",seg_city_id:"xx",seg_province_id:"xxxx"}｝
  override def getQryKeys(line: Map[String, String]): Set[String] =
    Set[String](line("user_phone")).
      filterNot(value => {
        value == null || value == "00000000000"||value.trim.length!=11
      }).map("phone_seg:" + _.substring(0,7))

}
