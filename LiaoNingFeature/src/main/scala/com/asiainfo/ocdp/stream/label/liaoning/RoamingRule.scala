package com.asiainfo.ocdp.stream.label.liaoning

import com.asiainfo.ocdp.stream.common.StreamingCache
import com.asiainfo.ocdp.stream.label.Label
import org.slf4j.LoggerFactory

import scala.collection.mutable

/**
  * 漫游标签，依赖区域标签、用户及国家标签
  * 根据用户的归属地信息与基站所在地信息对比，判断出用户的漫游类型
  * Created by yang on 2017/4/24.
  */
class RoamingRule extends Label {
  val logger = LoggerFactory.getLogger(this.getClass)

  val local_province_id="240" //本省ID
  val label_field_roaming="vst_id"//本标签增强的漫游类型字段名称

  val userLabel_field_state_id="user_state_id"//用户标签中增强的字段名称
  val userLabel_field_province_id="user_province_id"//用户标签中增强的字段名称
  val userLabel_field_city_id="user_city_id"//用户标签中增强的字段名称
  val siteLabel_field_city_id="bts_city_id"//区域标签中增强的字段名称

  override def attachLabel(line: Map[String, String], cache: StreamingCache, labelQryData: mutable.Map[String, mutable.Map[String, String]]): (Map[String, String], StreamingCache) = {
    var fieldMap = this.fieldsMap() //定义默认的空map，表示要打的标签字段及值
    //val info_cols = fieldMap.keys//conf.get(label_props_pname).split(",")

    val user_state_id=line.getOrElse(userLabel_field_state_id,"")//用户标签中增强的字段值
    val user_province_id=line.getOrElse(userLabel_field_province_id,"")//用户标签中增强的字段值
    val user_city_id=line.getOrElse(userLabel_field_city_id,"")//用户标签中增强的字段值
    val vst_city_id=line.getOrElse(siteLabel_field_city_id,"")//区域标签中增强的字段值
    println(user_state_id)

    var roamingType=""

    if("852"==user_state_id || "853"==user_state_id || "886"==user_state_id){
      roamingType="3"//港澳台漫游
    }else if("0000"!=user_state_id) {//国外!=0000
      roamingType="4" //国际漫游
    }else if("0000"==user_state_id){//国内=0000
      if(local_province_id==user_province_id){//本省
        if(vst_city_id==user_city_id){
          roamingType="0" //无漫游
        }else{
          roamingType="1" //省内漫游
        }
      }else if( "0000"!=user_province_id) {//省外
        roamingType="2" //省际漫游
      }
    }

    fieldMap.update(label_field_roaming,roamingType)
    fieldMap ++= line
    (fieldMap.toMap, cache)
  }

}
