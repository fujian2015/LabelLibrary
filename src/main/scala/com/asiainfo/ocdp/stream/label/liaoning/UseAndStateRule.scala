package com.asiainfo.ocdp.stream.label.liaoning

import com.asiainfo.ocdp.stream.common.StreamingCache
import com.asiainfo.ocdp.stream.label.Label
import org.slf4j.LoggerFactory

import scala.collection.mutable

/**
  * 用户及归属地国家省市标签，查询codis中用户、号段表信息，并增加到数据源原始数据中
  * 首先根据用户的imsi，查询codis用户表，增加《省内》用户的手机号码、国家省市信息。
  * 然后根据4g信令中的号码，查询全国号段表，增加《省外》用户的国家省市信息。
  * 最后根据用户的imsi，查询全球imsi，增加其他用户的国家省市信息。
  * {"props": [], "labelItems": [
  * {"pname": "product_no"   ,"pvalue": "product_no"},
  * {"pname": "归属国家ID"   ,"pvalue": "state_id"},
  * {"pname": "归属省ID"	 ,"pvalue": "province_id"},
  * {"pname": "归属地市ID"   ,"pvalue": "user_city_id"},
  * {"pname": "county_id"    ,"pvalue": "county_id"},
  * {"pname": "userinfo_cust","pvalue": "userinfo_cust"},
  * {"pname": "漫游类型"	 ,"pvalue": "vst_id"}
  * ]}
  */
class UseAndStateRule extends Label {
  val logger = LoggerFactory.getLogger(this.getClass)

  val ds_callingImsi = "imsi" //数据源中的imsi字段名称
  val ds_phoneNum = "msisdn" //数据源中11位长度的手机号码

  val codis_key_user_prefix = "userinfo:"  //codis中用户信息查询key的前缀
  val codis_key_phone_prefix = "phoneNum:" //codis中全国号段查询key的前缀
  val codis_key_intlgsm_prefix = "intlgsm:" //codis中intlgsm imsi查询key的前缀

  override def attachLabel(line: Map[String, String], cache: StreamingCache, labelQryData: mutable.Map[String, mutable.Map[String, String]]): (Map[String, String], StreamingCache) = {
    val imsi_val = line(ds_callingImsi)
    var phone_val = line(ds_phoneNum)

    var fieldMap = this.fieldsMap() //定义默认的空map，表示要打的标签字段及值
    val info_cols = fieldMap.keys//conf.get(label_props_pname).split(",")

    //首先,根据“userinfo:$imsi”，查询codis中省内用户表，增强省内用户信息
    val userinfo_map = labelQryData.getOrElse(codis_key_user_prefix+imsi_val, Map[String, String]())
    info_cols.foreach(colName => {
      userinfo_map.get(colName) match {
        case Some(value) =>
          fieldMap += (colName -> value)
        case None =>
      }
    })
    //2.根据号码前7位“phoneNum:XXXXXXX”，关联全网号段表，增强省外用户的国家省市信息。
    if( "".eq(fieldMap.getOrElse("state_id","")) || "".eq(fieldMap.getOrElse("province_id","")) || "".eq(fieldMap.getOrElse("city_id","")) ) {
      if(phone_val==null||"".eq(phone_val)) phone_val=fieldMap.getOrElse("product_no","")
      if(!"".eq(phone_val)) {
        val phonenum_map = labelQryData.getOrElse(codis_key_phone_prefix + phone_val.substring(0, 7), Map[String, String]())
        info_cols.foreach(colName => {
          phonenum_map.get(colName) match {
            case Some(value) =>
              fieldMap += (colName -> value)
            case None =>
          }
        })
      }
    }
    //3.根据imsi前5位“intlgsm:XXXXX”，增强国外用户的国家省市信息。
    if("".eq(fieldMap.getOrElse("state_id","")) || "".eq(fieldMap.getOrElse("province_id","")) || "".eq(fieldMap.getOrElse("city_id","")) ){
      val intlgsm_map = labelQryData.getOrElse(codis_key_intlgsm_prefix+imsi_val.substring(0,5), Map[String, String]())
      info_cols.foreach(colName => {
        intlgsm_map.get(colName) match {
          case Some(value) =>
            fieldMap += (colName -> value)
          case None =>
        }
      })
    }

    fieldMap ++= line
    (fieldMap.toMap, cache)
  }

  override def getQryKeys(line: Map[String, String]): Set[String] ={
    val keys=Set()[String]
    val imsi_str= line(ds_callingImsi);
    if(imsi_str== null||"".eq(imsi_str) || imsi_str == "000000000000000"||imsi_str == "ffffffffffffffff"){
    }else{
      keys.+(codis_key_user_prefix + imsi_str)
      keys.+(codis_key_intlgsm_prefix + imsi_str.substring(0,5))
    }
    //4g原始数据中包含用户号码，可直接根据号码关联到号段表，查询国家、省、市信息
    val phone_str= getPhoneNo(line(ds_phoneNum));
    if(phone_str== null||phone_str.length != 11){
    }else{
      keys.+(codis_key_phone_prefix + phone_str.substring(0,7))
    }
    keys
/*
    Set[String](line(callingImsiFileName),line(phoneFiledName)).
      filterNot(value => {
        value == null||"".eq(value) || value == "000000000000000"||value == "ffffffffffffffff"
      }).map(codis_key_prefix + _)
*/
  }

  /**对原始信令中的手机号码进行预处理*/
  def getPhoneNo(phone_no:String):String={
    if(phone_no==null||"".eq(phone_no)){
      ""
    }else if(phone_no.length>11){
      phone_no.substring(phone_no.length-11)
    }else{
      phone_no
    }

  }

}
