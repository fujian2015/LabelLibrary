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
  * {"pname": "归属国家ID"   ,"pvalue": "user_state_id"},
  * {"pname": "归属省ID"	 ,"pvalue": "user_province_id"},
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
    val imsi_val = line.getOrElse(ds_callingImsi,"")
    val msisdn_val = getMsisdn(line.getOrElse(ds_phoneNum,""))//4g信令中含号码，2g3g不包含

    var fieldMap = this.fieldsMap() //定义默认的空map，表示要打的标签字段及值
    val user_location_colList=conf.get("user_location_cols","user_state_id,user_province_id,user_city_id").split(",")//用户归属地字段
    val info_cols = fieldMap.keys.++(user_location_colList)
    //println(info_cols)
    user_location_colList.foreach(colName=> {
      if(!fieldMap.contains(colName)){
        fieldMap += (colName -> "")
      }
    })//初始化用户归属地字段

    //1-首先,根据“userinfo:$imsi”，查询codis中省内用户表，增强省内用户信息
    val userinfo_map = labelQryData.getOrElse(codis_key_user_prefix+imsi_val, Map[String, String]())
    info_cols.foreach(colName => {
      userinfo_map.get(colName) match {
        case Some(value) =>
          fieldMap += (colName -> value)
        case None =>
      }
    })
    //判断用户归属地信息是否完整
    var userLocationFlag=true
    user_location_colList.foreach(colName=>{
      if(fieldMap.getOrElse(colName,"")=="")userLocationFlag=false
    })
    //    println(userLocationFlag)

    //手机号码
    if(info_cols.exists(col=>col=="product_no") && fieldMap.getOrElse("product_no","")=="" ){
      fieldMap.update("product_no",msisdn_val)
    }
    val phone_val=if(msisdn_val=="") fieldMap.getOrElse("product_no","") else msisdn_val

    //2.如果用户归属地信息不完整，则根据号码前7位“phoneNum:XXXXXXX”，关联全网号段表，增强省外用户的国家省市信息。
    if(!userLocationFlag) {
      if(""!=phone_val) {
        val phoneSeg=if(phone_val.length<7) phone_val else phone_val.substring(0, 7)//手机号段
        val phonenum_map = labelQryData.getOrElse(codis_key_phone_prefix + phoneSeg, Map[String, String]())
        info_cols.foreach(colName => {
          phonenum_map.get(colName) match {
            case Some(value) =>
              fieldMap += (colName -> value)
            case None =>
          }
        })
      }

      //判断用户归属地信息是否完整
      userLocationFlag=true
      user_location_colList.foreach(colName=>{
        if(fieldMap.getOrElse(colName,"")=="")userLocationFlag=false
      })
    }
    //    println(userLocationFlag)

    //3.如果用户归属地信息不完整，则根据imsi前5位“intlgsm:XXXXX”，增强国外用户的国家省市信息。
    if(!userLocationFlag){
      val imsiPrefix=if(imsi_val.length<5) imsi_val else imsi_val.substring(0,5)
      val intlgsm_map = labelQryData.getOrElse(codis_key_intlgsm_prefix+imsiPrefix, Map[String, String]())
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
    val keys=mutable.Set[String]()
    val imsi_val= line.getOrElse(ds_callingImsi,"")
    //println(imsi_val)
    if(imsi_val== null||""==imsi_val || imsi_val == "000000000000000"||imsi_val == "ffffffffffffffff"){
    }else{
      keys.add(codis_key_user_prefix + imsi_val)
      keys.add(codis_key_intlgsm_prefix + (if(imsi_val.length<5) imsi_val else imsi_val.substring(0,5) ) )
    }
    //4g原始数据中包含用户号码，可直接根据号码关联到号段表，查询国家、省、市信息
    val msisdn_str= getMsisdn(line.getOrElse(ds_phoneNum,""))
    if(msisdn_str== null||msisdn_str==""){
    }else{
      keys.add(codis_key_phone_prefix + (if(msisdn_str.length<7) msisdn_str else msisdn_str.substring(0,7)) )
    }
    keys.toSet
    /*
        Set[String](line(callingImsiFileName),line(phoneFiledName)).
          filterNot(value => {
            value == null||"".eq(value) || value == "000000000000000"||value == "ffffffffffffffff"
          }).map(codis_key_prefix + _)
    */
  }

  /**对原始信令中的手机号码进行预处理,统一输出11位
    * msisdn格式有：
    *   861XXxxxxXXX, 1259012590XXXxxxxXXXX, 12590XXXxxxxXXXX,
    * */
  def getMsisdn(msisdn:String):String={
    if(msisdn==null||""==msisdn){
      ""
    }else if(msisdn.length>11){
      msisdn.substring(msisdn.length-11)
    }else{
      msisdn
    }
  }

}
