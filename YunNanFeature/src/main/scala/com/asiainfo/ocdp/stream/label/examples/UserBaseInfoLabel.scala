package com.asiainfo.ocdp.stream.label.examples

import com.asiainfo.ocdp.stream.common.StreamingCache
import com.asiainfo.ocdp.stream.constant.LabelConstant
import com.asiainfo.ocdp.stream.label.Label
import org.slf4j.LoggerFactory

import scala.collection.mutable

/**
  * Created by gengwang on 16/8/11.
  */
class UserBaseInfoLabel extends Label {
  val logger = LoggerFactory.getLogger(this.getClass)
  //val city_sine = "city_name"城市,省标签前缀
  val lineField_imsi="imsi"//原始信令中的字段名称，需要现场配置一致
  val lineField_phone="msisdn"//原始信令中的字段名称，需要现场配置一致

  val codisKeyPrefix_user="user_base_info:"//codis中key前缀
  val codisKeyPrefix_phoneSeg="phone_seg_info:"//codis中key前缀

  //val labelProvinceField="seg_province_name"
  val labelProvinceFlag="user_province_flag"//省内、省外标志,true|false

  override def attachLabel(line: Map[String, String], cache: StreamingCache, labelQryData: mutable.Map[String, mutable.Map[String, String]]): (Map[String, String], StreamingCache) = {
    val imsi=line.getOrElse(lineField_imsi,"")
    var phone_num=line.getOrElse(lineField_phone,"")//2G、4G信令中包含号码，3G中不包含号码

    val labelMap = fieldsMap()
    //val info_cols = getLabelConf.getFields()
    //val defaultName = conf.get("default_province_name", "YunNan")
    val user_location_colList=conf.get("user_location_cols","user_province_name,user_city_name").split(",")//用户归属地字段
    val info_cols = labelMap.keys.++(user_location_colList)
    //println(info_cols)

    //初始化用户归属地字段
    user_location_colList.foreach(colName=> {
      if(!labelMap.contains(colName)){
        labelMap += (colName -> "")
      }
    })
    labelMap += (labelProvinceFlag->"false")

    //1-首先查询(省内)用户资料表
    val codisUserInfo = labelQryData.getOrElse(s"${codisKeyPrefix_user}${imsi}", Map[String, String]())
    if (!codisUserInfo.isEmpty) {
      labelMap += (labelProvinceFlag -> "true")

      info_cols.foreach(labelName => {
        codisUserInfo.get(labelName) match {
          case Some(value) =>
            labelMap += (labelName -> value)
          case None =>
        }
      })

      //3G信令中无号码，查询本省用户表，先根据imsi得到用户号码
      if(phone_num==""){
        if(codisUserInfo.contains(lineField_phone)){
            phone_num="86"+codisUserInfo.getOrElse(lineField_phone,"")
            labelMap.update(lineField_phone,phone_num)
          //TODO
            //设置默认省份为云南省,3G信令可能判断不到归属地
            //labelMap.update(labelProvinceField,defaultName)
          }
      }
    }
    //判断用户归属地信息是否完整
    var userLocationFlag=true
    user_location_colList.foreach(colName=>{
      if(labelMap.getOrElse(colName,"")=="")userLocationFlag=false
    })
    //2、如果用户归属地为空，则根据手机号段，增强（省外）用户归属地信息
    if(!userLocationFlag) {
      val phone_seg = if (phone_num.length < 9) phone_num else phone_num.substring(2, 9)
      val codisPhoneInfo = if (phone_seg.length > 0) {
        labelQryData.getOrElse(s"${codisKeyPrefix_phoneSeg}${phone_seg}", mutable.Map[String, String]())
      } else {
        mutable.Map[String, String]()
      }
      info_cols.foreach(labelName => {
        codisPhoneInfo.get(labelName) match {
          case Some(value) =>
            labelMap += (labelName -> value)
          case None =>
        }
      })
    }
//    val cachedUser = labelQryData.getOrElse(s"user_base_info:${line("imsi")}", Map[String, String]())
//       try {
//          if (cachedUser.isEmpty) {
//            //如果查询不到user imsi, 则查询city_info信息,得到imsi码段的归属城市
//            var cachedCity: mutable.Map[String, String] = null
//
//            cachedCity = if (line(lineField_phone).length >= 9){
//              labelQryData.getOrElse(s"city_info:${line(lineField_phone).substring(2, 9)}", mutable.Map[String, String]())
//            }else{
//              mutable.Map[String,String]()
//            }
//
//            if (cachedCity.contains(city_sine)) {
//              val city = cachedCity(city_sine)
//              labelMap += (LabelConstant.LABEL_CITY -> city)
//            }
//            else {
//              labelMap += (LabelConstant.LABEL_CITY -> "")
//            }
//          }
//          else {
//            info_cols.foreach(labelName => {
//              val labelValue = cachedUser.getOrElse(labelName, "")
//              if (!labelValue.isEmpty) {
//                labelMap += (labelName -> labelValue)
//              }
//            })
//            //如果能查到user imsi,则将归属地设为陕西省
//            labelMap += (LabelConstant.LABEL_CITY -> defaultName)
//          }
//        } catch {
//          case ex: Exception => {
//            logger.error("Annoying error:",ex)
//          }
//        }

    labelMap ++= line
    (labelMap.toMap, cache)

  }

  override def getQryKeys(line: Map[String, String]): Set[String] = {
    val phone_num=line.getOrElse(lineField_phone,"")
    val imsi=line.getOrElse(lineField_imsi,"")

    if (phone_num.length < 9){
      Set[String](s"${codisKeyPrefix_user}${imsi}", s"${codisKeyPrefix_phoneSeg}${phone_num}")
    }else{
      Set[String](s"${codisKeyPrefix_user}${imsi}", s"${codisKeyPrefix_phoneSeg}${phone_num.substring(2, 9)}")
    }
  }

}
