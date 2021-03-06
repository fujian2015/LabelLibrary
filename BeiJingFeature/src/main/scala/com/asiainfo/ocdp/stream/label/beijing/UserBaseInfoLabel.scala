package com.asiainfo.ocdp.stream.label.beijing

import java.text.SimpleDateFormat

import com.asiainfo.ocdp.stream.common.StreamingCache
import com.asiainfo.ocdp.stream.label.Label
import org.slf4j.LoggerFactory
import scala.collection.mutable
import com.aisainfo.ocdp.stream.crypto.{AesCipher, Md5Crypto}

/**
  * 用户标签
  * Created by gengwang on 16/8/11.
  */
class UserBaseInfoLabel extends Label {
  val logger = LoggerFactory.getLogger(this.getClass)

  //imsi主键字段
  val imsiFieldName = "imsi"
  val imeiFieldName = "imei"
  val msisdnFieldName = "msisdn"
  //标签属性设置的字段
  val label_props_pname = "user_info_cols"
  //codis中用户信息查询key的前缀
  val codis_key_prefix = "userinfo:"

  // 北京移动特殊处理字段
  // 对imsi进行MD5加密
  val imsi_md5 = "imsi_md5"
  // 对imsi进行aes加密
  val imsi_aes = "imsi_aes"
  // 对imei前8位+第8位后的内容进行aes加密
  val imei_aes = "imei_aes"
  //截取imei前8位，imei长度>8
  val imei_substr_eight = "imei_substr8"
  //截取imsi前三位
  val imsi_substr_three = "imsi_substr3"
  //mme信令截取前9位
  val msisdn_substr_nine = "msisdn_substr9"
  // 漫游类型
  val roaming_type = "roaming_type"
  // 手机号截取前7位
  val phone_substr_seven = "phone_substr7"
  // 手机号字段，需与codis和json里面配置的一样
  val phoneFieldName = "phone_no"

  //mme信令日期格式转换：yyyyMMddHHmmss->yyyyMMdd HH:mm:ss:SSS
  val datetime_format = "datetime_format"
  //mme信令日期字段
  val datetimeFieldName = "datetime"
  val datetimeFieldName_Endtime = "procedure_end_time"
  val dateformat_yyyyMMddHHmmss = "yyyyMMddHHmmss"
  val dateformat_yyyyMMddHHmmssSSS = "yyyyMMdd HH:mm:ss:SSS"

  // 是否为省内用户1为北京市用户
  val is_local = "islocal"

  override def attachLabel(line: Map[String, String], cache: StreamingCache, labelQryData: mutable.Map[String, mutable.Map[String, String]]): (Map[String, String], StreamingCache) = {

    val labelMap = fieldsMap()
    //用户标签字段
    val info_cols = conf.get(label_props_pname).split(",")

    // 以下字段不需在json里面配置,默认为空
    labelMap += (imsi_md5 -> "")
    labelMap += (imsi_aes -> "")
    labelMap += (imei_aes -> "")
    labelMap += (imei_substr_eight -> "")
    labelMap += (imsi_substr_three -> "")
    labelMap += (msisdn_substr_nine -> "")
    labelMap += (datetime_format -> "")
    labelMap += (roaming_type -> "")
    labelMap += (phone_substr_seven -> "")
    labelMap += (is_local -> "")
    labelMap += ("starttime_ms" -> "")
    labelMap += ("endtime_ms" -> "")

    //从codis获取缓存的用户信息
    val qryKey = getQryKeys(line)
    val imsi = line(imsiFieldName)
    val imsi_pre_ten = imsi.substring(0, 10) //imsi前10位

    // println("1.qryKey:" + qryKey)
    if (qryKey.size == 2) {
      val cachedUser = labelQryData.getOrElse(qryKey.head, Map[String, String]())
      //println("2.cachedUser:" + cachedUser)
      if (cachedUser.isEmpty) {
        //如果查询不到imsi,代表省外用户
        //println("2.cachedUser is empty!")
        labelMap.update(is_local, "0")
        val cachedUser_out = labelQryData.getOrElse(codis_key_prefix + imsi_pre_ten, Map[String, String]())
        info_cols.foreach(labelName => {
          val labelValue = cachedUser_out.getOrElse(labelName, "")
          if (!labelValue.isEmpty) {
            labelMap += (labelName -> labelValue)
            // 截取手机号前7位
            if (labelName.equals(phoneFieldName)) {
              labelMap.update(phone_substr_seven, labelValue.substring(0, 7))
            }

          }
        })


      } else {
        //println("3.info_cols:" + info_cols)
        labelMap.update(is_local, "1") //表示北京市用户
        info_cols.foreach(labelName => {
          val labelValue = cachedUser.getOrElse(labelName, "")
          if (!labelValue.isEmpty) {
            labelMap += (labelName -> labelValue)

            // 截取手机号前7位
            // println("-------labelName:" + labelName + ",labelValue:" + labelValue)
            if (labelName.equals(phoneFieldName)) {
              labelMap.update(phone_substr_seven, labelValue.substring(0, 7))
              // println("-------phone_substr_seven:" + labelValue.substring(0, 7))
            }

          }
        })
      }

    }

    //追加imsi特殊处理字段
    //println("4.imsi:" + imsi)
    if (imsi != null && imsi != "") {

      if (imsi.length >= 3) {
        val imsi_pre = imsi.substring(0, 3)
        labelMap.update(imsi_substr_three, imsi_pre)
        // 判断是否为国际漫游
        if (imsi_pre != "460") {
          labelMap.update(roaming_type, "1")
        }
      }

      if (imsi.length == 15) {
        labelMap.update(imsi_md5, Md5Crypto.encrypt(imsi))
        labelMap.update(imsi_aes, AesCipher.encrypt(imsi))
      }

    }

    //追加imei特殊处理字段
    val imei = line(imeiFieldName)
    //println("5.imei:" + imei)
    if (imei != null && imei.length >= 8) {
      labelMap.update(imei_aes, (imei.substring(0, 8) + AesCipher.encrypt(imei.substring(8))))
      labelMap.update(imei_substr_eight, (imei.substring(0, 8)))
    }

    //4G mme信令处理
    val msisdn = line.getOrElse(msisdnFieldName, "")
    val datetime = line.getOrElse(datetimeFieldName, "")
    val endtime = line.getOrElse(datetimeFieldName_Endtime, "")

    //println("6.msisdn:" + msisdn + ",datetime:" + datetime)
    if (msisdn != "" && msisdn != null && msisdn.length >= 9) {
      labelMap.update(msisdn_substr_nine, (msisdn.substring(0, 9)))
    }

    if (datetime != "" && datetime != null && datetime.length < 21) {
      labelMap.update(datetime_format, transDateFormat(dateformat_yyyyMMddHHmmss, dateformat_yyyyMMddHHmmssSSS, datetime))

      val sdf = new SimpleDateFormat(dateformat_yyyyMMddHHmmss);
      val starttime_ms = sdf.parse(datetime).getTime().toString;
      //毫秒
      val endtime_ms = sdf.parse(endtime).getTime().toString; //毫秒
      labelMap.update("starttime_ms", starttime_ms)
      labelMap.update("endtime_ms", endtime_ms)
    } else {
      labelMap.update(datetime_format, datetime) //mc信令不用转换格式
    }

    labelMap ++= line
    (labelMap.toMap, cache)

  }

  /**
    * 日期格式转换
    *
    * @param fromFormat
    * @param toFormat
    * @param dateStr
    * @return
    */
  def transDateFormat(fromFormat: String, toFormat: String, dateStr: String): String = {
    val fromSdf: SimpleDateFormat = new SimpleDateFormat(fromFormat)
    var toDateStr: String = ""
    try {
      toDateStr = new SimpleDateFormat(toFormat).format(fromSdf.parse(dateStr))
    } catch {
      case ex: Exception =>
        logger.error(s"${dateStr} format do not match ${toFormat} ", ex)
    }

    toDateStr
  }

  override def getQryKeys(line: Map[String, String]): Set[String] =
  // 外省用户要用imsi前10位关联
    Set[String](line(imsiFieldName), line(imsiFieldName).substring(0, 10)).
      filterNot(value => {
        value == null || value == "000000000000000"
      }).map(codis_key_prefix + _)

}
