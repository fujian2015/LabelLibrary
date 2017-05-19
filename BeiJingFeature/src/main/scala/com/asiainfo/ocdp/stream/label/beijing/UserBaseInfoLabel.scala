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

  //mme信令日期格式转换：yyyyMMddHHmmss->yyyyMMdd HH:mm:ss:SSS
  val datetime_format = "datetime_format"
  //mme信令日期字段
  val datetimeFieldName = "datetime"
  val dateformat_yyyyMMddHHmmss = "yyyyMMddHHmmss"
  val dateformat_yyyyMMddHHmmssSSS = "yyyyMMdd HH:mm:ss:SSS"

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

    //从codis获取缓存的用户信息
    val qryKey = getQryKeys(line)

    // println("1.qryKey:" + qryKey)
    if (qryKey.size == 1) {
      val cachedUser = labelQryData.getOrElse(qryKey.head, Map[String, String]())
      //println("2.cachedUser:" + cachedUser)
      if (cachedUser.isEmpty) {
        //如果查询不到imsi,特殊处理
        //println("2.cachedUser is empty!")
      } else {
        //println("3.info_cols:" + info_cols)
        info_cols.foreach(labelName => {
          val labelValue = cachedUser.getOrElse(labelName, "")
          if (!labelValue.isEmpty) {
            labelMap += (labelName -> labelValue)
          }
        })
      }

    }

    //追加imsi特殊处理字段
    val imsi = line(imsiFieldName)
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
    //println("6.msisdn:" + msisdn + ",datetime:" + datetime)
    if (msisdn != "" && msisdn != null && msisdn.length >= 9) {
      labelMap.update(msisdn_substr_nine, (msisdn.substring(0, 9)))
      if (datetime != "" && datetime != null) {
        labelMap.update(datetime_format, transDateFormat(dateformat_yyyyMMddHHmmss, dateformat_yyyyMMddHHmmssSSS, datetime))
      }
    } else {
      labelMap.update(datetime_format, datetime)
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
    Set[String](line(imsiFieldName)).
      filterNot(value => {
        value == null || value == "000000000000000"
      }).map(codis_key_prefix + _)

}
