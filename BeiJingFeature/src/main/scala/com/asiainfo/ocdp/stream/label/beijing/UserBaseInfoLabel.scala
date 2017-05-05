package com.asiainfo.ocdp.stream.label.beijing

import java.text.SimpleDateFormat

import com.asiainfo.ocdp.stream.common.StreamingCache
import com.asiainfo.ocdp.stream.label.Label
import org.slf4j.LoggerFactory

import scala.collection.mutable
import com.asiainfo.dacp.crypto.{Crypto, CryptoContext}

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
  val imsi_md5 = "imsi_md5"
  // 对imsi进行MD5加密
  val imsi_aes = "imsi_aes"
  // 对imsi进行aes加密
  val imei_aes = "imei_aes"
  // 对imei前8位+后8位aes加密
  val imei_substr = "imei_substr"
  //截取imei前8位，imei长度>8
  val preimsi = "preimsi" //截取imsi前三位

  val msisdn_substr = "msisdn_substr"
  //mme信令截取前9位
  val datetime_format = "datetime_format"
  //mme信令日期格式转换：yyyyMMddHHmmss->yyyyMMdd HH:mm:ss:SSS
  val datetimeFieldName = "datetime"
  //mme信令日期字段
  val dateformat_yyyyMMddHHmmss = "yyyyMMddHHmmss"
  val dateformat_yyyyMMddHHmmssSSS = "yyyyMMdd HH:mm:ss:SSS"

  override def attachLabel(line: Map[String, String], cache: StreamingCache, labelQryData: mutable.Map[String, mutable.Map[String, String]]): (Map[String, String], StreamingCache) = {

    val labelMap = fieldsMap()
    //用户标签字段
    val info_cols = conf.get(label_props_pname).split(",")

    // add label with empty string if the fields does not exist
    info_cols.foreach(labelName => {
      labelMap += (labelName -> "")
    })

    // 以下字段不需在json里面配置,默认为空
    labelMap += (imsi_md5 -> "")
    labelMap += (imsi_aes -> "")
    labelMap += (imei_aes -> "")
    labelMap += (imei_substr -> "")
    labelMap += (preimsi -> "")
    labelMap += (msisdn_substr -> "")
    labelMap += (datetime_format -> "")

    //从codis获取缓存的用户信息
    val cachedUser = labelQryData.getOrElse(codis_key_prefix + line(imsiFieldName), Map[String, String]())
    if (cachedUser.isEmpty) {
      //如果查询不到imsi,特殊处理
      //labelMap += (LabelConstant.LABEL_CITY -> "")
    } else {
      info_cols.foreach(labelName => {
        val labelValue = cachedUser.getOrElse(labelName, "")
        if (!labelValue.isEmpty) {
          labelMap += (labelName -> labelValue)
        }
      })
    }

    val crypto_md5: Crypto = CryptoContext.getCrypto("md5")
    val crypto_aes: Crypto = CryptoContext.getCrypto("aes")

    //追加特殊处理字段
    if (line(imsiFieldName) != null && line(imsiFieldName).length == 15) {
      labelMap += (imsi_md5 -> crypto_md5.encrypt(line(imsiFieldName)))
      labelMap += (imsi_aes -> crypto_aes.encrypt(line(imsiFieldName)))
      labelMap += (preimsi -> (line(imsiFieldName).substring(0, 3)))
    }

    if (line(imeiFieldName) != null && line(imeiFieldName).length >= 8) {
      labelMap += (imei_aes -> (line(imeiFieldName).substring(0, 8) + crypto_aes.encrypt(line(imeiFieldName).substring(8))))
      labelMap += (imei_substr -> (line(imeiFieldName).substring(0, 8)))
    }

    //4G mme信令处理
    val msisdn = line.getOrElse(msisdnFieldName, "")
    val datetime = line.getOrElse(datetimeFieldName, "")
    if (msisdn != "" && msisdn != null) {
      labelMap += (msisdn_substr -> (msisdn.substring(0, 9)))
    }
    if (datetime != "" && datetime != null) {
      labelMap += (datetime_format -> transDateFormat(dateformat_yyyyMMddHHmmss, dateformat_yyyyMMddHHmmssSSS, datetime))
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
    val toDateStr: String = new SimpleDateFormat(toFormat).format(fromSdf.parse(dateStr))
    toDateStr
  }

  override def getQryKeys(line: Map[String, String]): Set[String] =
    Set[String](line(imsiFieldName)).
      filterNot(value => {
        value == null || value == "000000000000000"
      }).map(codis_key_prefix + _)

}
