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
  //城市,省标签前缀
  val city_sine = "city_name"

  override def attachLabel(line: Map[String, String], cache: StreamingCache, labelQryData: mutable.Map[String, mutable.Map[String, String]]): (Map[String, String], StreamingCache) = {

    val labelMap = fieldsMap()

    val info_cols = getLabelConf.getFields()
    val defaultName = conf.get("default_province_name", "YunNan")

    val cachedUser = labelQryData.getOrElse(s"user_base_info:${line("imsi")}", Map[String, String]())

        try {
          if (cachedUser.isEmpty) {
            //如果查询不到user imsi, 则查询city_info信息,得到imsi码段的归属城市
            var cachedCity: mutable.Map[String, String] = null

            cachedCity = if (line("msisdn").length >= 9){
              labelQryData.getOrElse(s"city_info:${line("msisdn").substring(2, 9)}", mutable.Map[String, String]())
            }else{
              mutable.Map[String,String]()
            }

            if (cachedCity.contains(city_sine)) {
              val city = cachedCity(city_sine)
              labelMap += (LabelConstant.LABEL_CITY -> city)
            }
            else {
              labelMap += (LabelConstant.LABEL_CITY -> "")
            }
          }
          else {
            info_cols.foreach(labelName => {
              val labelValue = cachedUser.getOrElse(labelName, "")
              if (!labelValue.isEmpty) {
                labelMap += (labelName -> labelValue)
              }
            })
            //如果能查到user imsi,则将归属地设为陕西省
            labelMap += (LabelConstant.LABEL_CITY -> defaultName)
          }
        } catch {
          case ex: Exception => {
            logger.error("Annoying error:",ex)
          }
        }


    labelMap ++= line

    (labelMap.toMap, cache)

  }

  override def getQryKeys(line: Map[String, String]): Set[String] = {
    if (line("msisdn").length < 9){
      Set[String](s"user_base_info:${line("imsi")}", s"city_info:${line("msisdn")}")
    }else{
      Set[String](s"user_base_info:${line("imsi")}", s"city_info:${line("msisdn").substring(2, 9)}")
    }
  }

}
