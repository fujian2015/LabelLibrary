package com.asiainfo.ocdp.stream.label.examples

import com.asiainfo.ocdp.stream.common.StreamingCache
import com.asiainfo.ocdp.stream.constant.LabelConstant
import com.asiainfo.ocdp.stream.label.Label
import org.slf4j.LoggerFactory

import scala.collection.mutable

/**
  * 打上旅游区域、安防区域标签
  * Created by gengwang on 16/8/11.
  * codis中的结构
      1) "security_area"	"A0AE07002"
      2) "net_type"		"GSM"
      3) "longitude"		"108.876167"
      4) "latitude"		"34.280785"
      5) "city_code"		"A0AE"
      6) "area"			"0.000000"
      7) "eparchy_id"		"A0"
  *
  *
  */
class AreaLabel extends Label {
  val logger = LoggerFactory.getLogger(this.getClass)
  //区域标签前缀
  val tour_type_sine = "tour_area"
  val security_type_sine = "security_area"
  //经纬度标签前缀
  val longitude_sine = "longitude"
  val latitude_sine = "latitude"
  override def attachLabel(line: Map[String, String], cache: StreamingCache, labelQryData: mutable.Map[String, mutable.Map[String, String]]): (Map[String, String], StreamingCache) = {

    val labelMap = fieldsMap()

    val cachedArea = labelQryData.getOrElse(getQryKeys(line).head, Map[String, String]())

    val info_cols=getLabelConf.getFields()//labelItems中的配置
    info_cols.foreach(labelName => {
        labelMap += (labelName -> cachedArea.getOrElse(labelName, ""))
    })
/*
    labelMap += (LabelConstant.LABEL_TOUR_AREA -> "")
    labelMap += (LabelConstant.LABEL_SECURITY_AREA -> "")
    labelMap += (LabelConstant.LABEL_LONGITUDE -> "")
    labelMap += (LabelConstant.LABEL_LATITUDE -> "")
    //标记经度
    if (cachedArea.contains(longitude_sine)) {
      // 从codis中取区域
      val longitude = cachedArea(longitude_sine).trim()
      if (longitude != null && longitude != "") {
        // 信令所在区域列表
        labelMap += (LabelConstant.LABEL_LONGITUDE -> longitude)
      }
    }
    //标记纬度
    if (cachedArea.contains(latitude_sine)) {
      // 从codis中取区域
      val latitude = cachedArea(latitude_sine).trim()
      if (latitude != null && latitude != "") {
        // 信令所在区域列表
        labelMap += (LabelConstant.LABEL_LATITUDE -> latitude)
      }
    }
    // 标记旅游区域标签
    if (cachedArea.contains(tour_type_sine)) {
      // 从codis中取区域
      val area = cachedArea(tour_type_sine).trim()
      if (area != null && area != "") {
        // 信令所在区域列表
        labelMap += (LabelConstant.LABEL_TOUR_AREA -> area)
      }
    }

    // 标记安防标签
    if (cachedArea.contains(security_type_sine)) {
      // 从codis中取区域
      val area = cachedArea(security_type_sine).trim()
      if (area != null && area != "") {
        labelMap += (LabelConstant.LABEL_SECURITY_AREA -> area)
      }
    }
*/

    labelMap ++= line
    (labelMap.toMap, cache)

  }

  /**
    * @param line :MC信令对像
    * @return codis数据库的key
    */
  override def getQryKeys(line: Map[String, String]): Set[String] = Set[String]("area_info:" + line("lac") + "_" + line("cell"))

  /**
    * 把cache的数据转为可变map
    */
  private def transformCacheMap2mutableMap(cacheInfo: Map[String, Map[String, String]]) = {
    val labelsPropMap = mutable.Map[String, mutable.Map[String, String]]()
    cacheInfo.map(infoMap => {
      val copProp = mutable.Map[String, String]()
      infoMap._2.foreach(copProp += _)
      labelsPropMap += (infoMap._1 -> copProp)
    })
    labelsPropMap
  }

  /**
    * 编辑完chache中的内容后重新置为不可变类属
    */
  private def transformCacheMap2ImmutableMap(labelsPropMap: mutable.Map[String, mutable.Map[String, String]]) = {
    if (labelsPropMap.isEmpty) Map[String, Map[String, String]]() else labelsPropMap.map(propSet => (propSet._1, propSet._2.toMap)).toMap
  }

}