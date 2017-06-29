package com.asiainfo.ocdp.stream.label.examples

import com.asiainfo.ocdp.stream.common.StreamingCache
import com.asiainfo.ocdp.stream.constant.LabelConstant
import com.asiainfo.ocdp.stream.label.Label
import org.slf4j.LoggerFactory

import scala.collection.mutable

/**
  * Created by peng on 2017/6/1.
  * 根据lac-ci，获取该基站的归属区域、经纬度等信息
  */
class AreaLabel extends Label {
  val logger = LoggerFactory.getLogger(this.getClass)
  //区域标签前缀
//  val tour_type_sine = "tour_area"
//  val security_type_sine = "security_area"
//  //经纬度标签前缀
//  val longitude_sine = "longitude"
//  val latitude_sine = "latitude"
  var lineField_lac="tac"//原始信令中的字段名称，需要现场配置一致
  var lineField_ci="cell_id"//原始信令中的字段名称，需要现场配置一致

  override def attachLabel(line: Map[String, String], cache: StreamingCache, labelQryData: mutable.Map[String, mutable.Map[String, String]]): (Map[String, String], StreamingCache) = {
    val labelMap = fieldsMap()
    val labelAddFields=getLabelConf.getFields()

    val cachedArea = labelQryData.getOrElse(getQryKeys(line).head, Map[String, String]())

    labelAddFields.foreach(labelName => {
      cachedArea.get(labelName) match {
        case Some(value) =>
          labelMap += (labelName -> value)
        case None =>
      }
    })

/*    labelMap += (LabelConstant.LABEL_TOUR_AREA -> "")
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
  override def getQryKeys(line: Map[String, String]): Set[String] = Set[String]("area_info:" + line.getOrElse(lineField_lac,"") + "_" + line.getOrElse(lineField_ci,""))

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
