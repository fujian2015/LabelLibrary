package com.asiainfo.ocdp.stream.label.beijing

import com.asiainfo.ocdp.stream.common.StreamingCache
import com.asiainfo.ocdp.stream.constant.LabelConstant
import com.asiainfo.ocdp.stream.label.Label
import org.slf4j.LoggerFactory

import scala.collection.mutable

/**
  * Created by surq on 15/12/20.
  * lac ci基站位置标签
  */
class SiteLabel extends Label {
  val logger = LoggerFactory.getLogger(this.getClass)

  val lac_fieldName = "lac"
  //数据源接口定义的lac字段名称，需与配置现场一致
  val ci_fieldName = "ci"
  //数据源接口定义的ci字段名称，需与配置现场一致
  val type_sine = "area_"
  //标签增强字段的前缀：驻留时长标签会使用、
  val info_sine = "areainfo_" //标签增强字段的前缀：以该前缀开头的标签字段需对应codis中的value的item字段，如areainfo_school_id,需在codis中存在school_id属性值

  val codis_key_prefix = "lacci2area:"
  //codis中基站信息查询key的前缀
  val codis_foreignKeys_separator = "_" //codis多个key的连接符
  val areas_separator = "," //areas分隔符，一个基站可属于多个区域

  override def attachLabel(line: Map[String, String], cache: StreamingCache, labelQryData: mutable.Map[String, mutable.Map[String, String]]): (Map[String, String], StreamingCache) = {

    val labelData = fieldsMap()
    //初始化标签增强字段的值""，LabelItems定义的字段map:bts_name->'',school_id->''
    val label_addFields = conf.getFields

    val qryKey = getQryKeys(line)

    if (qryKey.size == 1) {
      val currentArea = labelQryData.getOrElse(qryKey.head, Map[String, String]())

      // 标记业务区域标签： 如果codis hashmap中，codis value存在item为areas的字段，则取出areas字段中定义的相关区域的值
      //如{"lacci2area:xxxx":{areas:[WLAN,TRAIN,SCENIC,SCHOOL]}} 某个基站区域是WLAN覆盖区域、火车站、景区、校园
      if (currentArea.contains(LabelConstant.LABEL_AREA_LIST_KEY)) {
        // 从codis中取区域
        val areas = currentArea(LabelConstant.LABEL_AREA_LIST_KEY).trim()
        if (areas != null && areas != "") {
          // 信令所在区域列表
          val areasList = areas.split(areas_separator)
          // 只打信令所在区域中要指定的那些区域字段
          areasList.foreach(area => {
            // 添加区域标签前缀
            val labelKey = type_sine + area.trim
            // 在用户定义标签范围内则打标签
            if (label_addFields.contains(labelKey)) {
              // 打区域标签
              labelData.update(labelKey, "true")
            }
          })
        }
      }

      //过滤出标签字段中以areainfo_开头的字段
      val conf_lable_info_items = label_addFields.filter(item => if (item.startsWith(info_sine)) true else false)
      //对以areainfo_开头的字段，打上区域信息,如:areainfo_school_id标签字段，会从codis中取出该区域的shcool_id的值
      conf_lable_info_items.foreach(info => {
        val codis_item = info.substring(9) // 去除［areainfo_］前缀
        labelData.update(info, currentArea.getOrElse(codis_item, ""))
      })

      //其它标签字段附加
      val conf_lable_other_items = label_addFields.filter(item => if (item.startsWith(info_sine)) false else true)
      conf_lable_other_items.foreach(item => {
        currentArea.get(item) match {
          case Some(value) =>
            labelData += (item -> value)
          case None =>
        }
      })
    }

    labelData ++= line
    (labelData.toMap, cache)
  }

  /**
    * @param line :MC信令对像
    * @return codis数据库的key
    */
  override def getQryKeys(line: Map[String, String]): Set[String] = {
    Set[String](codis_key_prefix + line(lac_fieldName) + codis_foreignKeys_separator + line(ci_fieldName))
  }
}
