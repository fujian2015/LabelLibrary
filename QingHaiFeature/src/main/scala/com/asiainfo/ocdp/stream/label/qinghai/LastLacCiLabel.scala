package com.asiainfo.ocdp.stream.label.qinghai

import com.asiainfo.ocdp.stream.common.{LabelProps, StreamingCache}
import com.asiainfo.ocdp.stream.label.Label

import scala.collection.mutable

/**
  * 用户轨迹标签：缓存中记录上一次实时位置lac_ci
  */
class LastLacCiLabel extends Label {

  //数据源接口定义的lac字段名称，需与配置现场一致
  val originalDataFieldName_lac = "lac"

  //数据源接口定义的cellid字段名称，需与配置现场一致
  val originalDataFieldName_cell = "ci"

  //该标签定义的增强的lac字段名称，需与配置现场一致
  val labelAddFieldName_lac = "last_lac"

  //该标签定义的增强的cellid字段名称，需与配置现场一致
  val labelAddFieldName_cell = "last_ci"

  // 该标签定义的增强的last_lac_ci字段名称，需与配置现场一致
  val labelAddFieldName_lac_cell = "last_lac_ci"


  /** 取出上一次实时位置lac_ci，并增强到原始数据中
    *
    * @param originalDataMap   kafka原始数据+已经增强的标签数据
    * @param codisLableCache   从codis中查询出来的Lable:$uk缓存数据
    * @param codisLabelQryData 从codis中查询出来的label标签数据：如【userinfo:$UK】,【lacci2area:$lac:$ci】,
    **/
  override def attachLabel(originalDataMap: Map[String, String], codisLableCache: StreamingCache, codisLabelQryData: mutable.Map[String, mutable.Map[String, String]]): (Map[String, String], StreamingCache) = {

    val newLine = fieldsMap() //初始化增强字段，默认为“”

    //获取本次的实时位置lac和ci
    val new_lac = originalDataMap(originalDataFieldName_lac)
    val new_ci = originalDataMap(originalDataFieldName_cell)
    // println("-----new lac_ci-----:" + new_lac + "_" + new_ci)

    // 缓存不可变Map转换为可变map
    val changeLacCiCache = if (codisLableCache == null) new LabelProps else codisLableCache.asInstanceOf[LabelProps]
    val cacheImmutableMap = changeLacCiCache.labelsPropList
    val cacheMutableMap = transformCacheMap2mutableMap(cacheImmutableMap)

    //获取codis缓存的上一次的lac和ci
    cacheMutableMap.get(labelAddFieldName_lac_cell)
    match {
      case None => {
        // do nothing
      }
      case Some(cacheLastLacciMap) => {
        val last_lac = cacheLastLacciMap.getOrElse(labelAddFieldName_lac, "")
        val last_cell = cacheLastLacciMap.getOrElse(labelAddFieldName_cell, "")
        //println("-----last_lac_ci-----:" + last_lac + "_" + last_cell)

        //增强：上一次实时位置lac_ci的value
        newLine.update(labelAddFieldName_lac, last_lac)
        newLine.update(labelAddFieldName_cell, last_cell)
        if (last_lac != "" && last_cell != "") {
          newLine.update(labelAddFieldName_lac_cell, last_lac + "_" + last_cell)
        }
      }
    }

    //更新codis的cache
    val cacheNewLacciMap = mutable.Map[String, String]()
    cacheNewLacciMap += (labelAddFieldName_lac -> new_lac)
    cacheNewLacciMap += (labelAddFieldName_cell -> new_ci)
    changeLacCiCache.labelsPropList = transformCacheMap2ImmutableMap(cacheMutableMap += (labelAddFieldName_lac_cell -> cacheNewLacciMap))

    //返回增强后的数据、待更新的cache
    newLine ++= originalDataMap
    (newLine.toMap, changeLacCiCache)
  }

  /**
    * 编辑完chache中的内容后重新置为不可变类属
    */
  private def transformCacheMap2ImmutableMap(labelsPropMap: mutable.Map[String, mutable.Map[String, String]]) = {
    if (labelsPropMap.isEmpty) Map[String, Map[String, String]]() else labelsPropMap.map(propSet => (propSet._1, propSet._2.toMap)).toMap
  }

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
}

