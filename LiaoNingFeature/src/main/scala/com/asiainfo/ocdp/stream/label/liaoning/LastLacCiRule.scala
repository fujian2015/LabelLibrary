package com.asiainfo.ocdp.stream.label.liaoning

import com.asiainfo.ocdp.stream.common.StreamingCache
import com.asiainfo.ocdp.stream.label.Label
import scala.collection.mutable

/**
  * Created by yangyx on 2017/2/22
  * 上一次实时位置标签：缓存中记录上一次实时位置lac_ci
  */
class LastLacCiRule extends Label {
  /** 取出上一次实时位置lac_ci，并增强到原始数据中
    *
    * @param originalDataMap   kafka原始数据+已经增强的标签数据
    * @param codisLableCache   从codis中查询出来的Lable:$uk缓存数据
    * @param codisLabelQryData 从codis中查询出来的label标签数据：如【userinfo:$UK】,【lacci2area:$lac:$ci】,
    **/

  override def attachLabel(originalDataMap: Map[String, String], codisLableCache: StreamingCache, codisLabelQryData: mutable.Map[String, mutable.Map[String, String]]): (Map[String, String], StreamingCache) = {
    val originalDataFieldName_lac = "lac"
    //数据源接口定义的lac字段名称，需与配置现场一致
    val originalDataFieldName_cell = "ci"
    //数据源接口定义的cell字段名称，需与配置现场一致
    val lableAddFieldName_lac = "last_lac"
    //该标签定义的增强的lac字段名称，需与配置现场一致
    val lableAddFieldName_cell = "last_ci" //该标签定义的增强的cell字段名称，需与配置现场一致

    //本次的实时位置lac ci
    val new_lac = originalDataMap(originalDataFieldName_lac)
    val new_ci = originalDataMap(originalDataFieldName_cell)

    //取出上一次实时位置lac_ci的cache
    val changeLacCiCache = if (codisLableCache == null) new ChangeLacCiProps else codisLableCache.asInstanceOf[ChangeLacCiProps]
    val last_lac_id = changeLacCiCache.cacheLacCi.get(lableAddFieldName_lac) match {
      case Some(v) => v
      case None => ""
    }
    val last_cell_id = changeLacCiCache.cacheLacCi.get(lableAddFieldName_cell) match {
      case Some(v) => v
      case None => ""
    }

    val newLine = fieldsMap() //初始化增强字段，默认为“”
    //增强：上一次实时位置lac_ci的value
    newLine.update(lableAddFieldName_lac, last_lac_id)
    newLine.update(lableAddFieldName_cell, last_cell_id)
    newLine ++= originalDataMap

    //更新codis的cache
    changeLacCiCache.cacheLacCi = Map[String, String](lableAddFieldName_lac -> new_lac, lableAddFieldName_cell -> new_ci)

    //返回增强后的数据、待更新的codis cache
    (newLine.toMap, changeLacCiCache)
  }
}

class ChangeLacCiProps extends StreamingCache with Serializable {
  var cacheLacCi = Map[String, String]()
}
