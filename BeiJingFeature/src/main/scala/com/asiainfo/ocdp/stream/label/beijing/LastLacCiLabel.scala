package com.asiainfo.ocdp.stream.label.beijing

import com.asiainfo.ocdp.stream.common.StreamingCache
import com.asiainfo.ocdp.stream.label.Label

import scala.collection.mutable

/**
  * Created by yangyx on 2017/2/22
  * 上一次实时位置标签：缓存中记录上一次实时位置lac_ci
  */
class LastLacCiLabel extends Label {

  // 数据源接口定义的lac字段名称，需与配置现场一致
  val originalDataFieldName_lac = "lac"
  // 数据源接口定义的cell字段名称，需与配置现场一致
  val originalDataFieldName_cell = "ci"
  // 数据源接口定义的信令开始时间字段，需与用户标签代码里时间格式转换后的字段一致
  val originalDataFieldName_time = "datetime_format"

  // 上一次的lac，追加到原始信令中
  val labelAddFieldName_lac = "last_lac"
  // 上一次的cellid，追加到原始信令中
  val labelAddFieldName_cell = "last_ci"
  // 上一次的基站位置lac_ci，追加到原始信令中
  val labelAddFieldName_lac_cell = "last_lac_ci"
  // 上一次的进入时间
  val last_intime = "intime"
  // 上一次的活跃时间
  val last_activetime = "activetime"

  // 位置是否发生变更
  val is_changed = "ischanged"

  /** 取出上一次实时位置lac_ci，并增强到原始数据中
    *
    * @param originalDataMap   kafka原始数据+已经增强的标签数据
    * @param codisLableCache   从codis中查询出来的Lable:$uk缓存数据
    * @param codisLabelQryData 从codis中查询出来的label标签数据：如【userinfo:$UK】,【lacci2area:$lac:$ci】,
    **/
  override def attachLabel(originalDataMap: Map[String, String], codisLableCache: StreamingCache, codisLabelQryData: mutable.Map[String, mutable.Map[String, String]]): (Map[String, String], StreamingCache) = {

    val newLine = fieldsMap() //初始化增强字段为空，需要在前台json配置

    //本次的实时位置lac ci
    val new_lac = originalDataMap(originalDataFieldName_lac)
    val new_ci = originalDataMap(originalDataFieldName_cell)
    val new_time = originalDataMap(originalDataFieldName_time)
    val new_lac_ci = new_lac + "_" + new_ci

    //取出上一次实时位置lac_ci的cache
    val changeLacCiCache = if (codisLableCache == null) new ChangeLacCiProps else codisLableCache.asInstanceOf[ChangeLacCiProps]
    val last_lac_id = changeLacCiCache.cacheLacCi.get(labelAddFieldName_lac) match {
      case Some(v) => v
      case None => ""
    }
    val last_cell_id = changeLacCiCache.cacheLacCi.get(labelAddFieldName_cell) match {
      case Some(v) => v
      case None => ""
    }
    val last_in_time = changeLacCiCache.cacheLacCi.get(last_intime) match {
      case Some(v) => v
      case None => ""
    }
    val last_active_time = changeLacCiCache.cacheLacCi.get(last_activetime) match {
      case Some(v) => v
      case None => ""
    }
    val last_lac_ci = last_lac_id + "_" + last_cell_id


    //非第一次进入，已经在codis里面缓存过
    if (last_lac_id != "" && last_cell_id != "") {

      //当前信令时间>=上一次的活跃时间，即过滤掉了延时到达的数据
      if (new_time >= last_active_time) {

        //不管位置是否发生变更都需要增强数据
        newLine.update(labelAddFieldName_lac, last_lac_id)
        newLine.update(labelAddFieldName_cell, last_cell_id)
        newLine.update(labelAddFieldName_lac_cell, last_lac_ci)
        newLine.update(last_intime, last_in_time) //上一次的进入时间
        newLine.update(last_activetime, new_time) //上一次的活跃时间，如果位置发生变更则为离开时间

        //位置发生变更时
        if (!new_lac_ci.equals(last_lac_ci)) {

          newLine.update(is_changed, "true")
          // 更新codis缓存为最新的位置
          changeLacCiCache.cacheLacCi = Map[String, String](labelAddFieldName_lac -> new_lac, labelAddFieldName_cell -> new_ci, last_intime -> new_time, last_activetime -> new_time)
        } else {

          newLine.update(is_changed, "false")
          //如果位置没有发生变更只更新活跃时间
          changeLacCiCache.cacheLacCi = Map[String, String](labelAddFieldName_lac -> new_lac, labelAddFieldName_cell -> new_ci, last_intime -> last_in_time, last_activetime -> new_time)
        }
      }
    } else {
      //第一次进入只更新codis缓存:本次的lac、ci、当前信令时间表示进入时间；此时上一次的信息都为空
      //第一次进来认为是位置发生变更
      newLine.update(is_changed, "true")
      newLine.update(last_intime, new_time) //进入时间
      changeLacCiCache.cacheLacCi = Map[String, String](labelAddFieldName_lac -> new_lac, labelAddFieldName_cell -> new_ci, last_intime -> new_time, last_activetime -> new_time)
    }

    //返回增强后的数据、待更新的codis cache
    newLine ++= originalDataMap
    (newLine.toMap, changeLacCiCache)
  }
}

class ChangeLacCiProps extends StreamingCache with Serializable {
  var cacheLacCi = Map[String, String]()
}
