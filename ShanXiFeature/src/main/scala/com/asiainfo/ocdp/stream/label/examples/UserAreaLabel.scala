package com.asiainfo.ocdp.stream.label.examples

import com.asiainfo.ocdp.stream.common.{LabelProps, StreamingCache}
import com.asiainfo.ocdp.stream.constant.LabelConstant
import com.asiainfo.ocdp.stream.label.Label
import org.slf4j.LoggerFactory

import scala.collection.mutable

/**
  * 流入流出标签
  * Created by ld on 17/3/15.
  */
class UserAreaLabel extends Label {
  val logger = LoggerFactory.getLogger(this.getClass)
  //业务主键
  val imsiFieldName = "imsi"
  //特殊区域标签前缀
  val tour_type_sine = "tour_area"
  val security_type_sine = "security_area"
  // 旅游区域流入流出前缀
  val tour_type_in_sine = "tour_area_in"
  val tour_type_out_sine = "tour_area_out"
  // 安防区域流入流出前缀
  val security_type_in_sine = "security_area_in"
  val security_type_out_sine = "security_area_out"
  val user_tour_sine = "user_tour:"
  //用户所在旅游区域key前缀
  val user_security_sine = "user_security:" //用户所在安防区域key前缀

  override def attachLabel(line: Map[String, String], cache: StreamingCache, labelQryData: mutable.Map[String, mutable.Map[String, String]]): (Map[String, String], StreamingCache) = {

    // String->Map(String,String)
    val cacheInstance = if (cache == null) new LabelProps else cache.asInstanceOf[LabelProps]

    // cache中各区域的属性map
    val cacheImmutableMap = cacheInstance.labelsPropList
    // map属性转换
    val cacheMutableMap = transformCacheMap2mutableMap(cacheImmutableMap)

    val tour_key = user_tour_sine + line(imsiFieldName)
    val security_key = user_security_sine + line(imsiFieldName)

    //用户当前所在旅游区域编码
    val tour_area = line(tour_type_sine)
    //用户当前所在安防区域编码
    val security_area = line(security_type_sine)

    val labelMap = fieldsMap()
    //初始化流入流出标签值
    labelMap += (tour_type_in_sine -> "")
    labelMap += (tour_type_out_sine -> "")
    labelMap += (security_type_in_sine -> "")
    labelMap += (security_type_out_sine -> "")


    if ((tour_area != null && tour_area != "") || (security_area != null && security_area != "")) {

      //标记旅游区域
      if (tour_area != null && tour_area != "") {

        //比对codis缓存，判断流入流出
        cacheMutableMap.get(tour_key)
        match {
          case None => {
            val cacheSiteLabelsMap = mutable.Map[String, String]()
            cacheSiteLabelsMap += (tour_type_sine -> tour_area) //缓存当前旅游区域编码
            cacheMutableMap += (tour_key -> cacheSiteLabelsMap)
            //用户进入新区域则为流入，流出为空
            labelMap.update(tour_type_in_sine, tour_area)
            labelMap.update(tour_type_out_sine, "")
          }
          case Some(cacheSiteLabelsMap) => {
            val last_tour_area = cacheSiteLabelsMap.getOrElse(tour_type_sine, "")
            //用户所在特殊区域位置未发生变化，则流入流出为空,不更新缓存
            if (last_tour_area.equals(tour_area)) {
              labelMap.update(tour_type_in_sine, "")
              labelMap.update(tour_type_out_sine, "")
            }
            //用户所在特殊区域位置发生变化，则流入为当前位置，流出为上一次位置，更新缓存
            else {
              cacheSiteLabelsMap += (tour_type_sine -> tour_area) //缓存当前旅游区域编码
              cacheMutableMap += (tour_key -> cacheSiteLabelsMap)
              labelMap.update(tour_type_in_sine, tour_area)
              labelMap.update(tour_type_out_sine, last_tour_area)
            }
          }
        }

      }

      // 标记安防标签
      if (security_area != null && security_area != "") {

        //比对codis缓存，判断流入流出
        cacheMutableMap.get(security_key)
        match {
          case None => {
            val cacheSiteLabelsMap = mutable.Map[String, String]()
            cacheSiteLabelsMap += (security_type_sine -> security_area) //缓存当前安防区域编码
            cacheMutableMap += (security_key -> cacheSiteLabelsMap)
            //用户进入新区域则为流入，流出为空
            labelMap.update(security_type_in_sine, security_area)
            labelMap.update(security_type_out_sine, "")
          }
          case Some(cacheSiteLabelsMap) => {
            val last_security_area = cacheSiteLabelsMap.getOrElse(security_type_sine, "")
            //用户所在特殊区域位置未发生变化，则流入流出为空,不更新缓存
            if (last_security_area.equals(security_area)) {
              labelMap.update(security_type_in_sine, "")
              labelMap.update(security_type_out_sine, "")
            }
            //用户所在特殊区域位置发生变化，则流入为当前位置，流出为上一次位置，更新缓存
            else {
              cacheSiteLabelsMap += (security_type_sine -> security_area) //缓存当前安防区域编码
              cacheMutableMap += (security_key -> cacheSiteLabelsMap)
              labelMap.update(security_type_in_sine, security_area)
              labelMap.update(security_type_out_sine, last_security_area)
            }
          }
        }

      }
    } else {

      //将不在特殊区域的用户缓存置为空
      if (cacheMutableMap.contains(tour_key)) {
        cacheMutableMap.get(tour_key)
        match {
          case Some(cacheSiteLabelsMap) => {
            val last_tour_area = cacheSiteLabelsMap.getOrElse(tour_type_sine, "")
            labelMap.update(tour_type_in_sine, "")
            labelMap.update(tour_type_out_sine, last_tour_area)
            cacheSiteLabelsMap += (tour_type_sine -> "")
            cacheMutableMap += (tour_key -> cacheSiteLabelsMap)
          }
        }
      }
      if (cacheMutableMap.contains(security_key)) {
        cacheMutableMap.get(security_key)
        match {
          case Some(cacheSiteLabelsMap) => {
            val last_security_area = cacheSiteLabelsMap.getOrElse(security_type_sine, "")
            labelMap.update(security_type_in_sine, "")
            labelMap.update(security_type_out_sine, last_security_area)
            cacheSiteLabelsMap += (security_type_sine -> "")
            cacheMutableMap += (security_key -> cacheSiteLabelsMap)
          }
        }
      }
    }

    cacheInstance.labelsPropList = transformCacheMap2ImmutableMap(cacheMutableMap)
    labelMap ++= line
    (labelMap.toMap, cacheInstance)

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

  /**
    * 编辑完chache中的内容后重新置为不可变类属
    */
  private def transformCacheMap2ImmutableMap(labelsPropMap: mutable.Map[String, mutable.Map[String, String]]) = {
    if (labelsPropMap.isEmpty) Map[String, Map[String, String]]() else labelsPropMap.map(propSet => (propSet._1, propSet._2.toMap)).toMap
  }

}