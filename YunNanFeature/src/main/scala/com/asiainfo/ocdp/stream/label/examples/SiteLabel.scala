package com.asiainfo.ocdp.stream.label.examples

import com.asiainfo.ocdp.stream.common.{LabelProps, StreamingCache}
import com.asiainfo.ocdp.stream.constant.LabelConstant
import com.asiainfo.ocdp.stream.label.Label
import org.slf4j.LoggerFactory

import scala.collection.mutable

/**
  * 用户轨迹标签
  * 根据信令中的lac、ci、timestamp信息，来判断当前数据是否是最新位置信息、是否可用于沉淀用户轨迹模型
  * Created by gengwang on 16/8/11.
  */
class SiteLabel extends Label {

  lazy val thresholdValue = conf.getLong(LabelConstant.STAY_TIMEOUT, LabelConstant.DEFAULT_TIMEOUT_VALUE)
  val logger = LoggerFactory.getLogger(this.getClass)
  val fullPathKey = "full_path"//是否用于沉淀用户轨迹
  val isLatestSite = "isLatestSite"//是否最新位置数据

  var lineField_imsi="imsi"//原始信令中的字段名称，需要现场配置一致
  var lineField_lac="tac"//原始信令中的字段名称，需要现场配置一致
  var lineField_ci="cell_id"//原始信令中的字段名称，需要现场配置一致

  override def attachLabel(line: Map[String, String], cache: StreamingCache, labelQryData: mutable.Map[String, mutable.Map[String, String]]): (Map[String, String], StreamingCache) = {
    //2更新标签
    //2.1 根据标签缓存和当前日志信息更新标签
    //a. 获取locationStayRule的cache

   // val cacheInstance = if (cache == null) new LabelProps else cache.asInstanceOf[LabelProps]
    val labelMap = fieldsMap()
    val timestampFieldName = conf.get("timestamp", "procedure_start_time")

    val cacheInstance = if (cache == null) new LabelProps else cache.asInstanceOf[LabelProps]

    try {
    // cache中各区域的属性map，缓存数据中记录了2个信息：lac_cell和信令中时间戳的值
    val cacheImmutableMap = cacheInstance.labelsPropList
    // map属性转换
    val cacheMutableMap = transformCacheMap2mutableMap(cacheImmutableMap)

    val currentTimestamp = line.getOrElse(timestampFieldName,"0")//信令中的时间数据（时间戳）
    //val currentTimestamp_str = DateFormatUtils.dateMs2Str(currentTimestamp.toLong)
    val lac_cell  = line.getOrElse(lineField_lac,"") + "_" + line.getOrElse(lineField_ci,"")
    val normal_imsi = line.getOrElse(lineField_imsi,"")

    //[初始化标签值]
    labelMap.update(fullPathKey, "false")
    //labelMap.update("timestampstr", currentTimestamp_str)
    labelMap.update(isLatestSite, "false")

    //从label的cache中获取出历史缓存数据
    cacheMutableMap.get(normal_imsi)
      match{
        case None => {//无历史缓存，新建缓存
          val cacheSiteLabelsMap = mutable.Map[String, String]()
          cacheSiteLabelsMap += (timestampFieldName -> currentTimestamp)
          cacheSiteLabelsMap += ("lac_cell" -> lac_cell)

          cacheMutableMap += (normal_imsi -> cacheSiteLabelsMap)
          //enhance label to add latest timestamp
          labelMap.update(fullPathKey, "true")
          labelMap.update(isLatestSite, "true")
        }
        case Some(cacheSiteLabelsMap) => {//有历史缓存
          val latestLacCell = cacheSiteLabelsMap.get("lac_cell").get//缓存中的lac-ci
          val cacheTime = cacheSiteLabelsMap.get(timestampFieldName).get//缓存中的时间
          val cacheTimeMs = cacheTime.toLong

          val currentTimeMs = currentTimestamp.toLong//当前信令中的时间

          if (cacheTimeMs <= currentTimeMs) {//当前信令中的时间 晚与 缓存中的时间(正常数据)
            //Update the latest time to the site cache
            cacheSiteLabelsMap += (timestampFieldName -> currentTimestamp)
            cacheSiteLabelsMap += ("lac_cell" -> lac_cell)

            labelMap.update(isLatestSite, "true")//是最新位置

            if (!lac_cell.equals(latestLacCell)) {
              // Add the latest path to path cache
              labelMap.update(fullPathKey, "true")//信令中的lac-ci与缓存中lac-ci不同时，用于沉淀用户轨迹
            }

          }else {//延迟到达的数据
            if (cacheTimeMs - currentTimeMs < thresholdValue) {
              // 如果信令时间戳小与最新的位置时间戳,将此信令输出到Kafka中,记录轨迹
              // Add the latest path to path cache
              labelMap.update(fullPathKey, "true")
            }
          }

        }
      }


    // c. 给mcsoruce设定连续停留[LABEL_STAY]标签
    labelMap ++= line

    //3更新缓存
    // map属性转换
    cacheInstance.labelsPropList = transformCacheMap2ImmutableMap(cacheMutableMap)
    } catch {
      case ex: Exception => {
        logger.error("Annoying error:",ex)
      }
    }

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

/*
class LabelProps extends StreamingCache with Serializable {
  var labelsPropList = Map[String, Map[String, String]]()
}
*/
