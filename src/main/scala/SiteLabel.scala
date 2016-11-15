import com.asiainfo.ocdp.stream.common.{LabelProps, StreamingCache}
import com.asiainfo.ocdp.stream.constant.LabelConstant
import com.asiainfo.ocdp.stream.label.Label
import org.slf4j.LoggerFactory

import scala.collection.mutable

/**
  * Created by gengwang on 16/8/11.
  */
class SiteLabel extends Label {

  val logger = LoggerFactory.getLogger(this.getClass)
  val fullPathKey = "full_path"
  val isLatestSite = "isLatestSite"

  lazy val thresholdValue = conf.getLong(LabelConstant.STAY_TIMEOUT, LabelConstant.DEFAULT_TIMEOUT_VALUE)

  override def attachLabel(line: Map[String, String], cache: StreamingCache, labelQryData: mutable.Map[String, mutable.Map[String, String]]): (Map[String, String], StreamingCache) = {
    //2更新标签
    //2.1 根据标签缓存和当前日志信息更新标签
    //a. 获取locationStayRule的cache

   // val cacheInstance = if (cache == null) new LabelProps else cache.asInstanceOf[LabelProps]

    val cacheInstance = if (cache == null) new LabelProps else cache.asInstanceOf[LabelProps]

    // cache中各区域的属性map
    val cacheImmutableMap = cacheInstance.labelsPropList
    // map属性转换
    val cacheMutableMap = transformCacheMap2mutableMap(cacheImmutableMap)
    // mcsource 打标签用[初始化标签值]
    val labelMap = fieldsMap()

    val currentTimestamp = line("timestamp")
    //val currentTimestamp_str = DateFormatUtils.dateMs2Str(currentTimestamp.toLong)

    val lac_cell  = line("lac") + "_" + line("cell")
    val normal_imsi = line("imsi")

    labelMap.update(fullPathKey, "false")
    //labelMap.update("timestampstr", currentTimestamp_str)
    labelMap.update(isLatestSite, "false")


      cacheMutableMap.get(normal_imsi)
      match{
        case None => {
          val cacheSiteLabelsMap = mutable.Map[String, String]()
          cacheSiteLabelsMap += ("timestamp" -> currentTimestamp)
          cacheSiteLabelsMap += ("lac_cell" -> lac_cell)

          cacheMutableMap += (normal_imsi -> cacheSiteLabelsMap)
          //enhance label to add latest timestamp
          labelMap.update(fullPathKey, "true")
          labelMap.update(isLatestSite, "true")
        }
        case Some(cacheSiteLabelsMap) => {
          val latestLacCell = cacheSiteLabelsMap.get("lac_cell").get
          val cacheTime = cacheSiteLabelsMap.get("timestamp").get
          val cacheTimeMs = cacheTime.toLong
          val currentTimeMs = currentTimestamp.toLong

          if (cacheTimeMs <= currentTimeMs) {
            //Update the latest time to the site cache
            cacheSiteLabelsMap += ("timestamp" -> currentTimestamp)
            cacheSiteLabelsMap += ("lac_cell" -> lac_cell)

            labelMap.update(isLatestSite, "true")

            if (!lac_cell.equals(latestLacCell)) {
              // Add the latest path to path cache
              labelMap.update(fullPathKey, "true")
            }

          }
          else {
            if (cacheTimeMs - currentTimeMs < thresholdValue) {
              // 如果信令时间戳小与最新的位置时间戳,将次信令输出到Kafka中,记录轨迹
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
