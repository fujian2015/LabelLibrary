import com.asiainfo.ocdp.stream.common.{LabelProps, StreamingCache}
import com.asiainfo.ocdp.stream.constant.LabelConstant
import com.asiainfo.ocdp.stream.label.Label
import com.asiainfo.ocdp.stream.tools.DateFormatUtils
//import com.google.protobuf.TextFormat.ParseException
import org.slf4j.LoggerFactory

import scala.collection.mutable

/**
  * Created by gengwang on 16/8/11.
  */
class AccumulateLabel extends Label {
  //labelMap (isAccu_security,true)(isAccu_tour,true)
  val logger = LoggerFactory.getLogger(this.getClass)
  val isSecurityAccuKey = "isAccu_security"
  val isTourAccuKey = "isAccu_tour"

  lazy val thresholdValue = conf.getLong(LabelConstant.STAY_TIMEOUT, LabelConstant.DEFAULT_TIMEOUT_VALUE)

  override def attachLabel(line: Map[String, String], cache: StreamingCache, labelQryData: mutable.Map[String, mutable.Map[String, String]]): (Map[String, String], StreamingCache) = {
    //2更新标签
    //2.1 根据标签缓存和当前日志信息更新标签
    //a. 获取locationStayRule的cache

   // val cacheInstance = if (cache == null) new LabelProps else cache.asInstanceOf[LabelProps]

    val cacheInstance = if (cache == null) new LabelProps else cache.asInstanceOf[LabelProps]
    val interval = conf.get("interval").toLong
    // cache中各区域的属性map
    val cacheImmutableMap = cacheInstance.labelsPropList
    // map属性转换
    val cacheMutableMap = transformCacheMap2mutableMap(cacheImmutableMap)
    // mcsource 打标签用[初始化标签值]
    val labelMap = fieldsMap()
    //由于陕西移动信令格式时间戳为日期而非毫秒数,因此将日期末位补0,转为毫秒数进行处理
    val currentTime = DateFormatUtils.dateStr2Ms(line("timestamp")+"0","yyyyMMddhhmmssSSS")

    //由于业务需求统计周期为自然日,因此应将currentTime对应的当日时间00:00写入cache
    //currentTime为1970-01-01 08:00:00.000到当前的毫秒数,因此+8个小时的毫秒数(28800000L)得出1970-01-01 00:00:00.000 到当前的毫秒数
    //(currentTime + 28800000L) % (86400000L) 为当日超过00:00的毫秒数,
    val currentTime_byDay = currentTime  - (currentTime + 28800000L) % (86400000L)
    val tour_area  = line("tour_area")
    val security_area = line("security_area")
    val tour_cache_key = "@tour@" + tour_area
    val security_cache_key = "@security@" + security_area
    labelMap.update(isSecurityAccuKey, "false")
    labelMap.update(isTourAccuKey, "false")

    //分别根据 imsi@旅游区域 和 imsi@安防区域 查询缓存
    cacheMutableMap.get(tour_cache_key)
    match{
      case None => {
        val cacheAccuLabelsMap = mutable.Map[String, String]()
        cacheAccuLabelsMap += ("timestamp" -> currentTime_byDay.toString)
        cacheMutableMap += (tour_cache_key -> cacheAccuLabelsMap)
        //若缓存中不存在该 imsi@旅游区域,则将isTourAccuKey设为true,指需要将其加入累计人数的计数中
        labelMap.update(isTourAccuKey, "true")
      }
      case Some(cacheAccuLabelsMap) => {

        val cacheTime = cacheAccuLabelsMap.get("timestamp").get.toLong
        if (currentTime >= ((interval * 1000L) + cacheTime)) {
          //更新本次记录时间到cache
          cacheAccuLabelsMap += ("timestamp" -> currentTime_byDay.toString)
          cacheMutableMap += (tour_cache_key -> cacheAccuLabelsMap)
          labelMap.update(isTourAccuKey, "true")
        }
      }
    }
    cacheMutableMap.get(security_cache_key)
    match{
      case None => {
        val cacheAccuLabelsMap = mutable.Map[String, String]()
        cacheAccuLabelsMap += ("timestamp" -> currentTime_byDay.toString)
        cacheMutableMap += (security_cache_key -> cacheAccuLabelsMap)
        //若缓存中不存在该 imsi@安防区域,则将isSecurityAccuKey设为true,指需要将其加入累计人数的计数中
        labelMap.update(isSecurityAccuKey, "true")
      }
      case Some(cacheAccuLabelsMap) => {
        val cacheTime = cacheAccuLabelsMap.get("timestamp").get.toLong
        if (currentTime >= ((interval * 1000L) + cacheTime)) {
          //更新本次记录时间到cache
          cacheAccuLabelsMap += ("timestamp" -> currentTime_byDay.toString)
          cacheMutableMap += (security_cache_key -> cacheAccuLabelsMap)
          labelMap.update(isSecurityAccuKey, "true")
        }
      }
    }
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
