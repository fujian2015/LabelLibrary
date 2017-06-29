package com.asiainfo.ocdp.stream.label.examples

import java.text.SimpleDateFormat
import java.util.Date

import com.asiainfo.ocdp.stream.common.{LabelProps, StreamingCache}
import com.asiainfo.ocdp.stream.constant.LabelConstant
import com.asiainfo.ocdp.stream.label.Label
import com.asiainfo.ocdp.stream.tools.DateFormatUtils
//import com.google.protobuf.TextFormat.ParseException
import org.slf4j.LoggerFactory

import scala.collection.mutable

/**
  * Created by gengwang on 16/8/11.
  * 用于判断该条信令是否用于某个景区（区域）日人流量的统计。增强字段isAccu_area=true/false。
  * 依赖AreaLabel标签，
  * 对每条原始数据，与codis中的缓存数据比较，增强标签：是否用于景区人数的计算。
  */
class AccumulateLabel extends Label {
  //labelMap (isAccu_security,true)(isAccu_tour,true)
  val logger = LoggerFactory.getLogger(this.getClass)

  val lineField_imsi="imsi"//原始信令中的字段名称，需要现场配置一致
  val areaLabelField_areaCode="region_code"//AreaLabel标签已增强的区域字段
  val isAreaAccuKey = "isAccu_area"//标签增强字段

//  lazy val thresholdValue = conf.getLong(LabelConstant.STAY_TIMEOUT, LabelConstant.DEFAULT_TIMEOUT_VALUE)

  override def attachLabel(line: Map[String, String], cache: StreamingCache, labelQryData: mutable.Map[String, mutable.Map[String, String]]): (Map[String, String], StreamingCache) = {
    val cacheInstance = if (cache == null) new LabelProps else cache.asInstanceOf[LabelProps]
    val interval = conf.get("interval").toLong
    val lineField_timestamp = conf.get("timestamp", "procedure_start_time")

    // cache中各区域的属性map
    val cacheImmutableMap = cacheInstance.labelsPropList
    // map属性转换
    val cacheMutableMap = transformCacheMap2mutableMap(cacheImmutableMap)

    val normal_imsi = line.getOrElse(lineField_imsi,"")
    // mcsource 打标签用[初始化标签值]
    val labelMap = fieldsMap()
    labelMap.update(isAreaAccuKey, "false")

    //信令中的数据
    val region_codes  = line.getOrElse(areaLabelField_areaCode,"")//区域标签增强的区域ID,一个lac-ci对应的regionCode可能有多个(以","分隔)
    val lineTmestampStr=line.getOrElse(lineField_timestamp,"")
    val lineTmestamp:Long=if(lineTmestampStr.length>0 && lineTmestampStr.indexOf(":")<0)lineTmestampStr.toLong else 0L//信令中的时间戳（注此时间戳是UTC时间，非东8区的时间）
    //println("line time="+lineTmestamp);

    if(lineTmestamp>0L && region_codes.length>0 ) {
      //val current_daystr=new SimpleDateFormat("yyyyMMdd").format(new Date(lineTmestamp.toLong))//在使用java的Date对象时无影响。
      val localLineTime = lineTmestamp + 28800000L //转换为本时区的时间戳,+8小时
      val countStartTime = localLineTime - (localLineTime % (interval * 1000L)) //统计周期的起始时间戳
      //println("local line time="+localLineTime+", count start time="+countStartTime);

      //codis缓存中的数据格式{"$imsi"->{"xxx_school"->统计周期的起始时间戳,"xxx_train"->统计周期的起始时间戳} }
      //查询codis缓存中，该用户当天是否在该区域驻留过
      cacheMutableMap.get(normal_imsi)
      match {
        case None => {//1-无历史缓存
          //println("no history cache,init....")
          val cacheAccuLabelsMap = mutable.Map[String, String]()
          region_codes.split(",").foreach(tmpAreaCode=>{//一个lac-ci对应的regionCode可能有多个
            cacheAccuLabelsMap += (tmpAreaCode -> countStartTime.toString)
          })
          cacheMutableMap += (normal_imsi -> cacheAccuLabelsMap)
          //若缓存中不存在该区域,则设为true,意为"需要将其加入累计人数的计数中"
          labelMap.update(isAreaAccuKey, "true")
        }
        case Some(cacheAccuLabelsMap) => {//2-存在历史缓存
          //println("has history cache,check it....")
          region_codes.split(",").foreach(tmpAreaCode=>{//依次判断每个region_code
            if(cacheAccuLabelsMap.contains(tmpAreaCode)){//2.1 历史缓存中包含本region_code,判断时间
              val cacheTime = cacheAccuLabelsMap.getOrElse(tmpAreaCode,"0").toLong
              if(cacheTime==0L){//未在该区域出现过
                cacheAccuLabelsMap += (tmpAreaCode -> countStartTime.toString)
                labelMap.update(isAreaAccuKey, "true")
              }else if (localLineTime >= ((interval * 1000L) + cacheTime)) {//在该区域出现过，但已超出统计周期
                //更新本次记录时间到cache
                cacheAccuLabelsMap += (tmpAreaCode -> countStartTime.toString)
                labelMap.update(isAreaAccuKey, "true")
              }

            }else{//2.2 历史缓存中包含本region_code，添加到缓存中
              cacheAccuLabelsMap += (tmpAreaCode -> countStartTime.toString)
              labelMap.update(isAreaAccuKey, "true")
            }
          })

          //旧缓存中删除已超时的regionCode，避免缓存数据过大,只保留最近2个统计时段内的数据
          val filterCacheAccuLabelsMap =cacheAccuLabelsMap.filter(tmpRegionMap=>{
            val oldCacheTime=tmpRegionMap._2.toLong
            oldCacheTime >= ( countStartTime- 2 * (interval * 1000L) )
          })

          //cacheMutableMap += (normal_imsi -> cacheAccuLabelsMap)
          cacheMutableMap += (normal_imsi -> filterCacheAccuLabelsMap)
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
