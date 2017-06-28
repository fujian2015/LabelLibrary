package com.asiainfo.ocdp.stream.label

import java.util.Date

import com.asiainfo.ocdp.stream.common.{SimpleLabelProps, StreamingCache}
import com.asiainfo.ocdp.stream.label.Label
import org.slf4j.LoggerFactory

import scala.collection.mutable

/**
  * Created by yangyx on 2017/2/22
  * 当用户所在的lac-ci位置发生变化时，输出用户停留在上一个lac-ci位置以及起始时间
  * {"props":[],"labelItems":[
  * {"pname":"上一次lac","pvalue":"last_lac","description":"上一次lac"},
  * {"pname":"上一次ci","pvalue":"last_ci","description":"上一次ci"},
  * {"pname":"上一次lac-ci的首次时间","pvalue":"llc_first_dt","description":"上一次lac-ci的首次时间"},
  * {"pname":"上一次lac-ci的结束时间","pvalue":"llc_end_dt","description":"上一次lac-ci的结束时间"}
  * ]
  * }
  */
class LastLacCiLabelStayRule extends Label {
  val logger = LoggerFactory.getLogger(this.getClass)
  val originalDataFieldName_lac = "lac"  //数据源接口定义的lac字段名称，需与配置现场一致
  val originalDataFieldName_cell = "ci"  //数据源接口定义的cell字段名称，需与配置现场一致
  val originalDataFieldName_time = "time"  //数据源接口定义的信令时间字段名称，需与配置现场一致

  val labelAddFieldName_lac = "last_lac"  //标签增强的lac字段名称：上一次的lac位置，需与配置现场一致
  val labelAddFieldName_ci = "last_ci"  //标签增强字段：上一次的ci位置
  val labelAddFieldName_firstTime = "llc_first_dt"  //标签增强字段：停留在上一个lac-ci位置的起始时间
  val labelAddFieldName_endTime = "llc_end_dt"
  /** 取出上一次实时位置lac_ci，并增强到原始数据中
    *
    * @param originalDataMap   kafka原始数据+已经增强的标签数据
    * @param codisLableCache   从codis中查询出来的Lable:$uk缓存数据
    * @param codisLabelQryData 从codis中查询出来的label标签数据：如【userinfo:$UK】,【lacci2area:$lac:$ci】,
    **/
  override def attachLabel(originalDataMap: Map[String, String], codisLableCache: StreamingCache, codisLabelQryData: mutable.Map[String, mutable.Map[String, String]]): (Map[String, String], StreamingCache) = {
    //val sdf:java.text.SimpleDateFormat=new java.text.SimpleDateFormat(originalDataFieldName_timeFormat)
    //logger.debug("LastLacCiLabelStayRule begin......")
    //logger.debug(conf.getAll.toString)

    //本次的实时位置lac、ci、time
    val curr_lac = originalDataMap.getOrElse(originalDataFieldName_lac,"")
    val curr_ci = originalDataMap.getOrElse(originalDataFieldName_cell,"")
    val curr_timeStr = originalDataMap.getOrElse(originalDataFieldName_time,"")
    //logger.info("curr line data:lac="+curr_lac+",ci="+curr_ci+",time="+curr_timeStr)

    //取出上一次实时位置lac_ci的cache
    val lastLacCiCache = if (codisLableCache == null){new SimpleLabelProps} else codisLableCache.asInstanceOf[SimpleLabelProps]
    val lastLacCiCacheValueMap=lastLacCiCache.labelsPropList//codis缓存value
    val last_lac_id = lastLacCiCacheValueMap.getOrElse(labelAddFieldName_lac,"")
    val last_ci_id  = lastLacCiCacheValueMap.getOrElse(labelAddFieldName_ci,"")
    val llc_first = lastLacCiCacheValueMap.getOrElse(labelAddFieldName_firstTime,"")
    //val llc_end = lastLacCiCacheValueMap.getOrElse(labelAddFieldName_endTime,"")
    //logger.info("last LabelCache data:last_lac="+last_lac_id+",last_ci="+last_ci_id+",last_time="+llc_first)

    val addFieldMap = fieldsMap() //初始化增强字段，默认为“”
    //新codis缓存数据
    var newcache_lac=""
    var newcache_ci=""
    var newcache_ftime=""

    if(last_lac_id.isEmpty || last_ci_id.isEmpty){//1.第一次处理，无历史缓存数据
      //logger.info("no last labelCache,init......")
      //向codis缓存中记录本次的位置及first_dt，
      newcache_lac=curr_lac
      newcache_ci=curr_ci
      newcache_ftime=curr_timeStr

      //标签增强的各字段设置为空
    }else{//有历史缓存数据
      if(curr_lac==last_lac_id && curr_ci==last_ci_id){//2.用户所在位置无变化，
        //logger.info("last labelCache is same with current,no change labelCache......")
        //a:不更新codis缓存数据
        newcache_lac=last_lac_id
        newcache_ci=last_ci_id
        if(llc_first.isEmpty || llc_first.compareTo(curr_timeStr)>0){//延时到达数据
          newcache_ftime=curr_timeStr
        }else{
          newcache_ftime=llc_first
        }
        //b:标签中只增强字段lac、ci、firsttime,endtime为空
        addFieldMap.update(labelAddFieldName_lac,last_lac_id)
        addFieldMap.update(labelAddFieldName_ci,last_ci_id)
        if(llc_first.compareTo(curr_timeStr)>0){
          addFieldMap.update(labelAddFieldName_firstTime,curr_timeStr)
        }else{
          addFieldMap.update(labelAddFieldName_firstTime,llc_first)
        }
      }else{//3.用户所在位置发生变化，
        //logger.info("last labelCache is diff with current,change labelCache......")
        if(llc_first.compareTo(curr_timeStr)<0){//正常数据
          //a:codis缓存中记录本次的lac、ci、firsttime
          newcache_lac=curr_lac
          newcache_ci=curr_ci
          newcache_ftime=curr_timeStr
          //b:标签中增强字段lac、ci、firsttime、endtime
          addFieldMap.update(labelAddFieldName_lac,last_lac_id)
          addFieldMap.update(labelAddFieldName_ci,last_ci_id)
          addFieldMap.update(labelAddFieldName_firstTime,llc_first)
          addFieldMap.update(labelAddFieldName_endTime,curr_timeStr)
        }else{//延时到达数据，忽略
          newcache_lac=last_lac_id
          newcache_ci=last_ci_id
          newcache_ftime=llc_first
        }
      }
    }
   addFieldMap ++= originalDataMap

    //更新codis的cache
    lastLacCiCache.labelsPropList = Map(labelAddFieldName_lac->newcache_lac,labelAddFieldName_ci->newcache_ci,labelAddFieldName_firstTime->newcache_ftime)
    //logger.info("this saved LabelCache data:last_lac="+lastLacCiCache.labelsPropList.getOrElse("last_lac","")+",last_ci="+lastLacCiCache.labelsPropList.getOrElse("last_ci","")+",last_time="+lastLacCiCache.labelsPropList.getOrElse(labelAddFieldName_firstTime,""))

    //返回增强后的数据、待更新的codis cache
    (addFieldMap.toMap, lastLacCiCache)

  }

  override def getQryKeys(line: Map[String, String]): Set[String] = Set[String]("")

}

/**
  * codis的labelCahce中保存三个属性：last_lac、last-ci、llc_first_dt
  * */
//class ChangeLacCiProps extends StreamingCache with Serializable {
//  var cacheLacCi = Map[String, String]()
//}
