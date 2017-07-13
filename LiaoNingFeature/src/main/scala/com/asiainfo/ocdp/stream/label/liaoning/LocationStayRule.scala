package com.asiainfo.ocdp.stream.label.liaoning

import com.asiainfo.ocdp.stream.common.{LabelProps, StreamingCache}
import com.asiainfo.ocdp.stream.constant.LabelConstant
import com.asiainfo.ocdp.stream.tools.DateFormatUtils
import com.asiainfo.ocdp.stream.label.Label
import scala.collection.mutable
import scala.util.Sorting._
import java.util.Date

/**
  * 驻留时长标签
  * Created by tsingfu on 15/9/15.
  */
class LocationStayRule extends Label {

  // 获取配置 用户设定的各业务连续停留的时间坎(排序升序)
  lazy val stayTimeOrderList = conf.get(LabelConstant.STAY_LIMITS).split(LabelConstant.ITME_SPLIT_MARK).map(_.trim.toLong).sorted
  // 推送满足设置的数据坎的最大值:true;最小值：false
  lazy val userDefPushOrde = conf.getBoolean(LabelConstant.STAY_MATCHMAX, true)
  // 推送满足设置的数据的限定值，还是真实的累计值.真实的累计值:false;限定值:true
  lazy val pushLimitValue = conf.getBoolean(LabelConstant.STAY_OUTPUTTHRESHOLD, true)
  // 无效数据阈值的设定
  lazy val thresholdValue = conf.getLong(LabelConstant.STAY_TIMEOUT, LabelConstant.DEFAULT_TIMEOUT_VALUE)

  val time_sine = "time"
  //信令字段表示开始时间
  val dateformat = "yyyy-MM-dd HH:mm:ss.SSS"
  //信令字段开始时间的格式
  val type_sine = "stay_"

  override def attachLabel(line: Map[String, String], cache: StreamingCache, labelQryData: mutable.Map[String, mutable.Map[String, String]]): (Map[String, String], StreamingCache) = {

    val now = new Date()
    // println("---------开始执行驻留时长标签操作---------" + now)
    val cacheInstance = if (cache == null) new LabelProps else cache.asInstanceOf[LabelProps]

    // cache中各区域的属性map
    val cacheImmutableMap = cacheInstance.labelsPropList
    // map属性转换
    //println("---cacheImmutableMap:" + cacheImmutableMap)
    val cacheMutableMap = transformCacheMap2mutableMap(cacheImmutableMap)
    //println("---cacheMutableMap:" + cacheMutableMap)

    // mcsource 打标签用[初始化标签值]
    val initMap = fieldsMap()
    // 只对当前信令所在区域打驻留标签,用户定义的非信令所在区域用黙认值“0”代替
    val mcStayLabelsMap = initMap.map(kv => (kv._1, LabelConstant.LABEL_STAY_DEFAULT_TIME))

    // 画面定义驻留标签字段列表stay_task1
    val stadyLabelKey = initMap.keys.toList
    // 从已经部分增强的信令中找出所有区域，并过滤出要求打驻留标签的区域
    // 取在siteRule（区域规则）中所打的area标签list:area_task1,area_task2..
    val locations = line.filterKeys(_.startsWith("area_")).filter(area => (stadyLabelKey.contains("stay_" + (area._1.trim).substring(5)) && area._2 == "true"))

    // 去除前缀"stay_"，的区域列表,如：task1,task2..
    val locationList = locations.keys.map(key => (key.substring(5)).trim).toList

    // cache中所有区域的最大lastTime
    val cacheMaxLastTime = getCacheMaxLastTime(cacheMutableMap)

    //println("-----cacheMaxLastTime:" + cacheMaxLastTime)
    //println("-----locationList.size:" + locationList.size)
    //println("-----stay.timeout:" + thresholdValue)

    if (locationList.size > 0) {
      val timeMs = DateFormatUtils.dateStr2Ms(line(time_sine), dateformat) //信令数据产生的时间戳
      //println("-----timeMs:" + timeMs)

      locationList.map(location => {
        //println("-------location-------" + location)
        // A.此用的所有区域在cache中的信息已经过期视为无效，标签打为“0”；重新设定cache;(5小时，默认30分钟)
        if (timeMs - cacheMaxLastTime > thresholdValue) {
          //println("-----timeMs - cacheMaxLastTime > thresholdValue，放入缓存")

          // 1. 连续停留标签置“0”
          mcStayLabelsMap += ((type_sine + location) -> LabelConstant.LABEL_STAY_TIME_ZERO)
          // 2. 清除cache信息
          cacheMutableMap.clear()
          // 3. 重置cache信息
          val cacheStayLabelsMap = mutable.Map[String, String]()
          cacheStayLabelsMap += (LabelConstant.LABEL_STAY_FIRSTTIME -> timeMs.toString)
          cacheStayLabelsMap += (LabelConstant.LABEL_STAY_LASTTIME -> timeMs.toString)
          // 去掉前缀stay_,addCacheAreaStayTime方法设置的key为location不带前缀
          // cacheMutableMap += ((type_sine + location) -> cacheStayLabelsMap)
          cacheMutableMap += (location -> cacheStayLabelsMap)

          //println("缓存cacheMutableMap=" + cacheMutableMap)
        } else if (cacheMaxLastTime - timeMs > thresholdValue) {
          //println("-----cacheMaxLastTime - timeMs > thresholdValue，延迟到达视为无效")

          // 此条数据为延迟到达的数据，已超过阈值视为无效，标签打为“0”；cache不做设定;
          mcStayLabelsMap += ((type_sine + location) -> LabelConstant.LABEL_STAY_TIME_ZERO)
        } else {
          //C.此条数据时间为[(maxLastTime-thresholdValue)~maxLastTime~(maxLastTime+thresholdValue)]之间
          labelAction(location, cacheMutableMap, mcStayLabelsMap, timeMs)
        }
      })
    }

    // c. 给mcsoruce设定连续停留[LABEL_STAY]标签
    mcStayLabelsMap ++= line

    //3更新缓存
    // map属性转换
    cacheInstance.labelsPropList = transformCacheMap2ImmutableMap(cacheMutableMap)
    (mcStayLabelsMap.toMap, cacheInstance)
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

  /**
    * 从cache的区域List中取出最大的lastTime<br>
    */
  private def getCacheMaxLastTime(labelsPropMap: mutable.Map[String, mutable.Map[String, String]]): Long = {
    val areaPropArray = labelsPropMap.toArray

    if (areaPropArray.isEmpty) {
      //println("------areaPropArray.isEmpty------")
      0L
    } else {
      //println("------areaPropArray.is not empty------:" + areaPropArray.toString)
      // 对用户cache中的区域列表按lastTime排序（升序）
      quickSort(areaPropArray)(Ordering.by(_._2.get(LabelConstant.LABEL_STAY_LASTTIME)))
      // 取用户cache区域列表中的最大lastTime
      areaPropArray.reverse(0)._2.get(LabelConstant.LABEL_STAY_LASTTIME).get.toLong
    }
  }

  /**
    * 打标签处理并且更新cache
    *
    * @param location        ：当前要计算驻留时长的位置，如：task1，task2..
    * @param labelsPropMap   ：codis缓存的数据firttime，lasttime
    * @param mcStayLabelsMap ：要打驻留时长标签的字段，如：stay_task1，stay_task2
    * @param mcTime          ：当前信令数据的开始时间
    */
  private def labelAction(location: String,
                          labelsPropMap: mutable.Map[String, mutable.Map[String, String]],
                          mcStayLabelsMap: mutable.Map[String, String],
                          mcTime: Long) {

    //println("location:" + location + ",mcTime:" + mcTime)
    // 使用宽松的过滤策略，相同区域信令如果间隔超过${thresholdValue}，则判定为不连续
    val area = labelsPropMap.get(location)
    area match {
      case None => {
        //println("------labelsPropMap.get(location):case None")
        mcStayLabelsMap += ((type_sine + location) -> LabelConstant.LABEL_STAY_TIME_ZERO)
        addCacheAreaStayTime(labelsPropMap, location, mcTime, mcTime)
      }
      case Some(currentStatus) => {
        //取出缓存的firstTime和lastTime
        val first = getCacheStayTime(currentStatus).get("first").get
        val last = getCacheStayTime(currentStatus).get("last").get
        //println("----FIRST TIME : " + first + " , LAST TIME : " + last + " , MCTIME : " + mcTime)

        if (first > last) {
          // 无效数据，丢弃，本条视为first
          mcStayLabelsMap += ((type_sine + location) -> LabelConstant.LABEL_STAY_DEFAULT_TIME)
          updateCacheStayTime(currentStatus, mcTime, mcTime)
        } else if (mcTime < first) {
          // 本条记录属于延迟到达，更新开始时间
          if (first - mcTime > thresholdValue) {
            // 原则上这种情况不存在，mctime< maxlastTime + threshold 的数据都没有
            // 本条记录无效，输出空标签，不更新cache
            mcStayLabelsMap += ((type_sine + location) -> LabelConstant.LABEL_STAY_TIME_ZERO)
          } else {
            // 本条记录属于延迟到达，更新开始时间
            currentStatus.put(LabelConstant.LABEL_STAY_FIRSTTIME, mcTime.toString)
            mcStayLabelsMap.put((type_sine + location), evaluateTimeForLnyd(last - first, last - mcTime))
          }
        } else if (mcTime <= last) {
          // 本条属于延迟到达，不处理
          // mcStayLabelsMap += ((type_sine + location) -> LabelConstant.LABEL_STAY_DEFAULT_TIME)
          mcStayLabelsMap += ((type_sine + location) -> evaluateTimeForLnyd(last - first, mcTime - first))
        } else if (mcTime - last > thresholdValue) {
          // 本条与上一条数据间隔过大，判定为不连续
          mcStayLabelsMap += ((type_sine + location) -> LabelConstant.LABEL_STAY_DEFAULT_TIME)
          updateCacheStayTime(currentStatus, mcTime, mcTime)
        } else {
          // 本条为正常新数据，更新cache后判定
          currentStatus.put(LabelConstant.LABEL_STAY_LASTTIME, mcTime.toString)

          val newtime = evaluateTimeForLnyd(last - first, mcTime - first)
          mcStayLabelsMap.put((type_sine + location), newtime)
          //println("------newtime:" + newtime)

        }
      }
    }
  }

  /**
    * 辽宁移动计算驻留时长
    *
    * @param oldStayTime
    * @param newStayTime
    * @return
    */
  private def evaluateTimeForLnyd(oldStayTime: Long, newStayTime: Long): String = {
    newStayTime.toString
  }

  /**
    * 取cache中的firstTime,lastTime<br>
    * 返回结果map,key:"first"和"last"<br>
    */
  private def getCacheStayTime(currentStatus: mutable.Map[String, String]) = {
    val first = currentStatus.get(LabelConstant.LABEL_STAY_FIRSTTIME)
    val last = currentStatus.get(LabelConstant.LABEL_STAY_LASTTIME)
    if (first == None || last == None) mutable.Map("first" -> 0L, "last" -> 0L)
    else mutable.Map("first" -> first.get.toLong, "last" -> last.get.toLong)
  }

  /**
    *
    * 在cache中追加新的区域属性map并设值<br>
    *
    * @param labelsPropMap ：catche缓存数据firsttime，lasttime
    * @param location      ：要计算驻留时长的区域：task1，task2..
    * @param firstTime
    * @param lastTime
    */
  private def addCacheAreaStayTime(labelsPropMap: mutable.Map[String, mutable.Map[String, String]],
                                   location: String,
                                   firstTime: Long,
                                   lastTime: Long) {
    val map = mutable.Map[String, String]()
    updateCacheStayTime(map, firstTime, lastTime)
    labelsPropMap += (location -> map) //task1->("firsttime":111,"lasttime":222)
  }

  /**
    * 更新cache中区域属性map的firstTime,lastTime值<br>
    */
  private def updateCacheStayTime(map: mutable.Map[String, String],
                                  firstTime: Long,
                                  lastTime: Long) {
    map += (LabelConstant.LABEL_STAY_FIRSTTIME -> firstTime.toString)
    map += (LabelConstant.LABEL_STAY_LASTTIME -> lastTime.toString)
  }

  /**
    * 根据本次以及前次的停留时间计算出标签停留时间的值
    *
    * @param oldStayTime 上次的驻留时长
    * @param newStayTime 本次的驻留时长
    * @return
    */
  private def evaluateTime(oldStayTime: Long, newStayTime: Long): String = {
    // 新状态未达到最小坎时或旧状态超过最大坎值时返回黙认值“0”
    if (newStayTime < stayTimeOrderList(0) ||
      oldStayTime > stayTimeOrderList(stayTimeOrderList.size - 1)) {
      LabelConstant.LABEL_STAY_DEFAULT_TIME
    } else {
      val matchList = stayTimeOrderList.filter(limit => (oldStayTime <= limit && newStayTime >= limit))
      // 新旧停留时间在某坎区间内，返回黙认值“0”
      if (matchList.isEmpty) LabelConstant.LABEL_STAY_DEFAULT_TIME
      // 新旧停留时间为跨坎区域时间，推送设置的数据坎的值
      else if (pushLimitValue) {
        val result = if (userDefPushOrde) matchList.map(_.toLong).max else matchList.map(_.toLong).min
        result.toString
      } // 推送真实数据值
      else newStayTime.toString
    }
  }

}
