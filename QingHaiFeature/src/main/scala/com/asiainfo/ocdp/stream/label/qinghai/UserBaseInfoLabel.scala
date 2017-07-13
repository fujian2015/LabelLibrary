package com.asiainfo.ocdp.stream.label.qinghai

import com.asiainfo.ocdp.stream.common.StreamingCache
import com.asiainfo.ocdp.stream.label.Label
import org.slf4j.LoggerFactory

import scala.collection.mutable

/**
  * 用户标签，查询codis中用户标签并增加到原始数据中
  */
class UserBaseInfoRule extends Label {
  val logger = LoggerFactory.getLogger(this.getClass)

  val imsiFiledName = "imsi"
  //优先对哪个imsi进行打标签，需与现场配置的数据源接口字段一致
  val label_props_pname = "user_info_cols"
  //标签中定义的props的字段名称，需与配置现场一致。如："props":[{"pname":"user_info_cols","pvalue":"product_no,city_id"}]
  val codis_key_prefix = "userinfo:"
  //codis中用户信息查询key的前缀
  val field_is_local = "islocal" //定义是否为省外用户

  override def attachLabel(line: Map[String, String], cache: StreamingCache, labelQryData: mutable.Map[String, mutable.Map[String, String]]): (Map[String, String], StreamingCache) = {

    //初始化用户附加标签为空
    val labelMap = fieldsMap()
    labelMap += (field_is_local -> "") //默认省内用户

    //用户标签字段
    val info_cols = conf.get(label_props_pname).split(",")

    //从codis获取缓存的用户信息,userinfo:4600000**
    val qryKey = getQryKeys(line)

    if (qryKey.size == 1) {
      val cachedUser = labelQryData.getOrElse(qryKey.head, Map[String, String]())
      //println("1.cachedUser:" + cachedUser)
      if (cachedUser.isEmpty) {
        //如果查询不到imsi,不做处理
        labelMap.update(field_is_local, "true") //省外用户
      } else {
        //println("3.info_cols:" + info_cols)
        info_cols.foreach(labelName => {
          val labelValue = cachedUser.getOrElse(labelName, "")
          if (!labelValue.isEmpty) {
            labelMap += (labelName -> labelValue)
          }
        })
      }

    }

    labelMap ++= line
    (labelMap.toMap, cache)

  }

  override def getQryKeys(line: Map[String, String]): Set[String] =
    Set[String](line(imsiFiledName)).
      filterNot(value => {
        value == null || value == "000000000000000"
      }).map(codis_key_prefix + _)

}