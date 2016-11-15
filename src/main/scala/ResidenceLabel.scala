import com.asiainfo.ocdp.stream.common.StreamingCache
import com.asiainfo.ocdp.stream.label.Label

import scala.collection.mutable

/**
  * Created by gengwang on 16/10/21.
  */
class ResidenceLabel  extends Label{

  val isResidence_sine="isResidence"

  override def attachLabel(line: Map[String, String], cache: StreamingCache, labelQryData: mutable.Map[String, mutable.Map[String, String]]): (Map[String, String], StreamingCache) = {

    val labelMap = fieldsMap()

    val lac_cell  = line("lac") + "_" + line("cell")

    //判断该用户是否为该LAC CELL下的常住用户
    val residence_info = labelQryData.getOrElse("residence_info:" + line("imsi"), Map[String, String]())
    if (residence_info.isEmpty){
      labelMap += (isResidence_sine -> "false")
    }
    else{
      val work_lac  = residence_info.getOrElse("work_lac", "")
      val work_cell = residence_info.getOrElse("work_cellid", "")
      val live_lac  = residence_info.getOrElse("live_lac", "")
      val live_cell = residence_info.getOrElse("live_cellid", "")

      if (lac_cell.equals(work_lac + "_" + work_cell) || lac_cell.equals(live_lac + "_" + live_cell)){
        labelMap += (isResidence_sine -> "true")
      }
      else{
        labelMap += (isResidence_sine -> "false")
      }

    }

    labelMap ++= line

    (labelMap.toMap, cache)

  }


  /**
    * @param line:MC信令对像
    * @return codis数据库的key
    */
  override def getQryKeys(line: Map[String, String]): Set[String] = Set[String]("residence_info:" + line("imsi"))

}
