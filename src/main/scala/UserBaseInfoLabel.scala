import com.asiainfo.ocdp.stream.common.StreamingCache
import com.asiainfo.ocdp.stream.constant.LabelConstant
import com.asiainfo.ocdp.stream.label.Label
import org.slf4j.LoggerFactory

import scala.collection.mutable

/**
  * Created by gengwang on 16/8/11.
  */
class UserBaseInfoLabel  extends Label{
  val logger = LoggerFactory.getLogger(this.getClass)
  //城市,省标签前缀
  val city_sine = "city_name"
  val province_sine = "province"
  val areaPathKey = "area_path"

  override def attachLabel(line: Map[String, String], cache: StreamingCache, labelQryData: mutable.Map[String, mutable.Map[String, String]]): (Map[String, String], StreamingCache) = {

    val labelMap = fieldsMap()

    val info_cols = conf.get("user_info_cols").split(",")

    // add label with empty string if the user does not exist
    info_cols.foreach(labelName => {
      labelMap +=  (labelName -> "")
    })

    val cachedUser = labelQryData.getOrElse("user_base_info:" + line("imsi"), Map[String, String]())
    if (cachedUser.isEmpty) {
      //如果查询不到user imsi, 则查询city_info信息,得到imsi码段的归属城市
      var cachedCity : mutable.Map[String,String] = null
      //若imsi前三位为"460",则查询imsi的号段为前10位,否则为前3位
      if (line("imsi").substring(0,3).equals("460")){
        cachedCity = labelQryData.getOrElse("city_info:" + line("imsi").substring(0, 10), mutable.Map[String, String]())
      }
      else{
        cachedCity = labelQryData.getOrElse("city_info:" + line("imsi").substring(0, 3), mutable.Map[String, String]())
      }
      if (cachedCity.contains(city_sine)) {
        val city = cachedCity(city_sine)
        labelMap += (LabelConstant.LABEL_CITY -> city)
      }
      else{
        labelMap += (LabelConstant.LABEL_CITY -> "")

      }
    }
    else{
      info_cols.foreach(labelName => {
        val labelValue = cachedUser.getOrElse(labelName, "")
        if (!labelValue.isEmpty){
          labelMap += (labelName -> labelValue)
        }
      })
      //如果能查到user imsi,则将归属地设为陕西省
      labelMap += (LabelConstant.LABEL_CITY -> "Shaanxi")
    }

    labelMap ++= line

    (labelMap.toMap, cache)

  }

  override def getQryKeys(line: Map[String, String]): Set[String] = {
    //检查imsi前三位,若为'460'表示为国内imsi,则查询city_info时使用imsi前10位作为号段,
    //若为国外imsi,则查询city_info时使用imsi前3位作为号段
    if (line("imsi").substring(0,3).equals("460")){
      Set[String]("user_base_info:" + line("imsi"), "city_info:" + line("imsi").substring(0,10))
    }
    else {
      Set[String]("user_base_info:" + line("imsi"), "city_info:" + line("imsi").substring(0,3))
    }
  }

}
