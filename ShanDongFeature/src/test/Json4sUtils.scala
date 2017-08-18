import org.json4s.DefaultFormats
import org.json4s.JsonAST.JString
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._

import scala.collection.mutable.ArrayBuffer

/**
  * Created by tsingfu on 15/8/18.
  */
object Json4sUtils {

  def main(args: Array[String]): Unit = {
    val map = Map("imsi_aes" -> "+3dgvujgnUQskIeIvVjfpA==", "imsi_md5" -> "bbbbb", "ccc" -> "ccc")
    val str = map2JsonStr(map)
    val array = Array("imsi_aes", "imsi_md5")
    //println(str)
    val result = jsonStr2ArrTuple2(str, array)
    //println(result)
  }

  /**
    * 从 json格式的字符串中获取指定属性的取值
    *
    * @param jsonStr
    * @param fields
    * @return
    */
  def jsonStr2ArrTuple2(jsonStr: String, fields: Array[String]): Array[(String, String)] = {
    val result = ArrayBuffer[(String, String)]()
    for (field <- fields) {
      val jsonStr_target = compact(parse(jsonStr) \ field)
      val prop = parse(jsonStr_target) match {
        case JString(str) => (field, str)
        case _ => (field, null)
      }

      if (prop._2 != null) result.append(prop)
    }
    result.toArray
  }

  def map2JsonStr(jsonMap: Map[String, String]): String = {
    compact(render(jsonMap))
  }

  /**
    *
    * @param jsonStr input string with json format
    * @param fields  Json fields name
    * @return output string concatnated by comma
    */
  def jsonStr2String(jsonStr: String, fields: Array[String], delim: String): String =
    jsonStr2ArrTuple2(jsonStr, fields).map(tuple => tuple._2).mkString(delim)

  def jsonStr2Map(jsonStr: String): Map[String, String] = {
    implicit val formats = DefaultFormats
    parse(jsonStr).extract[Map[String, String]]
  }

}
