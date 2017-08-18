/**
  * Created by yang on 2017/5/18.
  */

import com.asiainfo.ocdp.stream.common.SimpleLabelProps
import com.asiainfo.ocdp.stream.label.LastLacCiLabelStayRule
import com.asiainfo.ocdp.stream.config.LabelConf

object LabelTest {
  def main(args: Array[String]) {
    val codisLableCache=new SimpleLabelProps
    val cacheMap=Map("last_lac"->"12333","last_ci"->"35353","llc_first_dt"->"2017-05-10 12:00:00")
    codisLableCache.labelsPropList=cacheMap

    val lineMap=Map("lac"->"12333","ci"->"35353","time"->"2017-05-10 11:05:00")
    val labelFields=List("last_lac","last_ci","llc_first_dt","llc_end_dt")
    val cfg=new LabelConf()
    cfg.setFields(labelFields)
    val a=new LastLacCiLabelStayRule
    a.init(cfg)

    val b=a.attachLabel(lineMap,codisLableCache,null)
    //println(b._1)
//    println(b._2)

//    val abc="1324,55,1121,1,111,101".split(",")
//    println(abc.contains("2"))

//    print("a"=="a")
//    val abc="2333";
//    val bcd="2333"
//    println(abc==bcd)
  }
}
