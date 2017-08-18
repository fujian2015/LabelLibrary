/**
  * Created by yang on 2017/5/18.
  */

import java.text.SimpleDateFormat
import java.util.Date
import java.sql.Time
import java.sql.Timestamp

import com.asiainfo.ocdp.stream.common.LabelProps
import com.asiainfo.ocdp.stream.config.LabelConf
import com.asiainfo.ocdp.stream.label.examples.AccumulateLabel
import com.asiainfo.ocdp.stream.label.examples.AreaLabel
import com.asiainfo.ocdp.stream.label.examples.UserBaseInfoLabel

import scala.collection.mutable

object LabelTestYn {
  def main(args: Array[String]) {
//    val ma=Map("a"->"b")
//    val mb=Map[String, String]()
//    println(ma.isEmpty)
//    println("bb="+mb.isEmpty)
//
//    val a=Set("000")
//    println(a.head)
//    val d=new Date()
//    val df=new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
//
//    println("UTC时间格式="+d.getTime)
//    println("北京时间格式="+d.toLocaleString)
//    println("GMT时间格式"+d.toGMTString)
//    println(""+df.format(d))

    //testAccumulateLabel()
    testUserBaseInfoLabel()
  }
  //景区人数统计标签
  def testAccumulateLabel(): Unit ={
    //codis历史缓存数据
    val codisLableCache=new LabelProps
    val cacheMap:Map[String, Map[String, String]]=Map("123456"->Map("12333"->"1498660800000","region_code2"->"1497660800247","region_code3"->"1498633823280"))
    codisLableCache.labelsPropList=cacheMap

    //codis维表数据
    val labelQryData: mutable.Map[String, mutable.Map[String, String]]=mutable.Map("area_info:12333_35353"->mutable.Map("region_type"->"1","region_type_name"->"222","city_code"->"0531"))
    //信令数据
    val lineMap=Map("imsi"->"123456","region_code"->"","procedure_start_time"->"1498660800000")

    //标签json配置
    val labelFields=List("isAccu_area")
    val cfg=new LabelConf()
    cfg.set("interval","300")
    cfg.setFields(labelFields)

    val a=new AccumulateLabel
    a.init(cfg)

    //标签测试代码
    val b=a.attachLabel(lineMap,codisLableCache,labelQryData)
    val newCache=b._2.asInstanceOf[LabelProps]
    println("new Data="+b._1)
    println("new cache="+newCache.labelsPropList)
  }

  //用户标签测试
  def testUserBaseInfoLabel(): Unit ={
    //codis历史缓存数据
//    val codisLableCache=new LabelProps
//    val cacheMap:Map[String, Map[String, String]]=Map("123456"->Map("12333"->"1498660800000","region_code2"->"1497660800247","region_code3"->"1498633823280"))
//    codisLableCache.labelsPropList=cacheMap

    //codis维表数据
    val labelQryData: mutable.Map[String, mutable.Map[String, String]]=mutable.Map(
        "user_base_info:123456"->mutable.Map("imsi"->"123456","msisdn"->"13853186257"),
        "phone_seg_info:1385318" ->mutable.Map("user_province_name"->"531","user_city_name"->"0531")
    )

    //信令数据
    val lineMap=Map("imsi"->"123456","msisdn"->"","procedure_start_time"->"1498660800000")

    //标签json配置
    val cfg=new LabelConf()
    cfg.set("default_province_name","云南省")
    cfg.set("user_location_cols","user_province_name,user_city_name")
    val labelFields=List("user_province_name","user_city_name","user_province_flag","sex","age_level","arpu_level")
    cfg.setFields(labelFields)

    val a=new UserBaseInfoLabel
    a.init(cfg)

    //标签测试代码
    val b=a.attachLabel(lineMap,null,labelQryData)
    val newCache=b._2.asInstanceOf[LabelProps]
    println("new Data="+b._1)
    //println("new cache="+newCache.labelsPropList)
  }

  //区域标签测试
  def testAreaLabel(): Unit ={
    //codis历史缓存数据
    val codisLableCache=new LabelProps
//    val cacheMap=Map("imsiId"->Map("region_code1"->"12333","last_ci"->"35353","llc_first_dt"->"2017-05-10 12:00:00"))
//    codisLableCache.labelsPropList=cacheMap

    //codis维表数据
    val labelQryData: mutable.Map[String, mutable.Map[String, String]]=mutable.Map("area_info:12333_35353"->mutable.Map("region_type"->"1","region_type_name"->"222","city_code"->"0531"))
    //信令数据
    val lineMap=Map("tac"->"12333","cell_id"->"35353","time"->"2017-05-10 11:05:00")

    //标签json配置
    val labelFields=List("region_type","region_type_name","city_code","longitude")
    val cfg=new LabelConf()
    cfg.setFields(labelFields)
    val a=new AreaLabel

    a.init(cfg)

    //标签测试代码
    val b=a.attachLabel(lineMap,null,labelQryData)
    println("new data="+b._1)
    println("new cache="+b._2)
  }
}
