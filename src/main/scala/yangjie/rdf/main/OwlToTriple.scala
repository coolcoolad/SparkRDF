package yangjie.rdf.main

import org.apache.spark.{SparkConf, SparkContext}
import yangjie.rdf.utils.IoHelper
import scala.xml.XML

/**
  * Created by yangjiecloud on 2016/4/5.
  */
object OwlToTriple {
  def main(args:Array[String]): Unit = {
    val conf = new SparkConf()
    val sc = new SparkContext(conf)

    val fPath = "/user/yangjiecloud/SparkRdf/owl"
    var rdd = sc.wholeTextFiles(fPath)
    rdd = rdd.flatMapValues(doc => {
      val xml = XML.loadString(doc)
      val entityNode = (xml \\ "_").filter(x => x.prefix == "ub" && x.attribute("http://www.w3.org/1999/02/22-rdf-syntax-ns#", "about") != None)
      val entityKv = entityNode.map(x => (x.attribute("http://www.w3.org/1999/02/22-rdf-syntax-ns#", "about"), x.label))
      entityKv.map(kv => {
        val key = kv._1 match {
          case Some(x) => x
          case None => "null"
        }
        s"${key},${kv._2}"
      })
    })
    val outPath = "/user/yangjiecloud/SparkRdf/triple"
    println(IoHelper.deleteFileInHDFS(fPath))
    rdd.saveAsTextFile(fPath)
    sc.stop()
    println("over")
  }
}