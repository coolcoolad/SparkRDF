package yangjie.rdf.main

import org.apache.spark.{SparkConf, SparkContext}
import yangjie.rdf.utils.IoHelper
import scala.xml.{XML,Node}

/**
  * Created by yangjiecloud on 2016/4/5.
  */
object OwlToTriple {
  def main(args:Array[String]): Unit = {
    val conf = new SparkConf()
    val sc = new SparkContext(conf)

    val fPath = "/user/yangjiecloud/SparkRdf/owl"
    val rdfPrefix = "http://www.w3.org/1999/02/22-rdf-syntax-ns#"
    def textFunc(chd:Node) = chd.label
    def resourceFunc(chd:Node) = chd.attribute(rdfPrefix, "resource")
    def childFunc(chd:Node) = chd.child.head.attribute(rdfPrefix, "resource")

    var rdd = sc.wholeTextFiles(fPath)
    rdd = rdd.flatMap{case (key,doc) => {
      val xml = XML.loadString(doc)
      val entityNode = (xml \ "_").filter(x => x.prefix == "ub" && x.attribute(rdfPrefix, "about") != None)
      entityNode.flatMap(node => {
        node.child.map(chd => {
          val uri = chd.child.isEmpty match {
            case false => childFunc(chd)
            case _ => {
              chd.attribute(rdfPrefix, "resource") match {
                case null => textFunc(chd)
                case _ => resourceFunc(chd)
              }
            }
          }
          (node.label,s"${chd.label},${uri}")
        })
      })
    }}
    val outPath = "/user/yangjiecloud/SparkRdf/triple"
    println(IoHelper.deleteFileInHDFS(outPath))
    rdd.saveAsTextFile(outPath)
    sc.stop()
    println("over")
  }
}