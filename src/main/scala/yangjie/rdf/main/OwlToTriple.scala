package yangjie.rdf.main

import org.apache.spark.{SparkConf, SparkContext}
import yangjie.rdf.utils.IoHelper
import scala.xml.{XML,Node}
import scala.None

/**
  * Created by yangjiecloud on 2016/4/5.
  */
object OwlToTriple {
  def main(args:Array[String]): Unit = {
    val conf = new SparkConf()
    val sc = new SparkContext(conf)

    val fPath = "/user/yangjiecloud/SparkRdf/owl"
    val rdfPrefix = "http://www.w3.org/1999/02/22-rdf-syntax-ns#"
    val testPrefix = "debug:"
    def textFunc(chd:Node) = chd.label
    def resourceFunc(chd:Node) = {
      val attr = chd.attribute(rdfPrefix, "resource")
      attr match {
      case None => testPrefix+chd.text
      case _ => attr.get.mkString
      }
    }
    def childFunc(chd:Node) = {
      val attr = chd.child.head.attribute(rdfPrefix, "about")
      attr match {
        case None => testPrefix+chd.text
        case _ => attr.get.mkString
      }
    }
    def aboutFunc(node:Node) = {
      val attr = node.attribute(rdfPrefix,"about")
      attr match {
        case None => testPrefix+node.text
        case _ => attr.get.mkString
      }
    }

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
                case None => textFunc(chd)
                case _ => resourceFunc(chd)
              }
            }
          }
          (aboutFunc(node),s"${chd.label},${uri}")
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