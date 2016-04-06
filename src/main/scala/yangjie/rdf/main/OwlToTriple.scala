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
    println(fPath)
    val rdfPrefix = "http://www.w3.org/1999/02/22-rdf-syntax-ns#"
    def textFunc(chd:Node) = chd.label
    def resourceFunc(chd:Node) = {
      val attr = chd.attribute(rdfPrefix, "resource")
      attr match {
      case None => "resourceDebug:"+chd.text
      case _ => attr.get.mkString
      }
    }
    def childFunc(chd:Node) = {
      val attr = chd.child.head.attribute(rdfPrefix, "about")
      attr match {
        case None => "childDebug:"+chd.text
        case _ => attr.get.mkString
      }
    }
    def aboutFunc(node:Node) = {
      val attr = node.attribute(rdfPrefix,"about")
      attr match {
        case None => "aboutDebug:"+node.text
        case _ => attr.get.mkString
      }
    }

    var rdd = sc.wholeTextFiles(fPath)
    rdd = rdd.flatMap{case (key,doc) => {
      val xml = XML.loadString(doc)
      val entityNode = (xml \ "_").filter(x => x.prefix == "ub" && (x \ s"@{${rdfPrefix}}about").length == 1)
      entityNode.flatMap(node => {
        (node \ "_").map(chd => {
          val uri = (chd \ "_").isEmpty match {
            case false => childFunc(chd)
            case _ => {
              (chd \ s"@{${rdfPrefix}}resource").length match {
                case 0 => textFunc(chd)
                case _ => resourceFunc(chd)
              }
            }
          }
          (aboutFunc(node),s"${chd.label},${uri}")
        })
      })
    }}
    val outPath = "/user/yangjiecloud/SparkRdf/triple"
    println(outPath)
    println(IoHelper.deleteFileInHDFS(outPath))
    rdd.saveAsTextFile(outPath)
    sc.stop()
    println("over")
  }
}