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

    def textFunc(chd:Node) = chd.text
    def resourceFunc(chd:Node) = {
      val attr = chd \ s"@{${rdfPrefix}}resource"
      attr.length match {
      case 0 => "resourceDebug:"+chd.text
      case _ => attr.head.mkString
      }
    }
    def childFunc(chd:Node) = {
      val attr = chd \ s"@{${rdfPrefix}}about"
      attr.length match {
        case 0 => "childDebug:"+chd.text
        case _ => attr.head.mkString
      }
    }
    def aboutFunc(node:Node) = {
      val attr = node \ s"@{${rdfPrefix}}about"
      attr.length match {
        case 0 => "aboutDebug:"+node.text
        case _ => attr.head.mkString
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