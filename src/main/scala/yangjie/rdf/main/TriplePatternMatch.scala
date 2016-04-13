package yangjie.rdf.main

import org.apache.jena.graph.{Node, Node_URI, Node_Variable}
import org.apache.jena.query.QueryFactory
import org.apache.jena.sparql.core.TriplePath
import org.apache.jena.sparql.syntax.{Element, ElementPathBlock, ElementVisitorBase, ElementWalker}
import org.apache.spark.{SparkConf, SparkContext}
import yangjie.rdf.utils.IoHelper

import scala.util.control.Breaks._
import scala.collection.mutable.ListBuffer

/**
  * Created by yangjiecloud on 2016/4/11.
  */
object TriplePatternMatch {
  def getPatternType(spo:TriplePath): String = {
    val v = classOf[Node_URI]
//    println(spo.getSubject.getClass)
//    println(spo.getPredicate.getClass)
//    println(spo.getObject.getClass)
    println(spo.getSubject.toString())
    val pType = (
      spo.getSubject.getClass == v match {
        case true => "1"
        case _ => "0"
      },
      spo.getPredicate.getClass == v match {
        case true => "1"
        case _ => "0"
      },
      spo.getObject.getClass == v match {
        case true => "1"
        case _ => "0"
      })
    pType.productIterator.mkString
  }

  def matchBySpark(spo: TriplePath): Unit = {
    val conf = new SparkConf()
    conf.setAppName("search by triple pattern")
    val sc = new SparkContext(conf)

    // 检查并生成 pattern type
    val patternType = getPatternType(spo)
    println(patternType)
    // 检索spark
    var rdd = sc.textFile("/user/yangjiecloud/SparkRdf/triple").filter{line => {
      val spoArr = line.split('\t')
      val patternType = ""
      patternType match {
        case "100" => spo.getSubject.getURI == spoArr(0)
        case "010" => spo.getPredicate.getURI == spoArr(1)
        case "001" => spo.getObject.getURI == spoArr(2)
        case _ => false
      }
    }}

    val outPath = "/user/yangjiecloud/SparkRdf/tripleMatchResult"
    println(outPath)
    println(IoHelper.deleteFileInHDFS(outPath))
    rdd.saveAsTextFile(outPath)
    sc.stop()
  }

  def getSpoFromPattern(pattern : Element): TriplePath = {
    var path:TriplePath = null
    ElementWalker.walk(pattern,new ElementVisitorBase() {
      override def visit(el:ElementPathBlock): Unit = {
        val triples = el.patternElts()
        breakable {
          while (triples.hasNext) {
            path = triples.next()
            break
          }
        }
      }
    })
    if (path == null)
      throw new Exception(s"get spo from pattern fail")
    return path
  }

  def main(args:Array[String]): Unit = {
    // 创建query
    val query = QueryFactory.create("select ?Z {<http://www.Department0.University0.edu> ?Z <http://www.University0.edu>}")
    // 获取pattern
    val pattern = query.getQueryPattern()
    // 获取spo
    val spo = getSpoFromPattern(pattern)
    // spark查spo
    matchBySpark(spo)
    println("ok")
  }
}
