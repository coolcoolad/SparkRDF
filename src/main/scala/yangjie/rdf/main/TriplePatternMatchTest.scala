package yangjie.rdf.main

import org.apache.jena.graph.Node_URI
import org.apache.jena.query.QueryFactory
import org.apache.jena.sparql.core.TriplePath
import org.apache.jena.sparql.syntax.{Element, ElementPathBlock, ElementVisitorBase, ElementWalker}
import org.apache.spark.{SparkConf, SparkContext}
import yangjie.rdf.utils.IoHelper

import scala.util.control.Breaks._

/**
  * Created by yangjiecloud on 2016/4/13.
  */
object TriplePatternMatchTest {
  def main(args:Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setAppName("search by triple pattern")
    val sc = new SparkContext(conf)

    val rdd = sc.textFile("/user/yangjiecloud/SparkRdf/triple").map(line => line.split('\t')).cache()
    val rdd1 = rdd.filter(spoArr => {
      "name" == spoArr(1) && "GraduateStudent0" == spoArr(2)
    }).map(x => (x(0) -> x.mkString("\t"))).cache()
    val rdd2 = rdd.filter(spoArr => {
      "takesCourse" == spoArr(1) && "http://www.Department0.University0.edu/GraduateCourse0" == spoArr(2)
    }).map(x => (x(0) -> x.mkString("\t"))).cache()

    val outPath = "/user/yangjiecloud/SparkRdf/joinMatchResult"
    println(outPath)
    println(IoHelper.deleteFileInHDFS(outPath))
    rdd1.join(rdd2).map(_._2).saveAsTextFile(outPath)
    sc.stop()
    println("ok")
  }
}
