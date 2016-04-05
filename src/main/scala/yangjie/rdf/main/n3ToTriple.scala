package yangjie.rdf.main

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by yangjiecloud on 2016/4/5.
  */
object n3ToTriple {
  def main(args:Array[String]): Unit = {
    val conf = new SparkConf()
    val sc = new SparkContext(conf)

    val fPath = "/user/yangjiecloud/SparkRdf/n3"
    var rdd = sc.wholeTextFiles(fPath)
    rdd = rdd.mapValues(doc => {
      doc
    })
  }
}
