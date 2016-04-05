package yangjie.rdf.utils

import org.apache.hadoop.fs.{FileSystem, Path, PathFilter}

/**
  * Created by yangjiecloud on 2016/4/5.
  */
object IoHelper {
  val hadoopConf = new org.apache.hadoop.conf.Configuration()
  val hdfs = FileSystem.get(hadoopConf)

  def deleteFileInHDFS(filePath: String): Boolean = {
    hdfs.delete(new Path(filePath), true)
  }
}
