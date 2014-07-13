import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

import scala.collection.mutable

object HadoopFileHelper {
  def listHdfsFiles(hdfsPath: Path): List[String] = {
    val fs = FileSystem.get(new Configuration())
    val i = fs.listFiles(hdfsPath, true)
    val arr = mutable.ListBuffer.empty[String]
    while (i.hasNext)
      arr += i.next.getPath.toString
    arr.toList
  }
}
