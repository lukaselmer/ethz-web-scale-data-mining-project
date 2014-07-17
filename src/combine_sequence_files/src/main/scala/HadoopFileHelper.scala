import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{Path, FileSystem}

import scala.collection.mutable

object HadoopFileHelper {
  def listHdfsDirs(path: Path): List[String] = {
    val fs = FileSystem.get(new Configuration())
    val i = fs.listStatus(path)
    val arr = mutable.ListBuffer.empty[String]
    for (o <- i.iterator)
      if (o.isDirectory)
        arr += o.getPath.toString
    arr.toList
  }

  def listHdfsFiles(path: Path): List[String] = {
    val fs = FileSystem.get(new Configuration())
    val i = fs.listFiles(path, true)
    val arr = mutable.ListBuffer.empty[String]
    while (i.hasNext)
      arr += i.next.getPath.toString
    arr.toList
  }
}
