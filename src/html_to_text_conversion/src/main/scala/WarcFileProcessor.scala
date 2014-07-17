import java.io.{InputStream, InputStreamReader}

import de.l3s.boilerpipe.extractors.ArticleExtractor
import org.apache.hadoop.io.Text
import org.apache.log4j.Logger
import org.jwat.warc.{WarcReaderFactory, WarcRecord}

import scala.collection.JavaConversions._

class WarcFileProcessor(val contentStream: InputStream, val logger: Logger) extends Traversable[(Text, Text)] {
  private var textLen: Integer = 0

  override def foreach[U](f: ((Text, Text)) => U): Unit = {
    val reader = WarcReaderFactory.getReader(contentStream)
    for (record: WarcRecord <- reader.iterator()) {
      if (record.getHeader("WARC-Type").value == "response") {
        val id = record.getHeader("WARC-TREC-ID").value
        val htmlStream = record.getPayloadContent()
        try {
          val text = extractText(htmlStream)
          textLen = text.length
          f(new Text(id), new Text(text + "\n"))
        } catch {
          case e: Exception => logger.error("Exception processing record: " + id, e)
          case e: StackOverflowError => logger.error("StackOverflowError processing record: " + id, e)
        }
      }
    }
  }

  def textLength(): Integer = {
    textLen
  }
  def extractText(stream: InputStream): String = {
    // TODO: Add some more content, e.g. <meta>-Tag data
    val text = ArticleExtractor.INSTANCE.getText(new InputStreamReader(stream))
    TextProcessor.cleanString(text)
  }
}
