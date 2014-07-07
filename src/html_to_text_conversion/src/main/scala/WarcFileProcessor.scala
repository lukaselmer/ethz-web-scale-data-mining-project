import java.io.{InputStream, InputStreamReader}
import java.text.Normalizer

import de.l3s.boilerpipe.extractors.ArticleExtractor
import org.apache.commons.io.IOUtils
import org.jwat.warc.{WarcReaderFactory, WarcRecord}

import scala.collection.JavaConversions._

class WarcFileProcessor(val content: String) extends Traversable[(String, String)] {
  override def foreach[U](f: ((String, String)) => U): Unit = {
    val reader = WarcReaderFactory.getReader(IOUtils.toInputStream(content))
    for (record: WarcRecord <- reader.iterator()) {
      if (record.getHeader("WARC-Type").value == "response") {
        val id = record.getHeader("WARC-TREC-ID").value
        val htmlStream = record.getPayloadContent()
        val text = extractText(htmlStream)
        f(id, text)
      }
    }
  }

  def extractText(stream: InputStream): String = {
    val text = ArticleExtractor.INSTANCE.getText(new InputStreamReader(stream))
    cleanString(text)
  }

  def cleanString(text: String): String = {
    new String(Normalizer.normalize(text.replaceAll("’|'|`|´|\"", ""), Normalizer.Form.NFKD).getBytes("ascii"), "ascii")
      .toLowerCase()
      .replaceAll("’|'|`|´|\"", "")
      .replaceAll("[^a-zA-Z0-9]+", " ")
      .replaceAll("\\s+", " ")
      .replaceAll("\\b\\w{1,2}\\b\\s?", " ")
      .replaceAll("\\s+", " ")
  }
}
