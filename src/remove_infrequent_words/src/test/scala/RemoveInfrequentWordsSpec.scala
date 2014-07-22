import org.apache.hadoop.io.Text
import org.scalatest._


class RemoveInfrequentWordsSpec extends FlatSpec with Matchers {

  "The RemoveInfrequentWordsApp" should "correctly remove words not in the keepWords list" in {
    RemoveInfrequentWordsApp.processWords(
      new Text("bli bla blub blubb blubbb bla bli blub"),
      Set("bli", "blubb", "blubbb")
    ) should be(new Text("bli blubb blubbb bli"))
  }


}
