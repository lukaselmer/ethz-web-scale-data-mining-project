import org.scalatest._


class TextProcessorSpec extends FlatSpec with Matchers {

  "The TextProcessor" should "process Strings" in {
    TextProcessor.cleanString("") should be("")
  }

  it should "remove stopwords 1" in {
    TextProcessor.cleanString("To be or not to be, that is the question") should be("question")
    TextProcessor.cleanString("What's up?") should be("")
    TextProcessor.cleanString("He's so the man") should be("man")
  }

  it should "remove stopwords 2" in {
    TextProcessor.cleanString("I know the lucky algorithm") should be("lucki algorithm")
  }

  it should "remove special characters" in {
    TextProcessor.cleanString("`\"*@.,;-_%&/())") should be("")
  }

  it should "normalize special characters" in {
    TextProcessor.cleanString("Ì‡ÒÚÓ˘ÂÂ") should be("")
    TextProcessor.cleanString("Á á Ć ć É é Í í Ó ó Ŕ ŕ Ś ś Ú ú Ý ý") should be("")
    TextProcessor.cleanString("ÍÆáöÐ") should be("")
    TextProcessor.cleanString("äöü") should be("")
  }

  it should "remove words with special characters" in {
    TextProcessor.cleanString("lÆexa") should be("")
    TextProcessor.cleanString("bärenstark") should be("")
    TextProcessor.cleanString("bärenstark") should be("")
  }

  it should "remove html escaped characters" in {
    TextProcessor.cleanString("&auml;") should be("")
    TextProcessor.cleanString("&blabla;") should be("")
  }

  it should "remove words including non ascii characters" in {
    TextProcessor.cleanString("CTRL+A") should be("ctrl")
    TextProcessor.cleanString("artykułu") should be("")
  }

  it should "ignore records with many special characters" in {
    TextProcessor.cleanString("Í2ÆáoÐ8vc^çYo»z ?2_²™U}áF“'A+l Ý]•™••™•™]YØÿæáñÑëÿ }KÕÝ | ð9ÌäT Öû î   a õ~Pkíëªñ•¡" +
      " TÎÅ 0e8Ô°ÕˆP¢ƒ caj5g1£^×ÁZ9ÂÕ È ŸÃE þO;—(Ä›ÀŒÏ… ¦7ú] y´Ë] ...\nWikinews :Edytowanie artykułów - Wikinews, w" +
      "olne źródło informacji") should be("")
    // Old version: should be("cajg wikinew edytowani wikinew woln informacji")
    // Better (?): or should be ("wikinews edytowanie wikinews olne informacji")
    // Old version: TextProcessor.cleanString("i skopiuj istniejącą wersję artykułu do dokumentu w") should be("skopiuj dokumentu")
    TextProcessor.cleanString("i skopiuj istniejącą wersję artykułu do dokumentu w") should be("")
    // Better (?): should be("")
  }

  it should "remove stopwords and do stemming" in {
    TextProcessor.cleanString("I am running in the park") should be("run park")
  }

  it should "do some stemming" in {
    // TODO: validate these tests!
    TextProcessor.cleanString("running park parking sun sunny") should be("run park park sun sunni")
    TextProcessor.cleanString("greeting") should be("greet")
    TextProcessor.cleanString("jumping") should be("jump")
    TextProcessor.cleanString("programmers") should be("programm")
    TextProcessor.cleanString("programers program programming programs") should be("program program program program")
    TextProcessor.cleanString("coders code coding examples") should be("coder code code exampl")
    TextProcessor.cleanString("chocolate is my weakness") should be("chocol weak")
  }

  it should "convert everything to lower case" in {
    TextProcessor.cleanString("HoUsE") should be("hous")
    TextProcessor.cleanString("ViLLa") should be("villa")
    TextProcessor.cleanString("SEARCH ENGINE") should be("search engin")
  }

  it should "remove numbers" in {
    TextProcessor.cleanString("42 774423 30954871293857") should be("")
    TextProcessor.cleanString("fi33nd") should be("find")
  }

  it should "remove urls" in {
    TextProcessor.cleanString("blub http://www.google.com/?aowefj=iogreonvr&grej=egroervn bla") should be("blub bla")
    TextProcessor.cleanString("blub www.google.com/?aowefj=iogreonvr&grej=egroervn bla") should be("blub bla")
    TextProcessor.cleanString("blub google.com/?aowefj=iogreonvr&grej=egroervn bla") should be("blub bla")
    TextProcessor.cleanString("blub som.bla/index?aowefj=iogreonvr&grej=egroervn bla") should be("blub bla")
    TextProcessor.cleanString("maciejowka.org/index.php?option=com_content&view=article&id=133&Itemid=82") should be("")
    TextProcessor.cleanString("pl.wiktionary.org/wiki/MediaWiki:Edittools") should be("")
    TextProcessor.cleanString("pdf.crse.com/manuals/3858958121.pdf") should be("")
  }

  it should "remove blank lines" in {
    TextProcessor.cleanString("blub\nbla\nblub") should be("blub bla blub")
  }

  it should "remove tabs" in {
    TextProcessor.cleanString("blub\tbla\tblub") should be("blub bla blub")
  }

  it should "not write words together :)" in {
    TextProcessor.cleanString("find 2011 find") should be("find find")
  }

  it should "remove short words" in {
    TextProcessor.cleanString("x y zz ab yx za abc un nu ku ba tw a q w e blub t nb c ae z g") should be("abc blub")
  }

  it should "remove whole documents if there are many non-ascii symbols in it" in {
    TextProcessor.cleanString("normalword somewordwithä somewordwithü somewordwithé somewordwithà somewordwithè") should be("")
    TextProcessor.cleanString("normalword somewordwithä") should be("")
  }

}
