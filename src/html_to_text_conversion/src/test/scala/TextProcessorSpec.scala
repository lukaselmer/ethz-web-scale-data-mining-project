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
    TextProcessor.cleanString("äöü") should be("aou")
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
    TextProcessor.cleanString("l33t") should be("l33t")
  }

  it should "remove urls" in {
    TextProcessor.cleanString("blub http://www.google.com/?aowefj=iogreonvr&grej=egroervn bla") should be("blub bla")
  }

  it should "remove blank lines" in {
    TextProcessor.cleanString("blub\nbla\nblub") should be("blub bla blub")
  }

  it should "remove tabs" in {
    TextProcessor.cleanString("blub\tbla\tblub") should be("blub bla blub")
  }
}
