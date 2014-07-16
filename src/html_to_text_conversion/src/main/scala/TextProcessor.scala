import java.io.StringReader
import java.text.Normalizer
import java.util.regex.Pattern

import org.apache.lucene.analysis.TokenStream
import org.apache.lucene.analysis.en.EnglishAnalyzer
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute
import org.apache.lucene.analysis.util.CharArraySet
import org.apache.lucene.util.Version

import scala.collection.JavaConversions._
import scala.collection.mutable

object TextProcessor {
  val stopWords = initStopWords()
  val analyzer: EnglishAnalyzer = new EnglishAnalyzer(Version.LUCENE_46, stopWords)
  val regex1 = Pattern.compile("\\b(https?|ftp|file)://[-a-zA-Z0-9+&@#/%?=~_|!:,.;]*[-a-zA-Z0-9+&@#/%=~_|]", Pattern.CASE_INSENSITIVE)
  val regex2 = Pattern.compile("[^a-zA-Z][0-9]+[^a-zA-Z]", Pattern.CASE_INSENSITIVE)
  val regex3 = Pattern.compile("[^a-zA-Z0-9 ]+", Pattern.CASE_INSENSITIVE)

  def cleanString(text: String): String = {
    val preprocessedText = regex2.matcher(regex1.matcher(text).replaceAll("")).replaceAll("")
    val stream = analyzer.tokenStream("contents", new StringReader(preprocessedText))
    val str = tokenStreamToString(stream)
    regex3.matcher(new String(Normalizer.normalize(str, Normalizer.Form.NFKD).getBytes("ascii"), "ascii")).
      replaceAll("").replaceAll("\\s+", " ").trim
  }

  private def tokenStreamToString(stream: TokenStream): String = {
    val sb = new mutable.StringBuilder()
    val attr = stream.addAttribute(classOf[CharTermAttribute]);
    stream.reset();
    while (stream.incrementToken()) {
      sb.append(attr.toString).append(" ")
    }
    stream.end();
    stream.close();
    sb.toString
  }

  def initStopWords(): CharArraySet = {
    // From StopAnalyzer.ENGLISH_STOP_WORDS_SET
    val words1 = "a,an,and,are,as,at,be,but,by,for,if,in,into,is,it,no,not,of,on,or,such,that,the,their,then,there," +
      "these,they,this,to,was,will,with"

    // From http://www.textfixer.com/resources/common-english-words.txt
    val words2 = "a,able,about,across,after,all,almost,also,am,among,an,and,any,are,as,at,be,because,been,but,by,can," +
      "cannot,could,dear,did,do,does,either,else,ever,every,for,from,get,got,had,has,have,he,her,hers,him,his,how," +
      "however,i,if,in,into,is,it,its,just,least,let,like,likely,may,me,might,most,must,my,neither,no,nor,not,of," +
      "off,often,on,only,or,other,our,own,rather,said,say,says,she,should,since,so,some,than,that,the,their,them," +
      "then,there,these,they,this,tis,to,too,twas,us,wants,was,we,were,what,when,where,which,while,who,whom,why," +
      "will,with,would,yet,you,your"

    // From http://www.ranks.nl/stopwords/ (all but the last block)
    val words3 = "a,about,above,after,again,against,all,am,an,and,any,are,aren't,as,at,be,because,been,before,being" +
      ",below,between,both,but,by,can't,cannot,could,couldn't,did,didn't,do,does,doesn't,doing,don't,down,during,eac" +
      "h,few,for,from,further,had,hadn't,has,hasn't,have,haven't,having,he,he'd,he'll,he's,her,here,here's,hers,hers" +
      "elf,him,himself,his,how,how's,i,i'd,i'll,i'm,i've,if,in,into,is,isn't,it,it's,its,itself,let's,me,more,most,m" +
      "ustn't,my,myself,no,nor,not,of,off,on,once,only,or,other,ought,our,ours,ourselves,out,over,own,same,shan't,sh" +
      "e,she'd,she'll,she's,should,shouldn't,so,some,such,than,that,that's,the,their,theirs,them,themselves,then,the" +
      "re,there's,these,they,they'd,they'll,they're,they've,this,those,through,to,too,under,until,up,very,was,wasn't" +
      ",we,we'd,we'll,we're,we've,were,weren't,what,what's,when,when's,where,where's,which,while,who,who's,whom,why," +
      "why's,with,won't,would,wouldn't,you,you'd,you'll,you're,you've,your,yours,yourself,yourselves,a's,able,about," +
      "above,according,accordingly,across,actually,after,afterwards,again,against,ain't,all,allow,allows,almost,alon" +
      "e,along,already,also,although,always,am,among,amongst,an,and,another,any,anybody,anyhow,anyone,anything,anywa" +
      "y,anyways,anywhere,apart,appear,appreciate,appropriate,are,aren't,around,as,aside,ask,asking,associated,at,av" +
      "ailable,away,awfully,be,became,because,become,becomes,becoming,been,before,beforehand,behind,being,believe,be" +
      "low,beside,besides,best,better,between,beyond,both,brief,but,by,c'mon,c's,came,can,can't,cannot,cant,cause,ca" +
      "uses,certain,certainly,changes,clearly,co,com,come,comes,concerning,consequently,consider,considering,contain" +
      ",containing,contains,corresponding,could,couldn't,course,currently,definitely,described,despite,did,didn't,di" +
      "fferent,do,does,doesn't,doing,don't,done,down,downwards,during,each,edu,eg,eight,either,else,elsewhere,enough" +
      ",entirely,especially,et,etc,even,ever,every,everybody,everyone,everything,everywhere,ex,exactly,example,excep" +
      "t,far,few,fifth,first,five,followed,following,follows,for,former,formerly,forth,four,from,further,furthermore" +
      ",get,gets,getting,given,gives,go,goes,going,gone,got,gotten,greetings,had,hadn't,happens,hardly,has,hasn't,ha" +
      "ve,haven't,having,he,he's,hello,help,hence,her,here,here's,hereafter,hereby,herein,hereupon,hers,herself,hi,h" +
      "im,himself,his,hither,hopefully,how,howbeit,however,i'd,i'll,i'm,i've,ie,if,ignored,immediate,in,inasmuch,inc" +
      ",indeed,indicate,indicated,indicates,inner,insofar,instead,into,inward,is,isn't,it,it'd,it'll,it's,its,itself" +
      ",just,keep,keeps,kept,know,known,knows,last,lately,later,latter,latterly,least,less,lest,let,let's,like,liked" +
      ",likely,little,look,looking,looks,ltd,mainly,many,may,maybe,me,mean,meanwhile,merely,might,more,moreover,most" +
      ",mostly,much,must,my,myself,name,namely,nd,near,nearly,necessary,need,needs,neither,never,nevertheless,new,ne" +
      "xt,nine,no,nobody,non,none,noone,nor,normally,not,nothing,novel,now,nowhere,obviously,of,off,often,oh,ok,okay" +
      ",old,on,once,one,ones,only,onto,or,other,others,otherwise,ought,our,ours,ourselves,out,outside,over,overall,o" +
      "wn,particular,particularly,per,perhaps,placed,please,plus,possible,presumably,probably,provides,que,quite,qv," +
      "rather,rd,re,really,reasonably,regarding,regardless,regards,relatively,respectively,right,said,same,saw,say,s" +
      "aying,says,second,secondly,see,seeing,seem,seemed,seeming,seems,seen,self,selves,sensible,sent,serious,seriou" +
      "sly,seven,several,shall,she,should,shouldn't,since,six,so,some,somebody,somehow,someone,something,sometime,so" +
      "metimes,somewhat,somewhere,soon,sorry,specified,specify,specifying,still,sub,such,sup,sure,t's,take,taken,tel" +
      "l,tends,th,than,thank,thanks,thanx,that,that's,thats,the,their,theirs,them,themselves,then,thence,there,there" +
      "'s,thereafter,thereby,therefore,therein,theres,thereupon,these,they,they'd,they'll,they're,they've,think,thir" +
      "d,this,thorough,thoroughly,those,though,three,through,throughout,thru,thus,to,together,too,took,toward,toward" +
      "s,tried,tries,truly,try,trying,twice,two,un,under,unfortunately,unless,unlikely,until,unto,up,upon,us,use,use" +
      "d,useful,uses,using,usually,value,various,very,via,viz,vs,want,wants,was,wasn't,way,we,we'd,we'll,we're,we've" +
      ",welcome,well,went,were,weren't,what,what's,whatever,when,whence,whenever,where,where's,whereafter,whereas,wh" +
      "ereby,wherein,whereupon,wherever,whether,which,while,whither,who,who's,whoever,whole,whom,whose,why,will,will" +
      "ing,wish,with,within,without,won't,wonder,would,wouldn't,yes,yet,you,you'd,you'll,you're,you've,your,yours,yo" +
      "urself,yourselves,zero,I,a,about,an,are,as,at,be,by,com,for,from,how,in,is,it,of,on,or,that,the,this,to,was,w" +
      "hat,when,where,who,will,with,the,www"

    // From http://www.ranks.nl/stopwords/ (only the last block)
    // TODO: verify that all these words can be removed!
    val words4 = "a,able,about,above,abst,accordance,according,accordingly,across,act,actually,added,adj,affected,af" +
      "fecting,affects,after,afterwards,again,against,ah,all,almost,alone,along,already,also,although,always,am,amon" +
      "g,amongst,an,and,announce,another,any,anybody,anyhow,anymore,anyone,anything,anyway,anyways,anywhere,apparent" +
      "ly,approximately,are,aren,arent,arise,around,as,aside,ask,asking,at,auth,available,away,awfully,b,back,be,bec" +
      "ame,because,become,becomes,becoming,been,before,beforehand,begin,beginning,beginnings,begins,behind,being,bel" +
      "ieve,below,beside,besides,between,beyond,biol,both,brief,briefly,but,by,c,ca,came,can,cannot,can't,cause,caus" +
      "es,certain,certainly,co,com,come,comes,contain,containing,contains,could,couldnt,d,date,did,didn't,different," +
      "do,does,doesn't,doing,done,don't,down,downwards,due,during,e,each,ed,edu,effect,eg,eight,eighty,either,else,e" +
      "lsewhere,end,ending,enough,especially,et,et-al,etc,even,ever,every,everybody,everyone,everything,everywhere,e" +
      "x,except,f,far,few,ff,fifth,first,five,fix,followed,following,follows,for,former,formerly,forth,found,four,fr" +
      "om,further,furthermore,g,gave,get,gets,getting,give,given,gives,giving,go,goes,gone,got,gotten,h,had,happens," +
      "hardly,has,hasn't,have,haven't,having,he,hed,hence,her,here,hereafter,hereby,herein,heres,hereupon,hers,herse" +
      "lf,hes,hi,hid,him,himself,his,hither,home,how,howbeit,however,hundred,i,id,ie,if,i'll,im,immediate,immediatel" +
      "y,importance,important,in,inc,indeed,index,information,instead,into,invention,inward,is,isn't,it,itd,it'll,it" +
      "s,itself,i've,j,just,k,keep\tkeeps,kept,kg,km,know,known,knows,l,largely,last,lately,later,latter,latterly,le" +
      "ast,less,lest,let,lets,like,liked,likely,line,little,'ll,look,looking,looks,ltd,m,made,mainly,make,makes,many" +
      ",may,maybe,me,mean,means,meantime,meanwhile,merely,mg,might,million,miss,ml,more,moreover,most,mostly,mr,mrs," +
      "much,mug,must,my,myself,n,na,name,namely,nay,nd,near,nearly,necessarily,necessary,need,needs,neither,never,ne" +
      "vertheless,new,next,nine,ninety,no,nobody,non,none,nonetheless,noone,nor,normally,nos,not,noted,nothing,now,n" +
      "owhere,o,obtain,obtained,obviously,of,off,often,oh,ok,okay,old,omitted,on,once,one,ones,only,onto,or,ord,othe" +
      "r,others,otherwise,ought,our,ours,ourselves,out,outside,over,overall,owing,own,p,page,pages,part,particular,p" +
      "articularly,past,per,perhaps,placed,please,plus,poorly,possible,possibly,potentially,pp,predominantly,present" +
      ",previously,primarily,probably,promptly,proud,provides,put,q,que,quickly,quite,qv,r,ran,rather,rd,re,readily," +
      "really,recent,recently,ref,refs,regarding,regardless,regards,related,relatively,research,respectively,resulte" +
      "d,resulting,results,right,run,s,said,same,saw,say,saying,says,sec,section,see,seeing,seem,seemed,seeming,seem" +
      "s,seen,self,selves,sent,seven,several,shall,she,shed,she'll,shes,should,shouldn't,show,showed,shown,showns,sh" +
      "ows,significant,significantly,similar,similarly,since,six,slightly,so,some,somebody,somehow,someone,somethan," +
      "something,sometime,sometimes,somewhat,somewhere,soon,sorry,specifically,specified,specify,specifying,still,st" +
      "op,strongly,sub,substantially,successfully,such,sufficiently,suggest,sup,sure"

    val words = List(words1, words2, words3, words4).mkString(",").split(",").toSet
    CharArraySet.unmodifiableSet(new CharArraySet(Version.LUCENE_46, words, false))
  }
}
