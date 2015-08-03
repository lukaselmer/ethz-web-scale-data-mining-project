import scala.collection.mutable.HashMap

class LdaSettings extends Serializable {

  def getSettings(settingInputFile: String) {
    val settingDictionary = new HashMap[String, Double];

    try {
      val lines = scala.io.Source.fromFile(settingInputFile).getLines();
      while(lines.hasNext) {
        val params = lines.next().split("=");
        if(params.length == 2) {
          val key = params(0).trim();
          val value = params(1).toDouble;
          settingDictionary.put(key, value);
        }
      }
    }
    catch {
      case e: Exception => throw new Exception("Error reading settings file.");
    }

    MAX_GAMMA_CONVERGENCE_ITERATION = settingDictionary.get("MAX_GAMMA_CONVERGENCE_ITEREATIONS").getOrElse(100.0).toInt ;
    MAX_GLOBAL_ITERATION = settingDictionary.get("MAX_GLOBAL_ITERATIONS").getOrElse(20.0).toInt ;
    ALPHA_MAX_ITERATION = settingDictionary.get("ALPHA_MAX_ITERATION").getOrElse(1000.0).toInt;
    ETA = settingDictionary.get("ETA").getOrElse(0.000000000001);
    DEFAULT_ALPHA_UPDATE_CONVERGE_THRESHOLD = settingDictionary.get("DEFAULT_ALPHA_UPDATE_CONVERGE_THRESHOLD").getOrElse(0.000001);
    ALPHA_UPDATE_MAXIMUM_DECAY = settingDictionary.get("ALPHA_UPDATE_MAXIMUM_DECAY").getOrElse(11.0).toInt;
    ALPHA_UPDATE_DECAY_VALUE = settingDictionary.get("ALPHA_UPDATE_DECAY_VALUE").getOrElse(0.8);
    ALPHA_INITIALIZATION = settingDictionary.get("ALPHA_INITIALIZATION").getOrElse(0.001);
    GAMMA_INITIALIZATION = settingDictionary.get("ALPHA_INITIALIZATION").getOrElse(0.1);
  }

  var MAX_GAMMA_CONVERGENCE_ITERATION: Int = _  //Maximum Number of Iterations for Updating parameters Gamma, PHI in the mappers
  var MAX_GLOBAL_ITERATION: Int = _             //Maximum Number of Global Iterations
  var ALPHA_MAX_ITERATION: Int = _              //Maximum Number of Iterations for updating hyper parameter Alpha
  var ETA: Double = _                           //Smoothing coefficient
  var DEFAULT_ALPHA_UPDATE_CONVERGE_THRESHOLD: Double = _ //Threshold for checking convergence of hyper parameter alpha
  var ALPHA_UPDATE_DECAY_VALUE: Double = _      //Step size decay for the alpha update
  var ALPHA_UPDATE_MAXIMUM_DECAY: Int = _       //Maximum number of decay steps
  var ALPHA_INITIALIZATION: Double = _          //Initial value of hyper parameter vector alpha
  var GAMMA_INITIALIZATION: Double = _          //Initial value of gamma
}
