import scala.collection.mutable.HashMap

class LdaSettings {

  def getSettings(settingInputFile: String) {
    val settingDictionary = new HashMap[String, Double];

    settingDictionary.put("MAX_GAMMA_CONVERGENCE_ITEREATIONS", 100);
    settingDictionary.put("MAX_GLOBAL_ITERATIONS", 20);
    settingDictionary.put("ALPHA_MAX_ITERATION", 1000);
    settingDictionary.put("ETA", 0.000000000001);
    settingDictionary.put("DEFAULT_ALPHA_UPDATE_CONVERGE_THRESHOLD", 0.000001);
    settingDictionary.put("ALPHA_UPDATE_MAXIMUM_DECAY", 11);
    settingDictionary.put("ALPHA_UPDATE_DECAY_VALUE", 0.8);
    settingDictionary.put("ALPHA_INITIALIZATION", 0.001);

    MAX_GAMMA_CONVERGENCE_ITERATION = settingDictionary.get("MAX_GAMMA_CONVERGENCE_ITEREATIONS").getOrElse(100.0).toInt ;
    MAX_GLOBAL_ITERATION = settingDictionary.get("MAX_GLOBAL_ITERATIONS").getOrElse(20.0).toInt ;
    ALPHA_MAX_ITERATION = settingDictionary.get("ALPHA_MAX_ITERATION").getOrElse(1000.0).toInt //(1000);
    ETA = settingDictionary.get("ETA").getOrElse(0.000000000001);
    DEFAULT_ALPHA_UPDATE_CONVERGE_THRESHOLD = settingDictionary.get("DEFAULT_ALPHA_UPDATE_CONVERGE_THRESHOLD").getOrElse(0.000001);
    ALPHA_UPDATE_MAXIMUM_DECAY = settingDictionary.get("ALPHA_UPDATE_MAXIMUM_DECAY").getOrElse(11.0).toInt;
    ALPHA_UPDATE_DECAY_VALUE = settingDictionary.get("ALPHA_UPDATE_DECAY_VALUE").getOrElse(0.8);
    ALPHA_INITIALIZATION = settingDictionary.get("ALPHA_INITIALIZATION").getOrElse(0.001);
  }

  var MAX_GAMMA_CONVERGENCE_ITERATION: Int
  var MAX_GLOBAL_ITERATION: Int
  var ALPHA_MAX_ITERATION: Int
  var ETA: Double
  var DEFAULT_ALPHA_UPDATE_CONVERGE_THRESHOLD: Double;
  var ALPHA_UPDATE_MAXIMUM_DECAY: Int
  var ALPHA_UPDATE_DECAY_VALUE: Double
  var ALPHA_INITIALIZATION: Double
}
