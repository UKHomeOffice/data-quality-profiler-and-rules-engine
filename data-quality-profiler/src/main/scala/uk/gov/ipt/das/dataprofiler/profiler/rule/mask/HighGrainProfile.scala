package uk.gov.ipt.das.dataprofiler.profiler.rule.mask

import HighGrainProfile.{definition, inplaceChange}
import uk.gov.ipt.das.dataprofiler.feature.FeatureDefinition
import uk.gov.ipt.das.dataprofiler.value.{NullValue, RecordValue, StringValue}

@SuppressWarnings(Array("org.wartremover.warts.Null","org.wartremover.warts.Var","org.wartremover.warts.Return"))
class HighGrainProfile private extends MaskProfiler with Serializable {

  override def profile(value: RecordValue): RecordValue =
    value match {
      case NullValue() => value
      case _ => StringValue(inplaceChange(value.valueAsString))
    }

  override def getDefinition: FeatureDefinition = definition
}
@SuppressWarnings(Array("org.wartremover.warts.Null","org.wartremover.warts.Var","org.wartremover.warts.Return"))
object HighGrainProfile {
  def apply(): HighGrainProfile = INSTANCE

  val INSTANCE = new HighGrainProfile

  private val definition = FeatureDefinition(`type` = "DQ", name = "HIGHGRAIN")

  // more efficient than ASCIICLASS_HIGHGRAIN, see benchmarks in HighGrainProfilerTest
  //TODO change return type of Optional String
  def inplaceChange(s: String): String = {
    if (s == null) return null

    val inPlaceStr = s.toCharArray

    var i = 0
    while (i < s.length) {
      val c = s(i)
      if (c >= 'A' && c <= 'Z') {
        inPlaceStr(i) = 'A'
      } else if (c >= 'a' && c <= 'z') {
        inPlaceStr(i) = 'a'
      } else if (c >= '0' && c <= '9') {
        inPlaceStr(i) = '9'
      } else if (c == '\t') {
        inPlaceStr(i) = 'T'
      } else {
        // don't change the char
      }
      i += 1
    }
    new String(inPlaceStr)
  }

}
