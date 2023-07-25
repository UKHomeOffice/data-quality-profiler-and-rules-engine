package uk.gov.ipt.das.dataprofiler.profiler.rule.mask

import LowGrainProfile.{charByChar, definition}
import uk.gov.ipt.das.dataprofiler.feature.FeatureDefinition
import uk.gov.ipt.das.dataprofiler.value.{NullValue, RecordValue, StringValue}

import scala.collection.immutable.HashSet
@SuppressWarnings(Array("org.wartremover.warts.NonUnitStatements"))
class LowGrainProfile private extends MaskProfiler with Serializable {

  override def profile(value: RecordValue): RecordValue =
    value match {
      case NullValue() => value
      case _ => StringValue(charByChar(value.valueAsString))
    }

  override def getDefinition: FeatureDefinition = definition
}
@SuppressWarnings(Array("org.wartremover.warts.Var","org.wartremover.warts.Null","org.wartremover.warts.Return","org.wartremover.warts.NonUnitStatements"))
object LowGrainProfile {
  val INSTANCE = new LowGrainProfile

  def apply(): LowGrainProfile = INSTANCE

  private val definition = FeatureDefinition(`type` = "DQ", name = "LOWGRAIN")

  private val collapsibleChars = HashSet[Char]('A', 'a', '9')
//TODO change return type to optional string
  //return empty string
  def charByChar(s: String): String = {
    if (s == null) return null

    val sb = new StringBuilder
    var i = 0
    var last: Char = ' '
    while (i < s.length) {
      val c = s(i)
      val toAdd = {
        if (c >= 'A' && c <= 'Z') {
          'A'
        } else if (c >= 'a' && c <= 'z') {
          'a'
        } else if (c >= '0' && c <= '9') {
          '9'
        } else if (c == '\t') {
          'T'
        } else {
          c
        }
      }

      if (!collapsibleChars.contains(toAdd) || toAdd != last) {
        sb.append(toAdd)
      }

      i += 1
      last = toAdd
    }
    sb.mkString
  }

}