package uk.gov.ipt.das.dataprofiler.profiler.rule.mask

import PopCheckProfiler.{POP_CHECKS, definition}
import uk.gov.ipt.das.dataprofiler.feature.FeatureDefinition
import uk.gov.ipt.das.dataprofiler.value.{NullValue, RecordValue, StringValue}

class PopCheckProfiler private extends MaskProfiler with Serializable {

  override def profile(value: RecordValue): RecordValue =
    value match {
      case NullValue() => value
      case _ => StringValue(POP_CHECKS(value.valueAsString))
    }

  override def getDefinition: FeatureDefinition = definition
}
@SuppressWarnings(Array("org.wartremover.warts.Null"))
object PopCheckProfiler {
  val INSTANCE = new PopCheckProfiler

  def apply(): PopCheckProfiler = INSTANCE

  private val definition = FeatureDefinition(`type` = "DQ", name = "POPCHECK")

  /**
   * A mask that says  “If the trimmed length
   * of this string is > 0, then output a 1,
   * else a 0.”
   *
   */
    //TODO turn in to optional if possible
  def POP_CHECKS(str: String): String = {
    str match {
      case null => "0"
      case "null" => "0"
      case "[]" => "0"
      case "" => "0"
      case _ => "1"
    }
  }

}