package uk.gov.ipt.das.dataprofiler.profiler.rule.mask.logic

import uk.gov.ipt.das.dataprofiler.profiler.rule.mask.BuiltIn
import uk.gov.ipt.das.dataprofiler.value.{NullValue, RecordValue, StringValue}
@SuppressWarnings(Array("org.wartremover.warts.Null"))
case class StringMinLength(name: String,
                           minLength: Int,
                           nullAllowed: Boolean) extends BuiltIn {
  override def rule: RecordValue => Boolean = {
    case null | NullValue() => nullAllowed
    case StringValue(s) => s.length >= minLength
    case _ => false
  }
}