package uk.gov.ipt.das.dataprofiler.profiler.rule.mask.logic

import uk.gov.ipt.das.dataprofiler.profiler.rule.mask.BuiltIn
import uk.gov.ipt.das.dataprofiler.value.{NullValue, RecordValue, StringValue}
@SuppressWarnings(Array("org.wartremover.warts.Null"))
case class StringMaxLength(name: String,
                           maxLength: Int,
                           nullAllowed: Boolean) extends BuiltIn {
  override def rule: RecordValue => Boolean = {
    case null | NullValue() => nullAllowed
    case StringValue(s) => s.length <= maxLength
    case _ => false
  }
}