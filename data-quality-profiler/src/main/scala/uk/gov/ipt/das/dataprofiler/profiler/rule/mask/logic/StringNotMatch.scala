package uk.gov.ipt.das.dataprofiler.profiler.rule.mask.logic

import uk.gov.ipt.das.dataprofiler.profiler.rule.mask.BuiltIn
import uk.gov.ipt.das.dataprofiler.value.{NullValue, RecordValue, StringValue}
@SuppressWarnings(Array("org.wartremover.warts.Null"))
case class StringNotMatch(name: String,
                          notMatch: String,
                          nullAllowed: Boolean) extends BuiltIn {
  override def rule: RecordValue => Boolean = {
    case null | NullValue() => nullAllowed
    case StringValue(s) => s != notMatch
    case _ => false
  }
}