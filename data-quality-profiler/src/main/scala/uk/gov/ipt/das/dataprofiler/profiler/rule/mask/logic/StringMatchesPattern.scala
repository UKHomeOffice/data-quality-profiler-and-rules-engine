package uk.gov.ipt.das.dataprofiler.profiler.rule.mask.logic

import uk.gov.ipt.das.dataprofiler.profiler.rule.mask.BuiltIn
import uk.gov.ipt.das.dataprofiler.value.{NullValue, RecordValue, StringValue}

import java.util.regex.Pattern
@SuppressWarnings(Array("org.wartremover.warts.Null"))
case class StringMatchesPattern(name: String,
                                pattern: Pattern,
                                nullAllowed: Boolean) extends BuiltIn {
  override def rule: RecordValue => Boolean = {
    case null | NullValue() => nullAllowed
    case StringValue(s) => pattern.matcher(s).matches()
    case _ => false
  }
}