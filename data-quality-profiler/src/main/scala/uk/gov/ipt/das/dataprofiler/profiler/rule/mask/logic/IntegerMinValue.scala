package uk.gov.ipt.das.dataprofiler.profiler.rule.mask.logic

import uk.gov.ipt.das.dataprofiler.profiler.rule.mask.BuiltIn
import uk.gov.ipt.das.dataprofiler.profiler.rule.mask.value.IntString
import uk.gov.ipt.das.dataprofiler.value.{DoubleValue, LongValue, NullValue, RecordValue, StringValue}
@SuppressWarnings(Array("org.wartremover.warts.Null"))
case class IntegerMinValue(name: String,
                           minValue: Int,
                           nullAllowed: Boolean) extends BuiltIn {
  override def rule: RecordValue => Boolean = {
    case null | NullValue() => nullAllowed
    case LongValue(l) => l >= minValue
    case DoubleValue(d) => d >= minValue
    case StringValue(IntString(i)) => i >= minValue
    case _ => false
  }
}