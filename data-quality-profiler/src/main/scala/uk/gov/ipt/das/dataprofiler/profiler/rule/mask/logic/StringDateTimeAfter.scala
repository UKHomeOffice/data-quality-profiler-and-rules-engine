package uk.gov.ipt.das.dataprofiler.profiler.rule.mask.logic

import uk.gov.ipt.das.dataprofiler.profiler.rule.mask.BuiltIn
import uk.gov.ipt.das.dataprofiler.profiler.rule.mask.value.TimestampString
import uk.gov.ipt.das.dataprofiler.value.{NullValue, RecordValue, StringValue}

import java.time.OffsetDateTime
@SuppressWarnings(Array("org.wartremover.warts.Null"))
case class StringDateTimeAfter(name: String,
                               afterDate: OffsetDateTime,
                               nullAllowed: Boolean) extends BuiltIn {
  override def rule: RecordValue => Boolean = {
    case null | NullValue() => nullAllowed
    case StringValue(TimestampString(d)) => d.isAfter(afterDate)
    case _ => false
  }
}